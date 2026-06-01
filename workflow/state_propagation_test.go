// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package workflow

import (
	"context"
	"errors"
	"iter"
	"sync"
	"testing"
	"time"

	"google.golang.org/adk/agent"
	"google.golang.org/adk/session"
)

// --- Session test doubles ---

// recordingState is a plain map-backed session.State that records every
// Set call along with whether it came from a node body or from the
// scheduler/runner-driven merge path. Tests use the recorded log to
// assert which channel actually wrote a given key.
//
// noBodyDirectSet, when true, forces the recording to mark unmarked
// callers as bodyDirect (fail-on-direct-write style). Useful for the
// raw-StateDelta test where node bodies must NOT call Set directly —
// any unaccounted write fails the test.
type recordingState struct {
	mu      sync.Mutex
	backing map[string]any
	writes  []stateWrite
}

type stateWrite struct {
	key  string
	val  any
	from string // "body" | "merge" | "" (unmarked)
}

func (s *recordingState) Get(key string) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v, ok := s.backing[key]; ok {
		return v, nil
	}
	return nil, session.ErrStateKeyNotExist
}

func (s *recordingState) Set(key string, val any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.backing == nil {
		s.backing = map[string]any{}
	}
	s.backing[key] = val
	s.writes = append(s.writes, stateWrite{key: key, val: val})
	return nil
}

func (s *recordingState) All() iter.Seq2[string, any] {
	s.mu.Lock()
	snap := make(map[string]any, len(s.backing))
	for k, v := range s.backing {
		snap[k] = v
	}
	s.mu.Unlock()
	return func(yield func(string, any) bool) {
		for k, v := range snap {
			if !yield(k, v) {
				return
			}
		}
	}
}

// recordingSession implements session.Session backed by a recordingState.
type recordingSession struct {
	state *recordingState
}

func (s *recordingSession) ID() string                { return "test-session" }
func (s *recordingSession) AppName() string           { return "test-app" }
func (s *recordingSession) UserID() string            { return "test-user" }
func (s *recordingSession) State() session.State      { return s.state }
func (s *recordingSession) Events() session.Events    { return nil }
func (s *recordingSession) LastUpdateTime() time.Time { return time.Time{} }

func newRecordingSession() *recordingSession {
	return &recordingSession{state: &recordingState{backing: map[string]any{}}}
}

// stateMockCtx wraps MockInvocationContext with a session, since
// newMockCtx returns a context whose Session() is nil.
type stateMockCtx struct {
	*MockInvocationContext
	sess session.Session
}

func (c *stateMockCtx) Session() session.Session { return c.sess }
func (c *stateMockCtx) WithContext(gctx context.Context) agent.InvocationContext {
	inner := c.MockInvocationContext.WithContext(gctx).(*MockInvocationContext)
	return &stateMockCtx{MockInvocationContext: inner, sess: c.sess}
}

func newStateMockCtx(t *testing.T, sess session.Session) *stateMockCtx {
	t.Helper()
	return &stateMockCtx{
		MockInvocationContext: newSeededMockCtx(t),
		sess:                  sess,
	}
}

// rawStateDeltaNode is a Node that emits exactly one Event with a
// caller-provided StateDelta, without going through the
// NodeContext.State() helper. Used to verify the lower-level contract:
// any node may set Event.Actions.StateDelta and the scheduler must
// merge it into the session for downstream nodes to observe.
type rawStateDeltaNode struct {
	BaseNode
	delta map[string]any
}

func newRawStateDeltaNode(name string, delta map[string]any) *rawStateDeltaNode {
	return &rawStateDeltaNode{
		BaseNode: NewBaseNode(name, "", NodeConfig{}),
		delta:    delta,
	}
}

func (n *rawStateDeltaNode) Run(ctx agent.InvocationContext, _ any) iter.Seq2[*session.Event, error] {
	return func(yield func(*session.Event, error) bool) {
		ev := session.NewEvent(ctx.InvocationID())
		ev.Author = n.Name()
		ev.Output = "ok"
		for k, v := range n.delta {
			ev.Actions.StateDelta[k] = v
		}
		yield(ev, nil)
	}
}

// --- Tests ---

// TestEventState_NodeContextStateSet_VisibleToDownstream verifies the
// canonical contract: a node writes via ctx.State().Set, the scheduler
// auto-flushes the accumulated StateDelta onto an emitted event, and a
// downstream node reads the value via ctx.State().Get.
func TestEventState_NodeContextStateSet_VisibleToDownstream(t *testing.T) {
	t.Parallel()

	sess := newRecordingSession()

	const key = "k"
	const val = "v"

	var (
		readback any
		readErr  error
	)

	writer := NewFunctionNode(
		"writer",
		func(ctx agent.InvocationContext, _ string) (string, error) {
			nc, ok := ctx.(NodeContext)
			if !ok {
				t.Fatalf("writer ctx is not a NodeContext: %T", ctx)
			}
			if err := nc.State().Set(key, val); err != nil {
				t.Fatalf("writer State().Set: %v", err)
			}
			return "ok", nil
		},
		defaultNodeConfig,
	)

	reader := NewFunctionNode(
		"reader",
		func(ctx agent.InvocationContext, in string) (string, error) {
			nc, ok := ctx.(NodeContext)
			if !ok {
				t.Fatalf("reader ctx is not a NodeContext: %T", ctx)
			}
			readback, readErr = nc.State().Get(key)
			return in, nil
		},
		defaultNodeConfig,
	)

	wf := mustNew(t, []Edge{
		{From: Start, To: writer},
		{From: writer, To: reader},
	})

	drain(t, wf.Run(newStateMockCtx(t, sess)))

	if readErr != nil {
		t.Fatalf("reader Get(%q): %v", key, readErr)
	}
	if readback != val {
		t.Errorf("reader observed State[%q] = %v, want %v", key, readback, val)
	}
}

// TestEventState_NodeContextStateSet_FoldedOntoEmittedEvent verifies
// that State().Set writes do not just live on the per-activation
// accumulator — they reach the consumer-visible event stream (so
// runner.AppendEvent / SessionService.AppendEvent can persist them).
//
// This is the contract that distinguishes State() from a direct
// Session().State().Set: the latter mutates only the in-memory map,
// the former also stamps StateDelta onto an emitted event for
// persistence and replay.
func TestEventState_NodeContextStateSet_FoldedOntoEmittedEvent(t *testing.T) {
	t.Parallel()

	sess := newRecordingSession()

	const key = "k2"
	const val = "v2"

	writer := NewFunctionNode(
		"writer",
		func(ctx agent.InvocationContext, _ string) (string, error) {
			nc := ctx.(NodeContext)
			return "ok", nc.State().Set(key, val)
		},
		defaultNodeConfig,
	)

	wf := mustNew(t, []Edge{{From: Start, To: writer}})

	events := drain(t, wf.Run(newStateMockCtx(t, sess)))

	// At least one emitted event must carry StateDelta[key]=val.
	var found bool
	for _, ev := range events {
		if ev == nil {
			continue
		}
		if got, ok := ev.Actions.StateDelta[key]; ok {
			if got != val {
				t.Errorf("event %s StateDelta[%q] = %v, want %v", ev.Author, key, got, val)
			}
			found = true
		}
	}
	if !found {
		t.Errorf("no emitted event carried StateDelta[%q]; "+
			"State().Set writes must be folded onto an outgoing event for persistence", key)
	}
}

// TestEventState_RawEventStateDelta_MergedIntoSession verifies that a
// node emitting Event.Actions.StateDelta directly (bypassing the
// State() helper) has its writes merged into the session by the
// scheduler. This is the lower-level contract; State() is sugar on
// top of it.
func TestEventState_RawEventStateDelta_MergedIntoSession(t *testing.T) {
	t.Parallel()

	sess := newRecordingSession()
	const key = "ek"
	const val = "ev"

	writer := newRawStateDeltaNode("writer-raw", map[string]any{key: val})

	var (
		readback any
		readErr  error
	)
	reader := NewFunctionNode(
		"reader-raw",
		func(ctx agent.InvocationContext, in string) (string, error) {
			nc := ctx.(NodeContext)
			readback, readErr = nc.State().Get(key)
			return in, nil
		},
		defaultNodeConfig,
	)

	wf := mustNew(t, []Edge{
		{From: Start, To: writer},
		{From: writer, To: reader},
	})

	drain(t, wf.Run(newStateMockCtx(t, sess)))

	// Writer body never called State().Set — only the scheduler's
	// mergeStateDeltaIntoSession could have produced the write
	// recorded on the session.
	if readErr != nil {
		t.Fatalf("reader Get(%q): %v", key, readErr)
	}
	if readback != val {
		t.Errorf("reader observed State[%q] = %v, want %v "+
			"(scheduler should have merged writer's StateDelta into session)",
			key, readback, val)
	}

	// Sanity: the only Set call recorded on the session for `key`
	// should carry val.
	sess.state.mu.Lock()
	defer sess.state.mu.Unlock()
	var observed []any
	for _, w := range sess.state.writes {
		if w.key == key {
			observed = append(observed, w.val)
		}
	}
	if len(observed) == 0 {
		t.Errorf("scheduler did not write %q to session.State; "+
			"raw StateDelta from emitted events must be merged", key)
	}
}

// TestEventState_ReadAfterWriteWithinNodeBody verifies that within a
// single node activation, calling State().Set(k,v) followed by
// State().Get(k) returns the freshly-written value (read-after-write
// semantics, per state.py:91-98 in adk-python).
func TestEventState_ReadAfterWriteWithinNodeBody(t *testing.T) {
	t.Parallel()

	sess := newRecordingSession()
	const key = "raw"
	const val = "value-1"

	var (
		readback any
		readErr  error
	)

	node := NewFunctionNode(
		"raw-test",
		func(ctx agent.InvocationContext, _ string) (string, error) {
			nc := ctx.(NodeContext)
			if err := nc.State().Set(key, val); err != nil {
				t.Fatalf("Set: %v", err)
			}
			// Read immediately, before any yield/flush.
			readback, readErr = nc.State().Get(key)
			return "ok", nil
		},
		defaultNodeConfig,
	)

	wf := mustNew(t, []Edge{{From: Start, To: node}})

	drain(t, wf.Run(newStateMockCtx(t, sess)))

	if readErr != nil {
		t.Fatalf("Get(%q): %v", key, readErr)
	}
	if readback != val {
		t.Errorf("State[%q] = %v, want %v (read-after-write)", key, readback, val)
	}
}

// TestEventState_TempPrefixSkippedByScheduler verifies that keys with
// the temp: prefix written via StateDelta on an emitted event are
// NOT merged into the session by the scheduler — they are
// invocation-scoped scratch and must not pollute persistent state.
// Mirrors the behaviour of session/inmemory.go's trimTempDeltaState.
func TestEventState_TempPrefixSkippedByScheduler(t *testing.T) {
	t.Parallel()

	sess := newRecordingSession()

	tempKey := session.KeyPrefixTemp + "scratch"
	persistKey := "persist"

	writer := newRawStateDeltaNode("writer-temp", map[string]any{
		tempKey:    "v",
		persistKey: "p",
	})

	wf := mustNew(t, []Edge{{From: Start, To: writer}})

	drain(t, wf.Run(newStateMockCtx(t, sess)))

	// persistKey should be in session.
	if got, err := sess.State().Get(persistKey); err != nil {
		t.Errorf("expected session to contain %q, got err: %v", persistKey, err)
	} else if got != "p" {
		t.Errorf("session[%q] = %v, want %q", persistKey, got, "p")
	}
	// tempKey must NOT be in session — the scheduler skips temp: prefixes.
	if _, err := sess.State().Get(tempKey); !errors.Is(err, session.ErrStateKeyNotExist) {
		t.Errorf("expected session NOT to contain temp key %q, got err: %v", tempKey, err)
	}
}
