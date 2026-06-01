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
	"iter"

	"google.golang.org/adk/agent"
	"google.golang.org/adk/session"
)

// NodeContext is the per-node context seen inside Node.Run and inside
// dynamic-node orchestrator bodies. It extends agent.InvocationContext
// with workflow-specific accessors.
//
// TODO(wolo): unify with the in-flight context-unification work
// (CallbackContext / ToolContext series).
type NodeContext interface {
	agent.InvocationContext

	// ResumedInput returns the response payload for a re-entry resume
	// activation keyed by InterruptID, or (nil, false) otherwise.
	ResumedInput(interruptID string) (any, bool)

	// Path returns the composite path of the currently-executing node.
	// Empty for top-level static nodes; "<parent_path>/<child_name>@<run_id>"
	// for dynamic children.
	Path() string

	// RunID returns the per-invocation identifier. Empty for top-level
	// static nodes; auto-counter or user-supplied via WithRunID for
	// dynamic children.
	RunID() string

	// WithBranch returns a NodeContext whose Branch() returns the
	// given value; all other fields (path, runID, subScheduler,
	// resumeInputs, embedded InvocationContext) are preserved.
	WithBranch(branch string) NodeContext

	// State returns the per-activation State wrapper that writes
	// through to both (a) a pending EventActions.StateDelta that
	// the scheduler folds onto the next emitted event (or a
	// synthetic trailing event at completion), and (b) the
	// underlying session.State for immediate read-after-write
	// visibility within the same node body.
	//
	// This is the canonical channel for inter-node state passing
	// — equivalent to Python's ctx.state["k"]=v idiom. Direct
	// writes to Session().State() bypass the event-delta pipeline
	// and will not be visible in event history or persisted by
	// non-inmemory session backends; prefer State().
	State() session.State
}

// nodeContext is the unexported NodeContext implementation.
type nodeContext struct {
	agent.InvocationContext

	// resumeInputs are keyed by InterruptID. Nil on fresh activations
	// and on handoff resume.
	resumeInputs map[string]any

	// path and runID are populated for dynamic children, empty for
	// top-level static activations.
	path  string
	runID string

	// subScheduler is non-nil only when this context belongs to a
	// dynamic-node activation; RunNode uses it to schedule children.
	subScheduler *dynamicSubScheduler

	// actions accumulates per-activation state mutations made via
	// State().Set(...). The scheduler producer (runNode) folds the
	// accumulated StateDelta onto the next emitted event before
	// sending it to the consumer; any remainder at completion is
	// flushed as a synthetic trailing event. Allocated lazily on
	// first use to avoid overhead for nodes that never touch state.
	actions *session.EventActions
}

// Compile-time: *nodeContext implements NodeContext.
var _ NodeContext = (*nodeContext)(nil)

// newNodeContext wraps parent for a top-level (static) activation.
func newNodeContext(parent agent.InvocationContext, resumeInputs map[string]any) *nodeContext {
	return &nodeContext{
		InvocationContext: parent,
		resumeInputs:      resumeInputs,
	}
}

// newDynamicNodeContext wraps parent for either a dynamic-node
// activation or one of its children, attaching path, runID, and the
// sub-scheduler RunNode reaches from the orchestrator body. Children
// pass the sub-scheduler's counter (or WithRunID) value as runID; a
// dynamic node's own activation passes runID="" — it is not itself a
// sub-scheduler child. Child inherits resumeInputs so HITL responses
// reach dynamic children.
func newDynamicNodeContext(parent NodeContext, path, runID string, sub *dynamicSubScheduler) *nodeContext {
	var inherited map[string]any
	if p, ok := parent.(*nodeContext); ok {
		inherited = p.resumeInputs
	}
	return &nodeContext{
		InvocationContext: parent,
		resumeInputs:      inherited,
		path:              path,
		runID:             runID,
		subScheduler:      sub,
	}
}

func (c *nodeContext) ResumedInput(interruptID string) (any, bool) {
	if c.resumeInputs == nil {
		return nil, false
	}
	v, ok := c.resumeInputs[interruptID]
	return v, ok
}

func (c *nodeContext) Path() string  { return c.path }
func (c *nodeContext) RunID() string { return c.runID }

// State returns the per-activation State wrapper. See NodeContext.State
// docstring for the contract. The same wrapper instance is returned on
// every call (with lazy allocation of the underlying EventActions on
// first write), so callers can hold the reference across yields.
func (c *nodeContext) State() session.State {
	return &nodeContextState{ctx: c}
}

// pendingStateDelta returns the accumulator's StateDelta map, allocating
// it lazily on first access. The returned map is the live accumulator
// — callers that fold it onto an outgoing event MUST clone before
// crossing a goroutine boundary (or call clearPendingStateDelta
// afterwards to detach the map).
//
// Returns nil iff the actions field has never been allocated and the
// allocate flag is false; this lets read paths avoid allocation when
// nothing has been written.
func (c *nodeContext) pendingStateDelta(allocate bool) map[string]any {
	if c.actions == nil {
		if !allocate {
			return nil
		}
		c.actions = &session.EventActions{StateDelta: make(map[string]any)}
	} else if c.actions.StateDelta == nil {
		if !allocate {
			return nil
		}
		c.actions.StateDelta = make(map[string]any)
	}
	return c.actions.StateDelta
}

// clearPendingStateDelta drops the accumulator's StateDelta map after
// it has been folded onto an emitted event, so that subsequent writes
// accumulate into a fresh map (and the already-emitted event keeps
// the snapshot it received).
func (c *nodeContext) clearPendingStateDelta() {
	if c.actions != nil {
		c.actions.StateDelta = nil
	}
}

func (c *nodeContext) WithBranch(branch string) NodeContext {
	// Reuse the package-level withBranch helper to swap Branch on
	// the underlying InvocationContext; preserve the NodeContext
	// envelope (path, runID, resumeInputs, subScheduler, actions)
	// unchanged. actions is shared by reference so writes through
	// either NodeContext flow into the same per-activation
	// accumulator.
	return &nodeContext{
		InvocationContext: withBranch(c.InvocationContext, branch),
		resumeInputs:      c.resumeInputs,
		path:              c.path,
		runID:             c.runID,
		subScheduler:      c.subScheduler,
		actions:           c.actions,
	}
}

// WithContext preserves the nodeContext wrapper when callers derive
// a new context from this one (e.g. when the scheduler attaches an
// OpenTelemetry span context). Without this override, the base
// invocationContext.WithContext would return a *invocationContext
// and silently drop the resumeInputs map, breaking re-entry resume
// activations and any other workflow-specific accessors.
func (c *nodeContext) WithContext(ctx context.Context) agent.InvocationContext {
	return &nodeContext{
		InvocationContext: c.InvocationContext.WithContext(ctx),
		resumeInputs:      c.resumeInputs,
		path:              c.path,
		runID:             c.runID,
		subScheduler:      c.subScheduler,
		actions:           c.actions,
	}
}

// nodeContextState implements session.State backed by a workflow
// nodeContext. Writes are recorded both on a per-activation
// EventActions accumulator (later folded onto an emitted event by
// the scheduler) AND on the underlying session.State (for immediate
// read-after-write visibility within the same node body).
//
// Mirrors the pattern of agent/agent.go's callbackContextState; the
// two will likely be unified by the upcoming context-unification
// refactor (TODO above NodeContext).
type nodeContextState struct {
	ctx *nodeContext
}

// Get returns the value for key, preferring the per-activation
// accumulator over the underlying session.State. Returns the
// session.State's error (typically ErrStateKeyNotExist) when neither
// holds the key.
func (s *nodeContextState) Get(key string) (any, error) {
	if delta := s.ctx.pendingStateDelta(false); delta != nil {
		if val, ok := delta[key]; ok {
			return val, nil
		}
	}
	return s.ctx.InvocationContext.Session().State().Get(key)
}

// Set records key=val on the per-activation accumulator and also
// writes through to the underlying session.State so subsequent
// Get calls (within the same node body, before any flush) observe
// the new value.
func (s *nodeContextState) Set(key string, val any) error {
	delta := s.ctx.pendingStateDelta(true)
	delta[key] = val
	return s.ctx.InvocationContext.Session().State().Set(key, val)
}

// All iterates the underlying session.State; pending accumulator
// writes are already mirrored there by Set, so this view is
// consistent with subsequent reads.
func (s *nodeContextState) All() iter.Seq2[string, any] {
	return s.ctx.InvocationContext.Session().State().All()
}

// Compile-time: *nodeContextState implements session.State.
var _ session.State = (*nodeContextState)(nil)
