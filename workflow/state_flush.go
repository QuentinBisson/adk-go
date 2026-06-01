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
	"strings"

	"google.golang.org/adk/agent"
	"google.golang.org/adk/session"
)

// flushPendingStateOnto folds the accumulator's pending StateDelta
// (writes recorded via NodeContext.State().Set during the current node
// activation) onto ev.Actions.StateDelta and clears the accumulator.
// The accumulator's map is detached from the node context first so the
// emitted event holds its own snapshot — safe to ship across the
// producer→consumer goroutine boundary.
//
// No-op if nc is not a *nodeContext, if ev is nil, or if there are no
// pending writes. Existing keys on ev.Actions.StateDelta are preserved;
// pending writes overwrite them on collision (later write wins).
func flushPendingStateOnto(nc agent.InvocationContext, ev *session.Event) {
	if ev == nil {
		return
	}
	pc, ok := nc.(*nodeContext)
	if !ok {
		return
	}
	delta := pc.pendingStateDelta(false)
	if len(delta) == 0 {
		// Nothing pending — fast path. Note: a present-but-empty
		// map still counts as zero len; we don't allocate an
		// empty StateDelta on ev in that case.
		return
	}
	// Detach the accumulator's map from the node context BEFORE
	// merging — clearPendingStateDelta sets nc.actions.StateDelta=nil
	// so subsequent writes in the same activation accumulate into a
	// fresh map and don't mutate the snapshot we just shipped.
	pc.clearPendingStateDelta()

	if ev.Actions.StateDelta == nil {
		ev.Actions.StateDelta = delta
		return
	}
	for k, v := range delta {
		ev.Actions.StateDelta[k] = v
	}
}

// mergeStateDeltaIntoSession applies the session-scoped entries of
// ev.Actions.StateDelta to sess.State() so a downstream node activated
// by the scheduler observes the writes through its own State().Get
// calls — even when the workflow is iterated directly (without a
// runner.Runner that would otherwise call SessionService.AppendEvent
// and trigger the same merge).
//
// Keys with the temp: prefix are skipped — they are invocation-scoped
// ephemeral values that must not pollute the persistent session.
// Keys with app: / user: prefixes are written through as-is; this
// matches inmemory.AppendEvent's behaviour (updateSessionState does
// maps.Copy across all non-temp keys without scope splitting).
//
// No-op if ev is nil, has no StateDelta, or if sess is nil. Set
// errors are silently dropped — this is a best-effort fast path for
// the iterator-only flow; the runner-driven flow has its own merge
// in SessionService.AppendEvent that also produces an error path.
func mergeStateDeltaIntoSession(sess session.Session, ev *session.Event) {
	if sess == nil || ev == nil || len(ev.Actions.StateDelta) == 0 {
		return
	}
	st := sess.State()
	if st == nil {
		return
	}
	for k, v := range ev.Actions.StateDelta {
		if strings.HasPrefix(k, session.KeyPrefixTemp) {
			continue
		}
		// Best-effort: the iterator-only path doesn't have a
		// natural error channel here; failures (e.g. schema
		// validation when we add it) will be re-surfaced by
		// the runner's AppendEvent if one is in the loop.
		_ = st.Set(k, v)
	}
}

// synthesizePendingStateEvent returns a synthetic state-only event
// carrying any unflushed StateDelta from nc, or nil if there is
// nothing to flush. Used by producers to ensure writes made via
// NodeContext.State().Set without a subsequent yield still reach the
// consumer (and the session) before the node completes.
//
// The returned event has Author set to authorName (the node name) and
// no Content / Output / Routes — it is a pure side-effect event. Branch
// stamping is left to the consumer's handleEvent path so this helper
// stays branch-agnostic.
func synthesizePendingStateEvent(nc agent.InvocationContext, authorName string) *session.Event {
	pc, ok := nc.(*nodeContext)
	if !ok {
		return nil
	}
	delta := pc.pendingStateDelta(false)
	if len(delta) == 0 {
		return nil
	}
	pc.clearPendingStateDelta()
	ev := session.NewEvent(nc.InvocationID())
	ev.Author = authorName
	// Reuse the detached delta map directly; clearPendingStateDelta
	// already severed it from the node context. NewEvent allocates
	// a fresh StateDelta, which we overwrite here.
	ev.Actions.StateDelta = delta
	return ev
}
