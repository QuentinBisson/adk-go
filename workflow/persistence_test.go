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
	"iter"
	"testing"

	"google.golang.org/adk/session"
)

// sliceEvents adapts a []*session.Event to the session.Events interface
// for tests; the session package exposes no constructor for a raw slice.
type sliceEvents []*session.Event

func (e sliceEvents) Len() int                { return len(e) }
func (e sliceEvents) At(i int) *session.Event { return e[i] }
func (e sliceEvents) All() iter.Seq[*session.Event] {
	return func(yield func(*session.Event) bool) {
		for _, ev := range e {
			if !yield(ev) {
				return
			}
		}
	}
}

// TestCollectNodeOutputs_OutputWithNilNodeInfo guards that an output
// event with a nil NodeInfo (a non-workflow output event) is attributed
// via Author without dereferencing NodeInfo.OutputFor and panicking.
func TestCollectNodeOutputs_OutputWithNilNodeInfo(t *testing.T) {
	nodesByName := map[string]Node{"nodeA": &dummyNode{name: "nodeA"}}

	events := sliceEvents{
		{Author: "nodeA", Output: "result-A", NodeInfo: nil},
	}

	outputs, completed := collectNodeOutputs(events, nodesByName)

	if got, want := outputs["nodeA"], "result-A"; got != want {
		t.Errorf("outputs[nodeA] = %v, want %v", got, want)
	}
	if !completed["nodeA"] {
		t.Errorf("completed[nodeA] = false, want true")
	}
}

// TestCollectNodeOutputs_DelegatedOutputRecoveredByStaticOwner mirrors a
// real runtime delegation event: an orchestrator static node "orch"
// delegates its output down a single-rooted dynamic chain via
// WithUseAsOutput. The runtime emits exactly one event whose Path and
// every OutputFor entry share the same root segment ("orch"), because a
// child path is always its parent path plus a suffix
// (dynamic_scheduler.go: childPath = parentPath + "/" + name + "@" +
// runID). On resume, "orch" recovers its output from that single event.
func TestCollectNodeOutputs_DelegatedOutputRecoveredByStaticOwner(t *testing.T) {
	nodesByName := map[string]Node{"orch": &dummyNode{name: "orch"}}

	events := sliceEvents{
		{
			Author: "orch",
			Output: "delegated",
			NodeInfo: &session.NodeInfo{
				Path:      "orch/middle@1/inner@1",
				OutputFor: []string{"orch/middle@1/inner@1", "orch/middle@1", "orch"},
			},
		},
	}

	outputs, _ := collectNodeOutputs(events, nodesByName)

	if got, want := outputs["orch"], "delegated"; got != want {
		t.Errorf("outputs[orch] = %v, want %v", got, want)
	}
}

// TestCollectNodeOutputs_OutputForAttributesForeignStaticOwner exercises
// the forward-looking branch in collectNodeOutputs that attributes a
// delegated output to a static node whose name does NOT match the
// emitting event's own static owner.
//
// The current runtime cannot produce such an event: delegation only
// flows up a single-rooted chain, so every OutputFor entry shares the
// emitting event's root segment (see
// TestCollectNodeOutputs_DelegatedOutputRecoveredByStaticOwner). This
// test hand-builds the cross-owner case to lock in the behavior in case
// a future mechanism delegates output to a foreign static node.
func TestCollectNodeOutputs_OutputForAttributesForeignStaticOwner(t *testing.T) {
	nodesByName := map[string]Node{
		"emitter": &dummyNode{name: "emitter"},
		"foreign": &dummyNode{name: "foreign"},
	}

	events := sliceEvents{
		{
			Author: "emitter",
			Output: "delegated",
			NodeInfo: &session.NodeInfo{
				Path:      "emitter/child@1",
				OutputFor: []string{"emitter/child@1", "foreign/child@1"},
			},
		},
	}

	outputs, _ := collectNodeOutputs(events, nodesByName)

	if got, want := outputs["emitter"], "delegated"; got != want {
		t.Errorf("outputs[emitter] = %v, want %v", got, want)
	}
	if got, want := outputs["foreign"], "delegated"; got != want {
		t.Errorf("outputs[foreign] = %v, want %v (OutputFor did not attribute to foreign static owner)", got, want)
	}
}
