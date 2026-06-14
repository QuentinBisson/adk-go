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

package agent

import (
	"context"
	"time"

	"google.golang.org/genai"

	"google.golang.org/adk/memory"
	"google.golang.org/adk/session"
	"google.golang.org/adk/tool/toolconfirmation"
)

// ContextMock is a permissive test double for the unified [Context] (and its
// aliases [ToolContext], [CallbackContext] and tool.Context).
//
// Embed it in a test fake and override only the methods your test cares about;
// every other method returns a zero value. Use it when unrelated calls on the
// context are harmless. If you instead want unexpected calls to fail the test,
// use [StrictContextMock], which panics on un-overridden methods.
type ContextMock struct{}

// WithAgentCancel implements [Context].
func (c *ContextMock) WithAgentCancel() (Context, context.CancelFunc) {
	return nil, nil
}

// WithAgentTimeout implements [Context].
func (c *ContextMock) WithAgentTimeout(timeout time.Duration) (Context, context.CancelFunc) {
	return nil, nil
}

// Actions implements [Context].
func (c *ContextMock) Actions() *session.EventActions {
	return nil
}

// Agent implements [Context].
func (c *ContextMock) Agent() Agent {
	return nil
}

// AgentName implements [Context].
func (c *ContextMock) AgentName() string {
	return ""
}

// AppName implements [Context].
func (c *ContextMock) AppName() string {
	return ""
}

// Artifacts implements [Context].
func (c *ContextMock) Artifacts() Artifacts {
	return nil
}

// Branch implements [Context].
func (c *ContextMock) Branch() string {
	return ""
}

// Deadline implements [Context].
func (c *ContextMock) Deadline() (deadline time.Time, ok bool) {
	panic("unimplemented")
}

// Done implements [Context].
func (c *ContextMock) Done() <-chan struct{} {
	panic("unimplemented")
}

// EndInvocation implements [Context].
func (c *ContextMock) EndInvocation() {
}

// Ended implements [Context].
func (c *ContextMock) Ended() bool {
	return false
}

// Err implements [Context].
func (c *ContextMock) Err() error {
	return nil
}

// FunctionCallID implements [Context].
func (c *ContextMock) FunctionCallID() string {
	return ""
}

// InvocationContext implements [Context].
func (c *ContextMock) InvocationContext() InvocationContext {
	return nil
}

// InvocationID implements [Context].
func (c *ContextMock) InvocationID() string {
	return ""
}

// IsolationScope implements [Context].
func (c *ContextMock) IsolationScope() string {
	return ""
}

// Memory implements [Context].
func (c *ContextMock) Memory() Memory {
	return nil
}

// Path implements [Context].
func (c *ContextMock) Path() string {
	return ""
}

// ReadonlyState implements [Context].
func (c *ContextMock) ReadonlyState() session.ReadonlyState {
	return nil
}

// RequestConfirmation implements [Context].
func (c *ContextMock) RequestConfirmation(hint string, payload any) error {
	return nil
}

// ResumedInput implements [Context].
func (c *ContextMock) ResumedInput(interruptID string) (any, bool) {
	return nil, false
}

// RunConfig implements [Context].
func (c *ContextMock) RunConfig() *RunConfig {
	return nil
}

// RunID implements [Context].
func (c *ContextMock) RunID() string {
	return ""
}

// SearchMemory implements [Context].
func (c *ContextMock) SearchMemory(ctx context.Context, query string) (*memory.SearchResponse, error) {
	return nil, nil
}

// Session implements [Context].
func (c *ContextMock) Session() session.Session {
	return nil
}

// SessionID implements [Context].
func (c *ContextMock) SessionID() string {
	return ""
}

// SetInvocationContext implements [Context].
func (c *ContextMock) SetInvocationContext(InvocationContext) {
}

// State implements [Context].
func (c *ContextMock) State() session.State {
	return nil
}

// SubScheduler implements [Context].
func (c *ContextMock) SubScheduler() DynamicSubScheduler {
	return nil
}

// ToolConfirmation implements [Context].
func (c *ContextMock) ToolConfirmation() *toolconfirmation.ToolConfirmation {
	return nil
}

// UserContent implements [Context].
func (c *ContextMock) UserContent() *genai.Content {
	return nil
}

// UserID implements [Context].
func (c *ContextMock) UserID() string {
	return ""
}

// Value implements [Context].
func (c *ContextMock) Value(key any) any {
	return nil
}

// WithBranch implements [Context].
func (c *ContextMock) WithBranch(branch string) Context {
	return nil
}

// WithContext implements [Context].
func (c *ContextMock) WithContext(ctx context.Context) InvocationContext {
	return nil
}

// WithContext implements [Context].
func (c *ContextMock) WithAgentContext(ctx context.Context) Context {
	return nil
}

func (c *ContextMock) OutputForAncestors() []string {
	return nil
}

var (
	_ Context           = (*ContextMock)(nil)
	_ InvocationContext = (*ContextMock)(nil)
)

// StrictContextMock is a strict test double for the unified [Context] (and its
// aliases [ToolContext], [CallbackContext] and tool.Context).
//
// Embed it in a test fake and override only the methods your test actually
// uses. Because it embeds the full Context surface, adding new methods to
// Context will NOT break embedders: they keep compiling and inherit the strict
// default below.
//
// Unlike [ContextMock] (which returns zero values), an un-overridden method
// panics with "not implemented". An unexpected call therefore fails the test
// loudly instead of silently returning a zero value, which is usually what you
// want from a focused unit test.
//
// The exception is the value-carrying part of [Context] that it inherits from
// the standard library's context.Context (Deadline, Done, Err and Value):
// those read from the supplied Ctx rather than panicking, so the mock carries a
// usable context payload. If Ctx is nil they panic like everything else.
type StrictContextMock struct {
	// Ctx supplies the values returned by Deadline, Done, Err and Value.
	Ctx context.Context
}

func (m *StrictContextMock) ctx() context.Context {
	if m.Ctx == nil {
		panic("agent.StrictContextMock: Ctx is nil")
	}
	return m.Ctx
}

// context.Context methods, served from Ctx instead of panicking.

// Deadline implements [Context].
func (m *StrictContextMock) Deadline() (time.Time, bool) { return m.ctx().Deadline() }

// Done implements [Context].
func (m *StrictContextMock) Done() <-chan struct{} { return m.ctx().Done() }

// Err implements [Context].
func (m *StrictContextMock) Err() error { return m.ctx().Err() }

// Value implements [Context].
func (m *StrictContextMock) Value(key any) any { return m.ctx().Value(key) }

// ReadonlyContext methods.

// UserContent implements [Context].
func (m *StrictContextMock) UserContent() *genai.Content { panic("not implemented") }

// InvocationID implements [Context].
func (m *StrictContextMock) InvocationID() string { panic("not implemented") }

// AgentName implements [Context].
func (m *StrictContextMock) AgentName() string { panic("not implemented") }

// ReadonlyState implements [Context].
func (m *StrictContextMock) ReadonlyState() session.ReadonlyState { panic("not implemented") }

// UserID implements [Context].
func (m *StrictContextMock) UserID() string { panic("not implemented") }

// AppName implements [Context].
func (m *StrictContextMock) AppName() string { panic("not implemented") }

// SessionID implements [Context].
func (m *StrictContextMock) SessionID() string { panic("not implemented") }

// Branch implements [Context].
func (m *StrictContextMock) Branch() string { panic("not implemented") }

// InvocationContext methods.

// Agent implements [Context].
func (m *StrictContextMock) Agent() Agent { panic("not implemented") }

// Artifacts implements [Context].
func (m *StrictContextMock) Artifacts() Artifacts { panic("not implemented") }

// Memory implements [Context].
func (m *StrictContextMock) Memory() Memory { panic("not implemented") }

// Session implements [Context].
func (m *StrictContextMock) Session() session.Session { panic("not implemented") }

// IsolationScope implements [Context].
func (m *StrictContextMock) IsolationScope() string { panic("not implemented") }

// RunConfig implements [Context].
func (m *StrictContextMock) RunConfig() *RunConfig { panic("not implemented") }

// EndInvocation implements [Context].
func (m *StrictContextMock) EndInvocation() { panic("not implemented") }

// Ended implements [Context].
func (m *StrictContextMock) Ended() bool { panic("not implemented") }

// WithContext implements [Context].
func (m *StrictContextMock) WithContext(context.Context) InvocationContext { panic("not implemented") }

// Context (tool/callback/node) methods.

// State implements [Context].
func (m *StrictContextMock) State() session.State { panic("not implemented") }

// FunctionCallID implements [Context].
func (m *StrictContextMock) FunctionCallID() string { panic("not implemented") }

// Actions implements [Context].
func (m *StrictContextMock) Actions() *session.EventActions { panic("not implemented") }

// SearchMemory implements [Context].
func (m *StrictContextMock) SearchMemory(context.Context, string) (*memory.SearchResponse, error) {
	panic("not implemented")
}

// ToolConfirmation implements [Context].
func (m *StrictContextMock) ToolConfirmation() *toolconfirmation.ToolConfirmation {
	panic("not implemented")
}

// RequestConfirmation implements [Context].
func (m *StrictContextMock) RequestConfirmation(hint string, payload any) error {
	panic("not implemented")
}

// ResumedInput implements [Context].
func (m *StrictContextMock) ResumedInput(interruptID string) (any, bool) { panic("not implemented") }

// Path implements [Context].
func (m *StrictContextMock) Path() string { panic("not implemented") }

// RunID implements [Context].
func (m *StrictContextMock) RunID() string { panic("not implemented") }

// WithBranch implements [Context].
func (m *StrictContextMock) WithBranch(branch string) Context { panic("not implemented") }

// SubScheduler implements [Context].
func (m *StrictContextMock) SubScheduler() DynamicSubScheduler { panic("not implemented") }

// InvocationContext implements [Context].
func (m *StrictContextMock) InvocationContext() InvocationContext { panic("not implemented") }

// SetInvocationContext implements [Context].
func (m *StrictContextMock) SetInvocationContext(InvocationContext) { panic("not implemented") }

// WithAgentContext implements [Context].
func (m *StrictContextMock) WithAgentContext(context.Context) Context { panic("not implemented") }

// WithAgentTimeout implements [Context].
func (m *StrictContextMock) WithAgentTimeout(time.Duration) (Context, context.CancelFunc) {
	panic("not implemented")
}

// WithAgentCancel implements [Context].
func (m *StrictContextMock) WithAgentCancel() (Context, context.CancelFunc) { panic("not implemented") }

// OutputForAncestors implements [Context].
func (m *StrictContextMock) OutputForAncestors() []string { panic("not implemented") }

var (
	_ Context           = (*StrictContextMock)(nil)
	_ InvocationContext = (*StrictContextMock)(nil)
)
