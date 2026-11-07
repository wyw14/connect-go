// Copyright 2021-2026 The Connect Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package connect_test

import (
	"context"
	"errors"
	"io"
	"iter"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connectinprocess"
	"connectrpc.com/connect/v2/internal/assert"
	pingv1 "connectrpc.com/connect/v2/internal/gen/connect/ping/v1"
)

// nopServerStream is a minimal [connect.ServerStream] for dispatcher tests
// whose handlers do not exchange messages.
type nopServerStream struct{}

func (nopServerStream) Receive(any) error  { return io.EOF }
func (nopServerStream) SendHeaders() error { return nil }
func (nopServerStream) Send(any) error     { return nil }

func noopHandler(context.Context, connect.Spec, connect.ServerStream) error { return nil }

func method(procedure string, handler connect.ServerFunc) connect.Method {
	return connect.Method{
		Spec: connect.Spec{
			Procedure:  procedure,
			StreamType: connect.StreamTypeUnary,
		},
		Handler: handler,
	}
}

func specProcedures(specs []connect.Spec) []string {
	procedures := make([]string, 0, len(specs))
	for _, spec := range specs {
		procedures = append(procedures, spec.Procedure)
	}
	return procedures
}

func collectSpecs(seq iter.Seq[connect.Spec]) []connect.Spec {
	var specs []connect.Spec
	for spec := range seq {
		specs = append(specs, spec)
	}
	return specs
}

// TestRegisterKeepsSetupBehavior verifies the setup-time Register API keeps
// its append ordering and duplicate panic on top of the new generational
// storage.
func TestRegisterKeepsSetupBehavior(t *testing.T) {
	t.Parallel()
	server := connect.NewServer()
	server.Register(method("/svc/A", noopHandler))
	server.Register(method("/svc/B", noopHandler))
	assert.Equal(t, specProcedures(collectSpecs(server.Specs())), []string{"/svc/A", "/svc/B"})
	assert.Equal(t, server.Revision(), connect.Revision(2))

	assert.Panics(t, func() {
		server.Register(method("/svc/A", noopHandler))
	})

	// A runtime replacement then a Register append coexist on the registry.
	rev, err := server.ReplaceMethods(method("/svc/B", noopHandler))
	assert.Nil(t, err)
	assert.Equal(t, rev, connect.Revision(3))
	server.Register(method("/svc/C", noopHandler))
	assert.Equal(t, specProcedures(collectSpecs(server.Specs())), []string{"/svc/B", "/svc/C"})
}

// TestReplaceMethodsLifecycle verifies add, replace, and remove procedures in
// complete generations and the resulting revisions and spec ordering.
func TestReplaceMethodsLifecycle(t *testing.T) {
	t.Parallel()
	server := connect.NewServer()
	rev1, err := server.ReplaceMethods(
		method("/svc/A", noopHandler),
		method("/svc/B", noopHandler),
	)
	assert.Nil(t, err)
	assert.Equal(t, rev1, connect.Revision(1))
	assert.Equal(t, specProcedures(collectSpecs(server.Specs())), []string{"/svc/A", "/svc/B"})

	// Replace A and add C: B is removed because the batch is the full table.
	rev2, err := server.ReplaceMethods(
		method("/svc/A", noopHandler),
		method("/svc/C", noopHandler),
	)
	assert.Nil(t, err)
	assert.Equal(t, rev2, connect.Revision(2))
	assert.Equal(t, specProcedures(collectSpecs(server.Specs())), []string{"/svc/A", "/svc/C"})

	// B is gone and dispatches unimplemented.
	err = server.Call(t.Context(), "/svc/B", nil, nopServerStream{})
	assert.Equal(t, connect.CodeOf(err), connect.CodeUnimplemented)
	// A still dispatches and runs the replacement handler without failing.
	err = server.Call(t.Context(), "/svc/A", nil, nopServerStream{})
	assert.Nil(t, err)

	// An empty batch is a valid removal of everything.
	rev3, err := server.ReplaceMethods()
	assert.Nil(t, err)
	assert.Equal(t, rev3, connect.Revision(3))
	assert.Equal(t, len(collectSpecs(server.Specs())), 0)
}

// TestReplaceMethodsValidation verifies every invalid batch is rejected as a
// whole without disturbing the published generation.
func TestReplaceMethodsValidation(t *testing.T) {
	t.Parallel()
	server := connect.NewServer()
	rev, err := server.ReplaceMethods(method("/svc/A", noopHandler))
	assert.Nil(t, err)

	cases := []struct {
		name    string
		methods []connect.Method
		want    string
	}{
		{
			name:    "nil handler",
			methods: []connect.Method{{Spec: connect.Spec{Procedure: "/svc/X", StreamType: connect.StreamTypeUnary}}},
			want:    "nil handler",
		},
		{
			name:    "empty procedure",
			methods: []connect.Method{{Spec: connect.Spec{StreamType: connect.StreamTypeUnary}, Handler: noopHandler}},
			want:    "procedure is empty",
		},
		{
			name:    "missing leading slash",
			methods: []connect.Method{method("svc/X", noopHandler)},
			want:    "must start with '/'",
		},
		{
			name:    "missing method segment",
			methods: []connect.Method{method("/svc/", noopHandler)},
			want:    "missing the method segment",
		},
		{
			name: "whitespace",
			methods: []connect.Method{{
				Spec:    connect.Spec{Procedure: "/svc/A B", StreamType: connect.StreamTypeUnary},
				Handler: noopHandler,
			}},
			want: "whitespace",
		},
		{
			name: "duplicate procedure",
			methods: []connect.Method{
				method("/svc/X", noopHandler),
				method("/svc/X", noopHandler),
			},
			want: "duplicate procedure",
		},
		{
			name: "invalid stream type",
			methods: []connect.Method{{
				Spec:    connect.Spec{Procedure: "/svc/X", StreamType: connect.StreamType(99)},
				Handler: noopHandler,
			}},
			want: "invalid stream type",
		},
		{
			name: "invalid idempotency",
			methods: []connect.Method{{
				Spec: connect.Spec{
					Procedure:        "/svc/X",
					StreamType:       connect.StreamTypeUnary,
					IdempotencyLevel: connect.IdempotencyLevel(99),
				},
				Handler: noopHandler,
			}},
			want: "invalid idempotency level",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, gotErr := server.ReplaceMethods(tc.methods...)
			assert.NotNil(t, gotErr)
			if !strings.Contains(gotErr.Error(), tc.want) {
				t.Errorf("error %q does not contain %q", gotErr.Error(), tc.want)
			}
			// The failed publication changed neither revision nor the table.
			assert.Equal(t, server.Revision(), rev)
			assert.Nil(t, server.Call(t.Context(), "/svc/A", nil, nopServerStream{}))
		})
	}

	// All problems in a batch are reported together.
	_, err = server.ReplaceMethods(
		method("bad", noopHandler),
		connect.Method{
			Spec:    connect.Spec{Procedure: "/svc/", StreamType: connect.StreamTypeUnary},
			Handler: noopHandler,
		},
	)
	assert.NotNil(t, err)
	if !strings.Contains(err.Error(), "index 0") || !strings.Contains(err.Error(), "index 1") {
		t.Errorf("joined error %q should mention both invalid indexes", err.Error())
	}
	assert.Equal(t, server.Revision(), rev)
}

// TestReplaceMethodsIfRevision verifies compare-revision publications and
// ensures a losing publisher never rolls back a winner's generation.
func TestReplaceMethodsIfRevision(t *testing.T) {
	t.Parallel()
	server := connect.NewServer()
	rev0 := server.Revision()

	rev1, err := server.ReplaceMethodsIfRevision(rev0, method("/svc/A", noopHandler))
	assert.Nil(t, err)
	assert.Equal(t, rev1, connect.Revision(1))

	// A stale expected revision conflicts and changes nothing.
	current, err := server.ReplaceMethodsIfRevision(rev0, method("/svc/B", noopHandler))
	assert.ErrorIs(t, err, connect.ErrRevisionConflict)
	assert.Equal(t, current, rev1)
	_, ok := server.Snapshot().Spec("/svc/A")
	assert.True(t, ok)
	_, ok = server.Snapshot().Spec("/svc/B")
	assert.False(t, ok)

	// A malformed batch on the current revision also changes nothing.
	current, err = server.ReplaceMethodsIfRevision(rev1, method("no-slash", noopHandler))
	assert.NotNil(t, err)
	assert.Equal(t, current, rev1)
	assert.Equal(t, server.Revision(), rev1)

	// A matching revision publishes.
	rev2, err := server.ReplaceMethodsIfRevision(rev1, method("/svc/C", noopHandler))
	assert.Nil(t, err)
	assert.Equal(t, rev2, connect.Revision(2))
	_, ok = server.Snapshot().Spec("/svc/C")
	assert.True(t, ok)
	_, ok = server.Snapshot().Spec("/svc/A")
	assert.False(t, ok)
}

// TestSnapshotCallPinnedAfterRemoval verifies a snapshot keeps dispatching a
// procedure the current generation removed, while fresh lookups fail.
func TestSnapshotCallPinnedAfterRemoval(t *testing.T) {
	t.Parallel()
	var oldCalls atomic.Int64
	server := connect.NewServer()
	_, err := server.ReplaceMethods(method("/svc/A", func(context.Context, connect.Spec, connect.ServerStream) error {
		oldCalls.Add(1)
		return nil
	}))
	assert.Nil(t, err)
	snapshot := server.Snapshot()
	pinnedSpec, ok := snapshot.Spec("/svc/A")
	assert.True(t, ok)

	_, err = server.ReplaceMethods()
	assert.Nil(t, err)
	_, ok = server.Snapshot().Spec("/svc/A")
	assert.False(t, ok)
	assert.Equal(t, connect.CodeOf(server.Call(t.Context(), "/svc/A", nil, nopServerStream{})), connect.CodeUnimplemented)

	// The pinned snapshot still resolves and dispatches the old generation.
	stillPinned, ok := snapshot.Spec("/svc/A")
	assert.True(t, ok)
	assert.Equal(t, stillPinned, pinnedSpec)
	assert.Nil(t, snapshot.Call(t.Context(), "/svc/A", nil, nopServerStream{}))
	assert.Equal(t, oldCalls.Load(), int64(1))
}

// TestCallPinsGenerationDuringReplace runs an RPC blocked inside its handler
// while the procedure is removed and re-added with a different spec and
// handler. The in-flight RPC must finish on the generation it entered with.
func TestCallPinsGenerationDuringReplace(t *testing.T) {
	t.Parallel()
	const procedure = "/test.Generation/Ping"
	entered := make(chan struct{})
	release := make(chan struct{})
	var observed atomic.Pointer[connect.Spec]

	oldHandler := func(_ context.Context, spec connect.Spec, stream connect.ServerStream) error {
		specCopy := spec
		observed.Store(&specCopy)
		var req pingv1.PingRequest
		if err := stream.Receive(&req); err != nil {
			return err
		}
		close(entered)
		<-release
		return stream.Send(&pingv1.PingResponse{Number: 1, Text: "old"})
	}
	server := connect.NewServer()
	_, err := server.ReplaceMethods(connect.Method{
		Spec: connect.Spec{
			Procedure:        procedure,
			StreamType:       connect.StreamTypeUnary,
			IdempotencyLevel: connect.IdempotencyNoSideEffects,
			Schema:           "old-schema",
		},
		Handler: oldHandler,
	})
	assert.Nil(t, err)

	client := connect.NewClient(connectinprocess.New(server))
	type callResult struct {
		res *pingv1.PingResponse
		err error
	}
	resultCh := make(chan callResult, 1)
	go func() {
		res := &pingv1.PingResponse{}
		err := client.CallUnary(
			t.Context(),
			connect.Spec{Procedure: procedure, StreamType: connect.StreamTypeUnary},
			&pingv1.PingRequest{Number: 41},
			res,
		)
		resultCh <- callResult{res: res, err: err}
	}()
	<-entered

	// Remove the procedure, then re-add it with a bidi spec, new schema, and
	// a new handler while the old RPC is still blocked.
	_, err = server.ReplaceMethods()
	assert.Nil(t, err)
	var newObserved atomic.Pointer[connect.Spec]
	newHandler := func(_ context.Context, spec connect.Spec, stream connect.ServerStream) error {
		specCopy := spec
		newObserved.Store(&specCopy)
		var req pingv1.PingRequest
		for {
			err := stream.Receive(&req)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return err
			}
		}
		return stream.Send(&pingv1.PingResponse{Number: 2, Text: "new"})
	}
	_, err = server.ReplaceMethods(connect.Method{
		Spec: connect.Spec{
			Procedure:  procedure,
			StreamType: connect.StreamTypeBidi,
			Schema:     "new-schema",
		},
		Handler: newHandler,
	})
	assert.Nil(t, err)

	close(release)
	result := <-resultCh
	assert.Nil(t, result.err)
	assert.Equal(t, result.res.GetText(), "old")
	assert.Equal(t, result.res.GetNumber(), int64(1))
	oldSpec := observed.Load()
	assert.NotNil(t, oldSpec)
	assert.Equal(t, oldSpec.StreamType, connect.StreamTypeUnary)
	assert.Equal(t, oldSpec.Schema, "old-schema")

	// New RPCs observe the new generation: bidi shape, new handler and schema.
	stream, err := client.CallClientStream(
		t.Context(),
		connect.Spec{Procedure: procedure, StreamType: connect.StreamTypeBidi},
	)
	assert.Nil(t, err)
	assert.Nil(t, stream.Send(&pingv1.PingRequest{Number: 7}))
	assert.Nil(t, stream.CloseSend())
	var res pingv1.PingResponse
	assert.Nil(t, stream.Receive(&res))
	assert.Equal(t, res.GetText(), "new")
	assert.Equal(t, res.GetNumber(), int64(2))
	assert.Nil(t, stream.Close())
	newSpec := newObserved.Load()
	assert.NotNil(t, newSpec)
	assert.Equal(t, newSpec.StreamType, connect.StreamTypeBidi)
	assert.Equal(t, newSpec.Schema, "new-schema")
}

// TestInterceptorWrapsOncePerGeneration verifies the interceptor chain is
// applied while building each generation, never on snapshot reads or calls.
func TestInterceptorWrapsOncePerGeneration(t *testing.T) {
	t.Parallel()
	var wraps atomic.Int64
	var calls atomic.Int64
	interceptor := func(next connect.ServerFunc) connect.ServerFunc {
		wraps.Add(1)
		return func(ctx context.Context, spec connect.Spec, stream connect.ServerStream) error {
			calls.Add(1)
			return next(ctx, spec, stream)
		}
	}
	server := connect.NewServer(interceptor)
	mk := func(procedure string) connect.Method { return method(procedure, noopHandler) }

	_, err := server.ReplaceMethods(mk("/svc/A"), mk("/svc/B"))
	assert.Nil(t, err)
	assert.Equal(t, wraps.Load(), int64(2))

	// Snapshot and iterator reads do not wrap anything.
	for range 10 {
		snapshot := server.Snapshot()
		_ = snapshot.Revision()
		_, _ = snapshot.Spec("/svc/A")
		if len(collectSpecs(snapshot.Specs())) != 2 {
			t.Fatal("snapshot must keep both methods")
		}
		if len(collectSpecs(server.Specs())) != 2 {
			t.Fatal("server must keep both methods")
		}
	}
	assert.Equal(t, wraps.Load(), int64(2))

	// Each call traverses the chain exactly once.
	for range 5 {
		assert.Nil(t, server.Call(t.Context(), "/svc/A", nil, nopServerStream{}))
	}
	assert.Equal(t, calls.Load(), int64(5))
	assert.Equal(t, wraps.Load(), int64(2))

	// Republishing wraps the new batch once; old wrapped handlers are reused
	// nowhere.
	_, err = server.ReplaceMethods(mk("/svc/A"), mk("/svc/B"))
	assert.Nil(t, err)
	assert.Equal(t, wraps.Load(), int64(4))

	// Register wraps only the newly appended method.
	server.Register(mk("/svc/C"))
	assert.Equal(t, wraps.Load(), int64(5))
}

// TestSpecsAndCallsConcurrentWithReplace hammers publications, snapshots,
// iterators, and calls concurrently. It is primarily exercised under -race:
// every iterator must observe one complete, internally consistent generation.
func TestSpecsAndCallsConcurrentWithReplace(t *testing.T) {
	t.Parallel()
	server := connect.NewServer()
	_, err := server.ReplaceMethods(method("/svc/A", noopHandler))
	assert.Nil(t, err)

	const goroutines = 4
	const iterations = 200
	var wg sync.WaitGroup
	stop := make(chan struct{})

	for range goroutines {
		wg.Go(func() {
			for range iterations {
				select {
				case <-stop:
					return
				default:
				}
				// A is always present so concurrent callers never hit a
				// missing procedure due to scheduling.
				_, _ = server.ReplaceMethods(method("/svc/A", noopHandler))
			}
		})
	}
	for range goroutines {
		wg.Go(func() {
			for range iterations {
				select {
				case <-stop:
					return
				default:
				}
				snapshot := server.Snapshot()
				first := collectSpecs(snapshot.Specs())
				second := collectSpecs(snapshot.Specs())
				if !slices.EqualFunc(first, second, func(a, b connect.Spec) bool { return a == b }) {
					t.Errorf("snapshot generation changed during iteration: %v vs %v", first, second)
					close(stop)
					return
				}
				for _, spec := range first {
					got, ok := snapshot.Spec(spec.Procedure)
					if !ok || got != spec {
						t.Errorf("snapshot lookup inconsistent for %q", spec.Procedure)
						close(stop)
						return
					}
				}
			}
		})
	}
	for range goroutines {
		wg.Go(func() {
			for range iterations {
				select {
				case <-stop:
					return
				default:
				}
				if err := server.Call(t.Context(), "/svc/A", nil, nopServerStream{}); err != nil {
					t.Errorf("Call: %v", err)
					close(stop)
					return
				}
			}
		})
	}
	wg.Wait()
}

// TestUnknownHandlerConcurrentWithReplace verifies the fallback is read
// atomically and applies to generations without the requested procedure.
func TestUnknownHandlerConcurrentWithReplace(t *testing.T) {
	t.Parallel()
	server := connect.NewServer()
	_, err := server.ReplaceMethods(method("/svc/A", noopHandler))
	assert.Nil(t, err)
	var fallbackCalls atomic.Int64
	server.SetUnknownHandler(func(context.Context, connect.Spec, connect.ServerStream) error {
		fallbackCalls.Add(1)
		return nil
	})

	assert.Nil(t, server.Call(t.Context(), "/svc/Missing", nil, nopServerStream{}))
	assert.Equal(t, fallbackCalls.Load(), int64(1))

	// A fresh generation without A still falls through to the same handler.
	_, err = server.ReplaceMethods(method("/svc/B", noopHandler))
	assert.Nil(t, err)
	assert.Nil(t, server.Call(t.Context(), "/svc/A", nil, nopServerStream{}))
	assert.Equal(t, fallbackCalls.Load(), int64(2))

	// Clearing the fallback restores CodeUnimplemented.
	server.SetUnknownHandler(nil)
	assert.Equal(t, connect.CodeOf(server.Call(t.Context(), "/svc/A", nil, nopServerStream{})), connect.CodeUnimplemented)
}
