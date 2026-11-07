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
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/internal/assert"
)

const lifecycleProcedure = "/test.Lifecycle/Call"

// noopServerStream is a minimal ServerStream for driving Server.Call directly
// without a transport.
type noopServerStream struct{}

func (noopServerStream) Receive(any) error  { return io.EOF }
func (noopServerStream) SendHeaders() error { return nil }
func (noopServerStream) Send(any) error     { return nil }

func newLifecycleServer(t *testing.T, handler connect.ServerFunc, interceptors ...connect.ServerInterceptor) *connect.Server {
	t.Helper()
	server := connect.NewServer(interceptors...)
	server.Register(connect.Method{
		Spec: connect.Spec{
			Procedure:  lifecycleProcedure,
			StreamType: connect.StreamTypeUnary,
		},
		Handler: handler,
	})
	return server
}

// blockingHandler enters (buffered, never blocks) and blocks until release
// closes, counting starts and completions.
type blockingHandler struct {
	entered   chan struct{}
	release   chan struct{}
	started   atomic.Int64
	completed atomic.Int64
}

func newBlockingHandler() *blockingHandler {
	return &blockingHandler{
		entered: make(chan struct{}, 256),
		release: make(chan struct{}),
	}
}

func (h *blockingHandler) handler(context.Context, connect.Spec, connect.ServerStream) error {
	h.started.Add(1)
	h.entered <- struct{}{}
	<-h.release
	h.completed.Add(1)
	return nil
}

func TestServerDrainRejectsNewCallsBeforeUserCode(t *testing.T) {
	t.Parallel()
	var interceptorEntries atomic.Int64
	var handlerEntries atomic.Int64
	interceptor := func(next connect.ServerFunc) connect.ServerFunc {
		return func(ctx context.Context, spec connect.Spec, stream connect.ServerStream) error {
			interceptorEntries.Add(1)
			return next(ctx, spec, stream)
		}
	}
	server := newLifecycleServer(t,
		func(context.Context, connect.Spec, connect.ServerStream) error {
			handlerEntries.Add(1)
			return nil
		},
		interceptor,
	)

	// Drain is idempotent and irreversible.
	server.Drain()
	server.Drain()

	err := server.Call(t.Context(), lifecycleProcedure, nil, noopServerStream{})
	assert.Equal(t, connect.CodeOf(err), connect.CodeUnavailable)
	assert.Equal(t, interceptorEntries.Load(), int64(0))
	assert.Equal(t, handlerEntries.Load(), int64(0))

	// An idle, drained server shuts down immediately.
	assert.Nil(t, server.Shutdown(t.Context()))
}

func TestServerShutdownWaitsForAcceptedCall(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		handler := newBlockingHandler()
		server := newLifecycleServer(t, handler.handler)

		callDone := make(chan error, 1)
		go func() {
			callDone <- server.Call(context.Background(), lifecycleProcedure, nil, noopServerStream{})
		}()
		<-handler.entered

		// The accepted call keeps Shutdown waiting; the timeout is
		// recognizable and reports the still-active count.
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := server.Shutdown(ctx)
		cancel()
		var shutdownErr *connect.ShutdownError
		assert.True(t, errors.As(err, &shutdownErr))
		assert.Equal(t, shutdownErr.Active, 1)
		assert.ErrorIs(t, err, context.DeadlineExceeded)

		// While waiting, the acceptance boundary is already closed.
		rejected := server.Call(context.Background(), lifecycleProcedure, nil, noopServerStream{})
		assert.Equal(t, connect.CodeOf(rejected), connect.CodeUnavailable)
		assert.Equal(t, handler.completed.Load(), int64(0))

		close(handler.release)
		assert.Nil(t, <-callDone)
		assert.Equal(t, handler.completed.Load(), int64(1))

		// A second Shutdown with a fresh context observes the zero count.
		assert.Nil(t, server.Shutdown(context.Background()))
		postShutdown := server.Call(context.Background(), lifecycleProcedure, nil, noopServerStream{})
		assert.Equal(t, connect.CodeOf(postShutdown), connect.CodeUnavailable)
	})
}

func TestServerCallAcceptanceBoundaryRace(t *testing.T) {
	t.Parallel()
	const firstWave = 50
	const secondWave = 150
	handler := newBlockingHandler()
	server := newLifecycleServer(t, handler.handler)

	// First wave: every call is accepted and parks inside the handler, so
	// Drain overlaps calls that are genuinely in flight.
	var firstWG sync.WaitGroup
	var accepted atomic.Int64
	for range firstWave {
		firstWG.Go(func() {
			if err := server.Call(t.Context(), lifecycleProcedure, nil, noopServerStream{}); err != nil {
				t.Errorf("first-wave Call: %v", err)
				return
			}
			accepted.Add(1)
		})
	}
	for range firstWave {
		<-handler.entered
	}
	server.Drain()

	// Second wave: the boundary is already closed, so every call must be
	// rejected with CodeUnavailable and never reach the handler.
	var secondWG sync.WaitGroup
	var rejected atomic.Int64
	for range secondWave {
		secondWG.Go(func() {
			err := server.Call(t.Context(), lifecycleProcedure, nil, noopServerStream{})
			if connect.CodeOf(err) == connect.CodeUnavailable {
				rejected.Add(1)
				return
			}
			t.Errorf("second-wave Call = %v, want CodeUnavailable", err)
		})
	}
	secondWG.Wait()

	close(handler.release)
	firstWG.Wait()

	assert.Equal(t, accepted.Load(), int64(firstWave))
	assert.Equal(t, rejected.Load(), int64(secondWave))
	assert.Equal(t, handler.started.Load(), int64(firstWave))
	assert.Equal(t, handler.completed.Load(), int64(firstWave))
	assert.Nil(t, server.Shutdown(t.Context()))
}

func TestServerUnknownHandlerIsCounted(t *testing.T) {
	t.Parallel()
	t.Run("fallback_counts_as_active", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			handler := newBlockingHandler()
			server := connect.NewServer()
			server.SetUnknownHandler(handler.handler)
			const unknownProcedure = "/test.Lifecycle/Unknown"

			callDone := make(chan error, 1)
			go func() {
				callDone <- server.Call(context.Background(), unknownProcedure, nil, noopServerStream{})
			}()
			<-handler.entered

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			err := server.Shutdown(ctx)
			cancel()
			var shutdownErr *connect.ShutdownError
			assert.True(t, errors.As(err, &shutdownErr))
			assert.Equal(t, shutdownErr.Active, 1)

			close(handler.release)
			assert.Nil(t, <-callDone)
			assert.Nil(t, server.Shutdown(context.Background()))
		})
	})
	t.Run("default_unimplemented_releases_slot", func(t *testing.T) {
		t.Parallel()
		server := connect.NewServer()
		err := server.Call(t.Context(), "/test.Lifecycle/Missing", nil, noopServerStream{})
		assert.Equal(t, connect.CodeOf(err), connect.CodeUnimplemented)
		// The immediate rejection still passed through the acceptance
		// boundary, so its slot must already be gone.
		assert.Nil(t, server.Shutdown(t.Context()))
	})
}

func TestServerMultipleShutdownWaiters(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		handler := newBlockingHandler()
		server := newLifecycleServer(t, handler.handler)

		callDone := make(chan error, 1)
		go func() {
			callDone <- server.Call(context.Background(), lifecycleProcedure, nil, noopServerStream{})
		}()
		<-handler.entered

		type waitResult struct{ err error }
		results := make(chan waitResult, 3)
		ctx1, cancel1 := context.WithCancel(context.Background())
		ctx2, cancel2 := context.WithCancel(context.Background())
		ctx3, cancel3 := context.WithCancel(context.Background())
		go func() { results <- waitResult{server.Shutdown(ctx1)} }()
		go func() { results <- waitResult{server.Shutdown(ctx2)} }()
		go func() { results <- waitResult{server.Shutdown(ctx3)} }()
		synctest.Wait() // let every waiter park on the active count

		// Canceling one waiter ends only that waiter; the server stays
		// drained and the other waiters keep waiting.
		cancel2()
		canceled := <-results
		var shutdownErr *connect.ShutdownError
		assert.True(t, errors.As(canceled.err, &shutdownErr))
		assert.Equal(t, shutdownErr.Active, 1)
		assert.ErrorIs(t, canceled.err, context.Canceled)

		select {
		case result := <-results:
			t.Fatalf("another waiter returned early: %v", result.err)
		default:
		}

		// A timeout-based waiter arrives, gives up, and also leaves the
		// long-lived waiters untouched.
		probeCtx, probeCancel := context.WithTimeout(context.Background(), time.Second)
		probeErr := server.Shutdown(probeCtx)
		probeCancel()
		assert.ErrorIs(t, probeErr, context.DeadlineExceeded)
		select {
		case result := <-results:
			t.Fatalf("another waiter returned early: %v", result.err)
		default:
		}

		close(handler.release)
		assert.Nil(t, <-callDone)
		for range 2 {
			result := <-results
			assert.Nil(t, result.err)
		}
		cancel1()
		cancel3()

		// The drain state was never reset by any cancellation.
		err := server.Call(context.Background(), lifecycleProcedure, nil, noopServerStream{})
		assert.Equal(t, connect.CodeOf(err), connect.CodeUnavailable)
	})
}

func TestServerCancelActiveCancelsDerivedContextOnly(t *testing.T) {
	t.Parallel()
	parent, parentCancel := context.WithCancel(t.Context())
	t.Cleanup(parentCancel)

	entered := make(chan struct{}, 1)
	server := newLifecycleServer(t, func(ctx context.Context, _ connect.Spec, _ connect.ServerStream) error {
		entered <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	})

	callDone := make(chan error, 1)
	go func() {
		callDone <- server.Call(parent, lifecycleProcedure, nil, noopServerStream{})
	}()
	<-entered

	server.CancelActive()
	err := <-callDone
	assert.ErrorIs(t, err, context.Canceled)
	// The cancellation targets the per-call derived context, not the parent.
	assert.Nil(t, parent.Err())
	assert.Nil(t, server.Shutdown(t.Context()))
}

func TestServerCancelActiveDoesNotForceContextIgnorer(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		handler := newBlockingHandler()
		server := newLifecycleServer(t, handler.handler)

		callDone := make(chan error, 1)
		go func() {
			callDone <- server.Call(context.Background(), lifecycleProcedure, nil, noopServerStream{})
		}()
		<-handler.entered

		// The handler ignores its context, so cancellation cannot end it.
		server.CancelActive()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := server.Shutdown(ctx)
		cancel()
		var shutdownErr *connect.ShutdownError
		assert.True(t, errors.As(err, &shutdownErr))
		assert.Equal(t, shutdownErr.Active, 1)
		assert.Equal(t, handler.completed.Load(), int64(0))

		close(handler.release)
		assert.Nil(t, <-callDone)
		assert.Nil(t, server.Shutdown(context.Background()))
	})
}

func TestServerPanicReleasesActiveSlot(t *testing.T) {
	t.Parallel()
	server := newLifecycleServer(t, func(context.Context, connect.Spec, connect.ServerStream) error {
		panic("boom") //nolint:forbidigo // exercises panic cleanup on the acceptance boundary
	})

	assert.Panics(t, func() {
		_ = server.Call(t.Context(), lifecycleProcedure, nil, noopServerStream{})
	})

	// The panicked call no longer counts as active, and the server is not
	// drained by the panic: a later call is still accepted.
	assert.Nil(t, server.Shutdown(t.Context()))
}

func TestServerInterceptorShortCircuitReleasesSlot(t *testing.T) {
	t.Parallel()
	var handlerEntries atomic.Int64
	interceptor := func(next connect.ServerFunc) connect.ServerFunc {
		return func(context.Context, connect.Spec, connect.ServerStream) error {
			return connect.Errorf(connect.CodePermissionDenied, "short circuit")
		}
	}
	server := newLifecycleServer(t,
		func(context.Context, connect.Spec, connect.ServerStream) error {
			handlerEntries.Add(1)
			return nil
		},
		interceptor,
	)

	err := server.Call(t.Context(), lifecycleProcedure, nil, noopServerStream{})
	assert.Equal(t, connect.CodeOf(err), connect.CodePermissionDenied)
	assert.Equal(t, handlerEntries.Load(), int64(0))
	assert.Nil(t, server.Shutdown(t.Context()))
}

func TestServerDrainFromHandlerDoesNotDeadlock(t *testing.T) {
	t.Parallel()
	var server *connect.Server
	server = newLifecycleServer(t, func(context.Context, connect.Spec, connect.ServerStream) error {
		server.Drain() // draining from inside the accepted call must return
		return nil
	})

	assert.Nil(t, server.Call(t.Context(), lifecycleProcedure, nil, noopServerStream{}))
	err := server.Call(t.Context(), lifecycleProcedure, nil, noopServerStream{})
	assert.Equal(t, connect.CodeOf(err), connect.CodeUnavailable)
	assert.Nil(t, server.Shutdown(t.Context()))
}

func TestServerDerivedContextKeepsCallInfoIsolation(t *testing.T) {
	t.Parallel()
	const otherProcedure = "/test.Lifecycle/Other"
	infos := make(chan *connect.CallInfo, 2)
	release := make(chan struct{})

	server := connect.NewServer()
	record := func(procedure string) {
		server.Register(connect.Method{
			Spec: connect.Spec{Procedure: procedure, StreamType: connect.StreamTypeUnary},
			Handler: func(ctx context.Context, spec connect.Spec, _ connect.ServerStream) error {
				info, ok := connect.CallInfoForServerContext(ctx)
				assert.True(t, ok)
				assert.Equal(t, info.Spec.Procedure, procedure)
				infos <- info
				<-release
				return nil
			},
		})
	}
	record(lifecycleProcedure)
	record(otherProcedure)

	// Transports populate Spec on the per-call CallInfo before dispatch;
	// mimic that here.
	first := &connect.CallInfo{Spec: connect.Spec{Procedure: lifecycleProcedure}}
	second := &connect.CallInfo{Spec: connect.Spec{Procedure: otherProcedure}}
	var wg sync.WaitGroup
	wg.Go(func() {
		_ = server.Call(t.Context(), lifecycleProcedure, first, noopServerStream{})
	})
	wg.Go(func() {
		_ = server.Call(t.Context(), otherProcedure, second, noopServerStream{})
	})

	// The two handlers report on an unordered channel, so compare as a set:
	// each call must observe exactly its own CallInfo, never the other's.
	gotFirst := <-infos
	gotSecond := <-infos
	assert.True(t, gotFirst != gotSecond)
	observed := map[*connect.CallInfo]struct{}{}
	observed[gotFirst] = struct{}{}
	observed[gotSecond] = struct{}{}
	_, sawFirst := observed[first]
	_, sawSecond := observed[second]
	assert.True(t, sawFirst)
	assert.True(t, sawSecond)

	// Canceling the derived contexts leaves each call's CallInfo reachable
	// and its own; neither call is cross-canceled by the other's context.
	server.CancelActive()
	close(release)
	wg.Wait()
	assert.Nil(t, server.Shutdown(t.Context()))
}

func TestServerDrainWithNoCallsShutsDownCleanly(t *testing.T) {
	t.Parallel()
	server := connect.NewServer()
	server.Drain()
	assert.Nil(t, server.Shutdown(t.Context()))

	// A context already ended on an idle server still reports success:
	// nothing remains to wait for.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.Nil(t, server.Shutdown(ctx))
}
