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

package connectinprocess_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"testing"
	"testing/synctest"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connectinprocess"
	pingv1 "connectrpc.com/connect/v2/internal/gen/connect/ping/v1"
	pingv1connect "connectrpc.com/connect/v2/internal/gen/connect/ping/v1/pingv1connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

const numberMessageSize int64 = 8

func boundedConfig(messages int, bytes int64) connectinprocess.BufferConfig {
	return connectinprocess.BufferConfig{MaxMessages: messages, MaxBytes: bytes}
}

func numberClone(src any) (any, error) {
	n, ok := src.(*number)
	if !ok {
		return nil, fmt.Errorf("numberClone: src is %T, not *number", src)
	}
	clone := *n
	return &clone, nil
}

func numberSize(msg any) (int64, error) {
	if _, ok := msg.(*number); !ok {
		return 0, fmt.Errorf("numberSize: msg is %T, not *number", msg)
	}
	return numberMessageSize, nil
}

func numberCopy(dst, src any) error {
	dstNumber, ok := dst.(*number)
	if !ok {
		return fmt.Errorf("numberCopy: dst is %T, not *number", dst)
	}
	srcNumber, ok := src.(*number)
	if !ok {
		return fmt.Errorf("numberCopy: src is %T, not *number", src)
	}
	*dstNumber = *srcNumber
	return nil
}

// newBoundedNumberStream builds a bidi stream over the non-proto number type
// with custom Clone, Size, and Copy functions.
func newBoundedNumberStream(t *testing.T, cfg connectinprocess.BufferConfig, handler connect.ServerFunc, opts ...connectinprocess.Option) connect.ClientStream {
	t.Helper()
	spec := connect.Spec{Procedure: "/test.Numbers/Bidi", StreamType: connect.StreamTypeBidi}
	server := connect.NewServer()
	server.Register(connect.Method{Spec: spec, Handler: handler})
	options := []connectinprocess.Option{
		connectinprocess.WithBoundedBuffers(cfg, cfg),
		connectinprocess.WithCloneFunc(numberClone),
		connectinprocess.WithSizeFunc(numberSize),
		connectinprocess.WithCopyFunc(numberCopy),
	}
	options = append(options, opts...)
	stream, err := connect.NewClient(connectinprocess.New(server, options...)).CallClientStream(t.Context(), spec)
	if err != nil {
		t.Fatalf("CallClientStream: %v", err)
	}
	t.Cleanup(func() { _ = stream.Close() })
	return stream
}

// TestBoundedAllStreamTypes verifies the default protobuf Clone and Size
// work for every stream cardinality.
func TestBoundedAllStreamTypes(t *testing.T) {
	t.Parallel()
	handler := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(handler, pingServer{})
	cfg := boundedConfig(8, 1<<20)
	client := pingv1connect.NewPingServiceClient(connect.NewClient(
		connectinprocess.New(handler, connectinprocess.WithBoundedBuffers(cfg, cfg)),
	))

	resp, err := client.Ping(t.Context(), &pingv1.PingRequest{Number: 42, Text: "hello"})
	if err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if got := resp.GetNumber(); got != 42 {
		t.Errorf("Ping Number = %d, want 42", got)
	}

	sum, err := client.Sum(t.Context())
	if err != nil {
		t.Fatalf("Sum: %v", err)
	}
	for _, n := range []int64{1, 2, 3} {
		if err := sum.Send(&pingv1.SumRequest{Number: n}); err != nil {
			t.Fatalf("Sum Send: %v", err)
		}
	}
	res, err := sum.CloseAndReceive()
	if err != nil {
		t.Fatalf("CloseAndReceive: %v", err)
	}
	if got := res.GetSum(); got != 6 {
		t.Errorf("Sum = %d, want 6", got)
	}

	up, err := client.CountUp(t.Context(), &pingv1.CountUpRequest{Number: 3})
	if err != nil {
		t.Fatalf("CountUp: %v", err)
	}
	for want := int64(1); ; want++ {
		msg, err := up.Receive()
		if errors.Is(err, io.EOF) {
			if want != 4 {
				t.Errorf("CountUp ended at %d, want 4", want)
			}
			break
		}
		if err != nil {
			t.Fatalf("CountUp Receive: %v", err)
		}
		if got := msg.GetNumber(); got != want {
			t.Errorf("CountUp Number = %d, want %d", got, want)
		}
	}

	bidi, err := client.CumSum(t.Context())
	if err != nil {
		t.Fatalf("CumSum: %v", err)
	}
	for _, n := range []int64{10, 20, 30} {
		if err := bidi.Send(&pingv1.CumSumRequest{Number: n}); err != nil {
			t.Fatalf("CumSum Send: %v", err)
		}
	}
	if err := bidi.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	wantSums := []int64{10, 30, 60}
	for i, want := range wantSums {
		msg, err := bidi.Receive()
		if err != nil {
			t.Fatalf("CumSum Receive[%d]: %v", i, err)
		}
		if got := msg.GetSum(); got != want {
			t.Errorf("CumSum Sum[%d] = %d, want %d", i, got, want)
		}
	}
	if _, err := bidi.Receive(); !errors.Is(err, io.EOF) {
		t.Errorf("final CumSum Receive = %v, want io.EOF", err)
	}
}

// TestBoundedSendReturnsBeforeReceive verifies Send returns once the copy is
// queued, before the handler has received anything.
func TestBoundedSendReturnsBeforeReceive(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		gate := make(chan struct{})
		received := make(chan *number, 1)
		stream := newBoundedNumberStream(t, boundedConfig(2, 64), func(_ context.Context, _ connect.Spec, s connect.ServerStream) error {
			<-gate
			req := &number{}
			if err := s.Receive(req); err != nil {
				return err
			}
			received <- req
			return nil
		})

		if err := stream.Send(&number{N: 7}); err != nil {
			t.Fatalf("Send: %v", err)
		}
		synctest.Wait()
		select {
		case req := <-received:
			t.Fatalf("handler received %v before it was released, want Send to buffer", req)
		default:
		}
		close(gate)
		synctest.Wait()
		select {
		case req := <-received:
			if req.N != 7 {
				t.Errorf("received N = %d, want 7", req.N)
			}
		default:
			t.Fatal("handler never received the buffered message")
		}
	})
}

// TestBoundedBothDirectionsBufferBeforeReceive verifies each side can enqueue
// several messages before either receives, and that FIFO order holds in both
// directions.
func TestBoundedBothDirectionsBufferBeforeReceive(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		stream := newBoundedNumberStream(t, boundedConfig(4, 1<<20), func(_ context.Context, _ connect.Spec, s connect.ServerStream) error {
			// Enqueue responses before consuming any request.
			for i := 1; i <= 3; i++ {
				if err := s.Send(&number{N: 100 + i}); err != nil {
					return err
				}
			}
			var got []int
			for {
				req := &number{}
				err := s.Receive(req)
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					return err
				}
				got = append(got, req.N)
			}
			want := []int{1, 2, 3}
			if len(got) != len(want) {
				return fmt.Errorf("handler got %v, want %v", got, want)
			}
			for i := range want {
				if got[i] != want[i] {
					return fmt.Errorf("handler got[%d] = %d, want %d", i, got[i], want[i])
				}
			}
			return nil
		})

		for i := 1; i <= 3; i++ {
			if err := stream.Send(&number{N: i}); err != nil {
				t.Fatalf("Send[%d]: %v", i, err)
			}
		}
		if err := stream.CloseSend(); err != nil {
			t.Fatalf("CloseSend: %v", err)
		}
		for want := 101; want <= 103; want++ {
			res := &number{}
			if err := stream.Receive(res); err != nil {
				t.Fatalf("Receive: %v", err)
			}
			if res.N != want {
				t.Errorf("response N = %d, want %d", res.N, want)
			}
		}
		if err := stream.Receive(&number{}); !errors.Is(err, io.EOF) {
			t.Errorf("final Receive = %v, want io.EOF", err)
		}
	})
}

// TestBoundedQuotaOneNoDeadlock verifies a one-message queue still makes
// progress for a ping-pong bidi RPC.
func TestBoundedQuotaOneNoDeadlock(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		stream := newBoundedNumberStream(t, boundedConfig(1, 1<<20), func(_ context.Context, _ connect.Spec, s connect.ServerStream) error {
			sum := 0
			for {
				req := &number{}
				err := s.Receive(req)
				if errors.Is(err, io.EOF) {
					return nil
				}
				if err != nil {
					return err
				}
				sum += req.N
				if err := s.Send(&number{N: sum}); err != nil {
					return err
				}
			}
		})
		for round := 1; round <= 5; round++ {
			if err := stream.Send(&number{N: round}); err != nil {
				t.Fatalf("Send[%d]: %v", round, err)
			}
			res := &number{}
			if err := stream.Receive(res); err != nil {
				t.Fatalf("Receive[%d]: %v", round, err)
			}
			want := round * (round + 1) / 2
			if res.N != want {
				t.Errorf("round %d: N = %d, want %d", round, res.N, want)
			}
		}
		if err := stream.CloseSend(); err != nil {
			t.Fatalf("CloseSend: %v", err)
		}
		if err := stream.Receive(&number{}); !errors.Is(err, io.EOF) {
			t.Errorf("final Receive = %v, want io.EOF", err)
		}
	})
}

// TestBoundedSlowConsumerRequest verifies a full queue blocks Send until the
// server receives, and the unblocked Send preserves order.
func TestBoundedSlowConsumerRequest(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		gate := make(chan struct{})
		got := make(chan int, 4)
		stream := newBoundedNumberStream(t, boundedConfig(2, 1<<20), func(_ context.Context, _ connect.Spec, s connect.ServerStream) error {
			<-gate
			for {
				req := &number{}
				err := s.Receive(req)
				if errors.Is(err, io.EOF) {
					return nil
				}
				if err != nil {
					return err
				}
				got <- req.N
			}
		})

		if err := stream.Send(&number{N: 1}); err != nil {
			t.Fatalf("Send 1: %v", err)
		}
		if err := stream.Send(&number{N: 2}); err != nil {
			t.Fatalf("Send 2: %v", err)
		}
		sendErr := make(chan error, 1)
		go func() { sendErr <- stream.Send(&number{N: 3}) }()
		synctest.Wait()
		select {
		case err := <-sendErr:
			t.Fatalf("third Send returned %v while the queue was full", err)
		default:
		}
		close(gate)
		if err := <-sendErr; err != nil {
			t.Fatalf("blocked Send: %v", err)
		}
		if err := stream.CloseSend(); err != nil {
			t.Fatalf("CloseSend: %v", err)
		}
		synctest.Wait()
		var want []int
		for n := 1; n <= 3; n++ {
			select {
			case n := <-got:
				want = append(want, n)
			default:
				t.Fatalf("missing message %d", n)
			}
		}
		for i, n := range []int{1, 2, 3} {
			if want[i] != n {
				t.Errorf("got[%d] = %d, want %d", i, want[i], n)
			}
		}
	})
}

// TestBoundedSlowConsumerResponse verifies a server Send blocks while the
// client is behind, then completes once the client receives.
func TestBoundedSlowConsumerResponse(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		firstSent := make(chan struct{})
		stream := newBoundedNumberStream(t, boundedConfig(1, 1<<20), func(_ context.Context, _ connect.Spec, s connect.ServerStream) error {
			if err := s.Send(&number{N: 1}); err != nil {
				return err
			}
			close(firstSent)
			return s.Send(&number{N: 2})
		})

		if err := stream.SendHeaders(); err != nil {
			t.Fatalf("SendHeaders: %v", err)
		}
		<-firstSent
		synctest.Wait()
		// The handler's second Send is blocked behind the one-message
		// response quota until the first message is received.
		res := &number{}
		if err := stream.Receive(res); err != nil {
			t.Fatalf("first Receive: %v", err)
		}
		if res.N != 1 {
			t.Errorf("first N = %d, want 1", res.N)
		}
		if err := stream.Receive(res); err != nil {
			t.Fatalf("second Receive: %v", err)
		}
		if res.N != 2 {
			t.Errorf("second N = %d, want 2", res.N)
		}
		if err := stream.Receive(&number{}); !errors.Is(err, io.EOF) {
			t.Errorf("final Receive = %v, want io.EOF", err)
		}
	})
}

// TestBoundedMessageReuseNoAlias verifies the queue holds independent copies:
// mutating a message after Send returns cannot change what the other side
// receives, in either direction.
func TestBoundedMessageReuseNoAlias(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		got := make(chan *number, 2)
		stream := newBoundedNumberStream(t, boundedConfig(4, 1<<20), func(_ context.Context, _ connect.Spec, s connect.ServerStream) error {
			req := &number{}
			if err := s.Receive(req); err != nil {
				return err
			}
			got <- req
			res := &number{N: 9}
			if err := s.Send(res); err != nil {
				return err
			}
			res.N = 90 // the queued copy must be unaffected
			return nil
		})

		req := &number{N: 7}
		if err := stream.Send(req); err != nil {
			t.Fatalf("Send: %v", err)
		}
		req.N = 70 // the queued copy must be unaffected
		synctest.Wait()
		select {
		case seen := <-got:
			if seen.N != 7 {
				t.Errorf("handler saw N = %d, want 7", seen.N)
			}
		default:
			t.Fatal("handler did not receive the request")
		}
		res := &number{}
		if err := stream.Receive(res); err != nil {
			t.Fatalf("Receive: %v", err)
		}
		if res.N != 9 {
			t.Errorf("client N = %d, want 9", res.N)
		}
	})
}

// TestBoundedCloneFailureReleasesQuota verifies a Clone error fails the Send,
// enqueues nothing, and leaves both quotas available for later messages.
func TestBoundedCloneFailureReleasesQuota(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		clone := func(src any) (any, error) {
			if n, ok := src.(*number); ok && n.N == 2 {
				return nil, errors.New("boom: clone refused")
			}
			return numberClone(src)
		}
		stream := newBoundedNumberStream(t, boundedConfig(4, 1<<20), func(_ context.Context, _ connect.Spec, s connect.ServerStream) error {
			for {
				req := &number{}
				err := s.Receive(req)
				if errors.Is(err, io.EOF) {
					return nil
				}
				if err != nil {
					return err
				}
			}
		}, connectinprocess.WithCloneFunc(clone))

		if err := stream.Send(&number{N: 1}); err != nil {
			t.Fatalf("Send 1: %v", err)
		}
		err := stream.Send(&number{N: 2})
		if err == nil || err.Error() != "boom: clone refused" {
			t.Fatalf("failing Send = %v, want the clone error", err)
		}
		// Quota was released: fill the two-message queue again.
		if err := stream.Send(&number{N: 3}); err != nil {
			t.Fatalf("Send 3: %v", err)
		}
		if err := stream.Send(&number{N: 4}); err != nil {
			t.Fatalf("Send 4: %v", err)
		}
		if err := stream.CloseSend(); err != nil {
			t.Fatalf("CloseSend: %v", err)
		}
	})
}

// TestBoundedSizeFailure verifies a Size error fails the Send before any
// quota is reserved or Clone runs.
func TestBoundedSizeFailure(t *testing.T) {
	t.Parallel()
	cloned := make(chan struct{}, 1)
	size := func(msg any) (int64, error) {
		if n, ok := msg.(*number); ok && n.N == 99 {
			return 0, errors.New("boom: size refused")
		}
		return numberSize(msg)
	}
	clone := func(src any) (any, error) {
		select {
		case cloned <- struct{}{}:
		default:
		}
		return numberClone(src)
	}
	stream := newBoundedNumberStream(t, boundedConfig(1, 1<<20), func(_ context.Context, _ connect.Spec, s connect.ServerStream) error {
		return s.Receive(&number{})
	}, connectinprocess.WithSizeFunc(size), connectinprocess.WithCloneFunc(clone))

	err := stream.Send(&number{N: 99})
	if err == nil || err.Error() != "boom: size refused" {
		t.Fatalf("Send = %v, want the size error", err)
	}
	select {
	case <-cloned:
		t.Fatal("Clone ran even though Size failed")
	default:
	}
	if err := stream.Send(&number{N: 1}); err != nil {
		t.Fatalf("follow-up Send: %v", err)
	}
}

// TestBoundedCopyFailure verifies a Copy error surfaces from Receive, consumes
// the queued message and its quota, and leaves the stream usable.
func TestBoundedCopyFailure(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		copyFn := func(dst, src any) error {
			if n, ok := src.(*number); ok && n.N == 2 {
				return errors.New("boom: copy refused")
			}
			return numberCopy(dst, src)
		}
		// The CopyFunc runs on the receiving side, so the server observes
		// each request-side transfer result.
		receiveErrs := make(chan error, 4)
		stream := newBoundedNumberStream(t, boundedConfig(4, 1<<20), func(_ context.Context, _ connect.Spec, s connect.ServerStream) error {
			for range 3 {
				receiveErrs <- s.Receive(&number{})
			}
			receiveErrs <- s.Receive(&number{})
			return nil
		}, connectinprocess.WithCopyFunc(copyFn))

		for _, n := range []int{1, 2, 3} {
			if err := stream.Send(&number{N: n}); err != nil {
				t.Fatalf("Send %d: %v", n, err)
			}
		}
		if err := stream.CloseSend(); err != nil {
			t.Fatalf("CloseSend: %v", err)
		}
		synctest.Wait()
		wantCopyErr := errors.New("boom: copy refused")
		wantErrs := []error{nil, wantCopyErr, nil, io.EOF}
		for i, want := range wantErrs {
			select {
			case got := <-receiveErrs:
				switch {
				case errors.Is(want, io.EOF):
					if !errors.Is(got, io.EOF) {
						t.Errorf("Receive[%d] = %v, want io.EOF", i, got)
					}
				case want == nil:
					if got != nil {
						t.Errorf("Receive[%d] = %v, want nil", i, got)
					}
				default:
					if got == nil || got.Error() != want.Error() {
						t.Errorf("Receive[%d] = %v, want %v", i, got, want)
					}
				}
			default:
				t.Fatalf("missing Receive result %d", i)
			}
		}
	})
}

// TestBoundedOversizeMessage verifies a message larger than the byte quota
// fails with CodeResourceExhausted before enqueue and never reaches the
// handler.
func TestBoundedOversizeMessage(t *testing.T) {
	t.Parallel()
	delivered := make(chan int, 2)
	stream := newBoundedNumberStream(t, boundedConfig(10, numberMessageSize/2), func(_ context.Context, _ connect.Spec, s connect.ServerStream) error {
		for {
			req := &number{}
			err := s.Receive(req)
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				return err
			}
			delivered <- req.N
		}
	})

	err := stream.Send(&number{N: 1})
	if got := connect.CodeOf(err); got != connect.CodeResourceExhausted {
		t.Fatalf("oversize Send code = %v, want CodeResourceExhausted", got)
	}
	select {
	case n := <-delivered:
		t.Fatalf("oversize message %d was enqueued", n)
	default:
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
}

// TestBoundedByteQuotaBlocks verifies the byte quota bounds the queue even
// when the message-count quota is far from exhausted.
func TestBoundedByteQuotaBlocks(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		blocked := make(chan error, 1)
		gate := make(chan struct{})
		stream := newBoundedNumberStream(t, boundedConfig(10, 2*numberMessageSize), func(_ context.Context, _ connect.Spec, s connect.ServerStream) error {
			<-gate
			for {
				req := &number{}
				err := s.Receive(req)
				if errors.Is(err, io.EOF) {
					return nil
				}
				if err != nil {
					return err
				}
			}
		})
		if err := stream.Send(&number{N: 1}); err != nil {
			t.Fatalf("Send 1: %v", err)
		}
		if err := stream.Send(&number{N: 2}); err != nil {
			t.Fatalf("Send 2: %v", err)
		}
		go func() { blocked <- stream.Send(&number{N: 3}) }()
		synctest.Wait()
		select {
		case err := <-blocked:
			t.Fatalf("third Send returned %v while byte quota was exhausted", err)
		default:
		}
		close(gate)
		if err := <-blocked; err != nil {
			t.Fatalf("blocked Send: %v", err)
		}
		if err := stream.CloseSend(); err != nil {
			t.Fatalf("CloseSend: %v", err)
		}
	})
}

// TestBoundedSendCanceledWhenFull verifies context cancellation ends a Send
// blocked on quota without corrupting the already queued messages.
func TestBoundedSendCanceledWhenFull(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		gate := make(chan struct{})
		got := make(chan int, 1)
		spec := connect.Spec{Procedure: "/test.Numbers/Cancel", StreamType: connect.StreamTypeBidi}
		server := connect.NewServer()
		server.Register(connect.Method{Spec: spec, Handler: func(_ context.Context, _ connect.Spec, s connect.ServerStream) error {
			<-gate
			req := &number{}
			if err := s.Receive(req); err != nil {
				return err
			}
			got <- req.N
			return nil
		}})
		transport := connectinprocess.New(server,
			connectinprocess.WithBoundedBuffers(boundedConfig(1, 1<<20), boundedConfig(1, 1<<20)),
			connectinprocess.WithCloneFunc(numberClone),
			connectinprocess.WithSizeFunc(numberSize),
			connectinprocess.WithCopyFunc(numberCopy),
		)
		ctx, cancel := context.WithCancel(t.Context())
		stream, err := connect.NewClient(transport).CallClientStream(ctx, spec)
		if err != nil {
			t.Fatalf("CallClientStream: %v", err)
		}
		if err := stream.Send(&number{N: 1}); err != nil {
			t.Fatalf("Send 1: %v", err)
		}
		sendErr := make(chan error, 1)
		go func() { sendErr <- stream.Send(&number{N: 2}) }()
		synctest.Wait()
		cancel()
		if err := <-sendErr; !errors.Is(err, context.Canceled) {
			t.Errorf("blocked Send = %v, want context.Canceled", err)
		}
		// The accepted message is still deliverable after the blocked Send
		// gave up.
		close(gate)
		synctest.Wait()
		select {
		case n := <-got:
			if n != 1 {
				t.Errorf("delivered N = %d, want 1", n)
			}
		default:
			t.Fatal("accepted message was lost after cancellation")
		}
	})
}

// TestBoundedReceiveCanceled verifies a Receive waiting on an empty queue ends
// promptly when the context cancels.
func TestBoundedReceiveCanceled(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		spec := connect.Spec{Procedure: "/test.Numbers/RecvCancel", StreamType: connect.StreamTypeBidi}
		server := connect.NewServer()
		server.Register(connect.Method{Spec: spec, Handler: func(ctx context.Context, _ connect.Spec, _ connect.ServerStream) error {
			<-ctx.Done()
			return nil
		}})
		transport := connectinprocess.New(server,
			connectinprocess.WithBoundedBuffers(boundedConfig(1, 1<<20), boundedConfig(1, 1<<20)),
			connectinprocess.WithCloneFunc(numberClone),
			connectinprocess.WithSizeFunc(numberSize),
			connectinprocess.WithCopyFunc(numberCopy),
		)
		ctx, cancel := context.WithCancel(t.Context())
		stream, err := connect.NewClient(transport).CallClientStream(ctx, spec)
		if err != nil {
			t.Fatalf("CallClientStream: %v", err)
		}
		recvErr := make(chan error, 1)
		go func() { recvErr <- stream.Receive(&number{}) }()
		synctest.Wait()
		cancel()
		if err := <-recvErr; !errors.Is(err, context.Canceled) {
			t.Errorf("Receive = %v, want context.Canceled", err)
		}
	})
}

// TestBoundedCloseSendDrains verifies CloseSend rejects new Sends with
// io.EOF but delivers every accepted request in order before the server's
// Receive reaches io.EOF.
func TestBoundedCloseSendDrains(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		got := make(chan int, 4)
		stream := newBoundedNumberStream(t, boundedConfig(4, 1<<20), func(_ context.Context, _ connect.Spec, s connect.ServerStream) error {
			for {
				req := &number{}
				err := s.Receive(req)
				if errors.Is(err, io.EOF) {
					return nil
				}
				if err != nil {
					return err
				}
				got <- req.N
			}
		})

		for _, n := range []int{1, 2, 3} {
			if err := stream.Send(&number{N: n}); err != nil {
				t.Fatalf("Send %d: %v", n, err)
			}
		}
		if err := stream.CloseSend(); err != nil {
			t.Fatalf("CloseSend: %v", err)
		}
		if err := stream.CloseSend(); err != nil {
			t.Errorf("second CloseSend = %v, want nil (idempotent)", err)
		}
		if err := stream.Send(&number{N: 4}); !errors.Is(err, io.EOF) {
			t.Errorf("Send after CloseSend = %v, want io.EOF", err)
		}
		synctest.Wait()
		for want := 1; want <= 3; want++ {
			select {
			case got := <-got:
				if got != want {
					t.Errorf("got %d, want %d", got, want)
				}
			default:
				t.Fatalf("missing accepted message %d", want)
			}
		}
	})
}

// TestBoundedServerErrorAfterQueuedResponses verifies the client drains every
// accepted response before observing the server's final error and trailers.
func TestBoundedServerErrorAfterQueuedResponses(t *testing.T) {
	t.Parallel()
	spec := connect.Spec{Procedure: "/test.Numbers/ServerError", StreamType: connect.StreamTypeServer}
	server := connect.NewServer()
	server.Register(connect.Method{Spec: spec, Handler: func(ctx context.Context, _ connect.Spec, s connect.ServerStream) error {
		info, _ := connect.CallInfoForServerContext(ctx)
		info.ResponseTrailer().Set("X-Audit", "done")
		if err := s.Receive(&number{}); err != nil {
			return err
		}
		for _, n := range []int{1, 2} {
			if err := s.Send(&number{N: n}); err != nil {
				return err
			}
		}
		return connect.NewError(connect.CodeAborted, "gave up")
	}})
	transport := connectinprocess.New(server,
		connectinprocess.WithBoundedBuffers(boundedConfig(4, 1<<20), boundedConfig(4, 1<<20)),
		connectinprocess.WithCloneFunc(numberClone),
		connectinprocess.WithSizeFunc(numberSize),
		connectinprocess.WithCopyFunc(numberCopy),
	)
	ctx, info := connect.NewClientContext(t.Context())
	stream, err := connect.NewClient(transport).CallServerStream(ctx, spec, &number{N: 0})
	if err != nil {
		t.Fatalf("CallServerStream: %v", err)
	}
	for want := 1; want <= 2; want++ {
		res := &number{}
		if err := stream.Receive(res); err != nil {
			t.Fatalf("Receive %d: %v", want, err)
		}
		if res.N != want {
			t.Errorf("N = %d, want %d", res.N, want)
		}
	}
	err = stream.Receive(&number{})
	if got := connect.CodeOf(err); got != connect.CodeAborted {
		t.Fatalf("final Receive code = %v, want CodeAborted (err=%v)", got, err)
	}
	if got := info.ResponseTrailer().Get("X-Audit"); got != "done" {
		t.Errorf("trailer = %q, want %q", got, "done")
	}
}

// TestBoundedServerErrorBeforeResponse verifies a handler that fails without
// enqueuing any response still surfaces its verdict to the client.
func TestBoundedServerErrorBeforeResponse(t *testing.T) {
	t.Parallel()
	spec := connect.Spec{Procedure: "/test.Numbers/Reject", StreamType: connect.StreamTypeServer}
	server := connect.NewServer()
	server.Register(connect.Method{Spec: spec, Handler: func(context.Context, connect.Spec, connect.ServerStream) error {
		return connect.NewError(connect.CodePermissionDenied, "nope")
	}})
	cfg := boundedConfig(2, 1<<20)
	transport := connectinprocess.New(server,
		connectinprocess.WithBoundedBuffers(cfg, cfg),
		connectinprocess.WithCloneFunc(numberClone),
		connectinprocess.WithSizeFunc(numberSize),
		connectinprocess.WithCopyFunc(numberCopy),
	)
	stream, err := connect.NewClient(transport).CallServerStream(t.Context(), spec, &number{N: 1})
	if err != nil {
		t.Fatalf("CallServerStream: %v", err)
	}
	err = stream.Receive(&number{})
	if got := connect.CodeOf(err); got != connect.CodePermissionDenied {
		t.Fatalf("Receive code = %v, want CodePermissionDenied (err=%v)", got, err)
	}
}

// TestBoundedCloseReleasesWithoutDrain verifies abandoning a buffered stream
// with Close unblocks the handler and frees every goroutine and queued
// message without reading to io.EOF.
func TestBoundedCloseReleasesWithoutDrain(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		handlerDone := make(chan struct{})
		spec := connect.Spec{Procedure: "/test.Numbers/Close", StreamType: connect.StreamTypeBidi}
		server := connect.NewServer()
		server.Register(connect.Method{Spec: spec, Handler: func(_ context.Context, _ connect.Spec, s connect.ServerStream) error {
			defer close(handlerDone)
			for {
				if err := s.Send(&number{N: 1}); err != nil {
					return err
				}
			}
		}})
		transport := connectinprocess.New(server,
			connectinprocess.WithBoundedBuffers(boundedConfig(4, 1<<20), boundedConfig(4, 1<<20)),
			connectinprocess.WithCloneFunc(numberClone),
			connectinprocess.WithSizeFunc(numberSize),
			connectinprocess.WithCopyFunc(numberCopy),
		)
		stream, err := connect.NewClient(transport).CallClientStream(t.Context(), spec)
		if err != nil {
			t.Fatalf("CallClientStream: %v", err)
		}
		for range 3 {
			if err := stream.Send(&number{N: 1}); err != nil {
				t.Fatalf("Send: %v", err)
			}
		}
		if err := stream.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if err := stream.Close(); err != nil {
			t.Errorf("second Close = %v, want nil (idempotent)", err)
		}
		if err := stream.Receive(&number{}); !errors.Is(err, context.Canceled) {
			t.Errorf("Receive after Close = %v, want context.Canceled", err)
		}
		synctest.Wait()
		select {
		case <-handlerDone:
		default:
			t.Error("handler goroutine did not return after Close")
		}
	})
}

// TestBoundedCloseWithoutUse verifies streams that are closed while holding
// queued messages do not leak goroutines.
//
//nolint:paralleltest // counts goroutines, which parallel siblings would skew
func TestBoundedCloseWithoutUse(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		handler := connect.NewServer()
		pingv1connect.RegisterPingServiceHandler(handler, pingServer{})
		cfg := boundedConfig(8, 1<<20)
		client := pingv1connect.NewPingServiceClient(connect.NewClient(
			connectinprocess.New(handler, connectinprocess.WithBoundedBuffers(cfg, cfg)),
		))

		before := runtime.NumGoroutine()
		for range 100 {
			stream, err := client.CumSum(t.Context())
			if err != nil {
				t.Fatalf("CumSum: %v", err)
			}
			for range 3 {
				if err := stream.Send(&pingv1.CumSumRequest{Number: 1}); err != nil {
					t.Fatalf("Send: %v", err)
				}
			}
			if err := stream.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
		}
		synctest.Wait()
		if got := runtime.NumGoroutine(); got != before {
			t.Fatalf("goroutines leaked: %d before, %d after", before, got)
		}
	})
}

// TestBoundedHeadersFixedAtFirstSend verifies response headers are fixed at
// the server's first Send and cannot be changed by later queued processing.
func TestBoundedHeadersFixedAtFirstSend(t *testing.T) {
	t.Parallel()
	spec := connect.Spec{Procedure: "/test.Headers/Bounded", StreamType: connect.StreamTypeServer}
	release := make(chan struct{})
	server := connect.NewServer()
	server.Register(connect.Method{Spec: spec, Handler: func(ctx context.Context, _ connect.Spec, s connect.ServerStream) error {
		if err := s.Receive(&number{}); err != nil {
			return err
		}
		info, _ := connect.CallInfoForServerContext(ctx)
		info.ResponseHeader().Set("Early", "1")
		if err := s.Send(&number{N: 1}); err != nil {
			return err
		}
		info.ResponseHeader().Set("Late", "1")
		info.ResponseTrailer().Set("Trailer", "1")
		<-release
		return nil
	}})
	cfg := boundedConfig(4, 1<<20)
	transport := connectinprocess.New(server,
		connectinprocess.WithBoundedBuffers(cfg, cfg),
		connectinprocess.WithCloneFunc(numberClone),
		connectinprocess.WithSizeFunc(numberSize),
		connectinprocess.WithCopyFunc(numberCopy),
	)
	ctx, info := connect.NewClientContext(t.Context())
	stream, err := connect.NewClient(transport).CallServerStream(ctx, spec, &number{N: 0})
	if err != nil {
		t.Fatalf("CallServerStream: %v", err)
	}
	if err := stream.Receive(&number{}); err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if got := info.ResponseHeader().Get("Early"); got != "1" {
		t.Errorf("Early header = %q, want %q", got, "1")
	}
	close(release)
	if err := stream.Receive(&number{}); !errors.Is(err, io.EOF) {
		t.Errorf("final Receive = %v, want io.EOF", err)
	}
	if got := info.ResponseHeader().Get("Late"); got != "" {
		t.Errorf("Late header = %q, want it dropped", got)
	}
	if got := info.ResponseTrailer().Get("Trailer"); got != "1" {
		t.Errorf("Trailer = %q, want %q", got, "1")
	}
}

// TestBoundedSlowCloneDoesNotBlockOtherDirection verifies Clone runs without
// the RPC-wide mutex: a blocked request-side Clone cannot stop a response
// from flowing.
func TestBoundedSlowCloneDoesNotBlockOtherDirection(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		cloneGate := make(chan struct{})
		clone := func(src any) (any, error) {
			if n, ok := src.(*number); ok && n.N == 1 {
				<-cloneGate
			}
			return numberClone(src)
		}
		got := make(chan *number, 1)
		stream := newBoundedNumberStream(t, boundedConfig(4, 1<<20), func(_ context.Context, _ connect.Spec, s connect.ServerStream) error {
			if err := s.Send(&number{N: 42}); err != nil {
				return err
			}
			req := &number{}
			if err := s.Receive(req); err != nil {
				return err
			}
			got <- req
			return nil
		}, connectinprocess.WithCloneFunc(clone))

		sendErr := make(chan error, 1)
		go func() { sendErr <- stream.Send(&number{N: 1}) }()
		synctest.Wait()
		// While the request Clone is blocked, the response still flows.
		res := &number{}
		if err := stream.Receive(res); err != nil {
			t.Fatalf("Receive while Clone blocked: %v", err)
		}
		if res.N != 42 {
			t.Errorf("response N = %d, want 42", res.N)
		}
		select {
		case <-got:
			t.Fatal("request was delivered while its Clone was blocked")
		default:
		}
		close(cloneGate)
		if err := <-sendErr; err != nil {
			t.Fatalf("blocked Send: %v", err)
		}
		synctest.Wait()
		select {
		case req := <-got:
			if req.N != 1 {
				t.Errorf("request N = %d, want 1", req.N)
			}
		default:
			t.Fatal("request was not delivered after Clone unblocked")
		}
	})
}

// TestBoundedInvalidConfig verifies unusable buffer limits fail when the
// stream is created rather than at the first Send.
func TestBoundedInvalidConfig(t *testing.T) {
	t.Parallel()
	server := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(server, pingServer{})
	spec := connect.Spec{
		StreamType: connect.StreamTypeUnary,
		Procedure:  pingv1connect.PingServicePingProcedure,
	}
	for _, tt := range []struct {
		name string
		cfg  connectinprocess.BufferConfig
		want string
	}{
		{"zero_messages", boundedConfig(0, 64), "MaxMessages"},
		{"negative_messages", boundedConfig(-1, 64), "MaxMessages"},
		{"zero_bytes", boundedConfig(1, 0), "MaxBytes"},
		{"negative_bytes", boundedConfig(1, -1), "MaxBytes"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			transport := connectinprocess.New(server, connectinprocess.WithBoundedBuffers(tt.cfg, boundedConfig(1, 64)))
			if _, err := transport.NewClientStream(t.Context(), spec); err == nil ||
				!strings.Contains(err.Error(), tt.want) {
				t.Errorf("NewClientStream err = %v, want error mentioning %s", err, tt.want)
			}
		})
	}
}

// TestBoundedNonProtoRequiresCustomFuncs verifies the default protobuf Clone
// and Size reject non-protobuf messages instead of assuming the schema.
func TestBoundedNonProtoRequiresCustomFuncs(t *testing.T) {
	t.Parallel()
	spec := connect.Spec{Procedure: "/test.Numbers/NonProto", StreamType: connect.StreamTypeBidi}
	server := connect.NewServer()
	server.Register(connect.Method{Spec: spec, Handler: func(_ context.Context, _ connect.Spec, s connect.ServerStream) error {
		return s.Receive(&number{})
	}})
	// Bounded, with only a CopyFunc: ProtoSize must reject the number type.
	transport := connectinprocess.New(server,
		connectinprocess.WithBoundedBuffers(boundedConfig(1, 64), boundedConfig(1, 64)),
		connectinprocess.WithCopyFunc(numberCopy),
	)
	stream, err := connect.NewClient(transport).CallClientStream(t.Context(), spec)
	if err != nil {
		t.Fatalf("CallClientStream: %v", err)
	}
	err = stream.Send(&number{N: 1})
	if err == nil || !strings.Contains(err.Error(), "proto.Message") {
		t.Fatalf("Send = %v, want a proto.Message error", err)
	}
}

// TestProtoCloneAndProtoSize verifies the default buffered-mode helpers.
func TestProtoCloneAndProtoSize(t *testing.T) {
	t.Parallel()
	t.Run("clone_is_independent", func(t *testing.T) {
		t.Parallel()
		src := &pingv1.PingRequest{Number: 1, Text: "one"}
		cloned, err := connectinprocess.ProtoClone(src)
		if err != nil {
			t.Fatalf("ProtoClone: %v", err)
		}
		cloneMsg, ok := cloned.(*pingv1.PingRequest)
		if !ok {
			t.Fatalf("cloned type = %T, want *PingRequest", cloned)
		}
		src.Number = 2
		src.Text = "two"
		if cloneMsg.GetNumber() != 1 || cloneMsg.GetText() != "one" {
			t.Errorf("clone = %v, want independent copy {1 one}", cloneMsg)
		}
	})
	t.Run("clone_dynamic", func(t *testing.T) {
		t.Parallel()
		desc := (&pingv1.PingRequest{}).ProtoReflect().Descriptor()
		src := dynamicpb.NewMessage(desc)
		src.Set(desc.Fields().ByName("number"), protoreflect.ValueOfInt64(5))
		src.Set(desc.Fields().ByName("text"), protoreflect.ValueOfString("dyn"))
		cloned, err := connectinprocess.ProtoClone(src)
		if err != nil {
			t.Fatalf("ProtoClone: %v", err)
		}
		clonedMsg, ok := cloned.(proto.Message)
		if !ok {
			t.Fatalf("cloned type = %T, want proto.Message", cloned)
		}
		if !proto.Equal(clonedMsg, src) {
			t.Errorf("dynamic clone = %v, want equal to src", cloned)
		}
	})
	t.Run("size_matches_proto", func(t *testing.T) {
		t.Parallel()
		msg := &pingv1.PingRequest{Number: 9, Text: "nine"}
		got, err := connectinprocess.ProtoSize(msg)
		if err != nil {
			t.Fatalf("ProtoSize: %v", err)
		}
		if want := int64(proto.Size(msg)); got != want {
			t.Errorf("ProtoSize = %d, want %d", got, want)
		}
	})
	t.Run("non_proto_errors", func(t *testing.T) {
		t.Parallel()
		if _, err := connectinprocess.ProtoClone(&number{N: 1}); err == nil ||
			!strings.Contains(err.Error(), "proto.Message") {
			t.Errorf("ProtoClone err = %v, want proto.Message error", err)
		}
		if _, err := connectinprocess.ProtoSize(&number{N: 1}); err == nil ||
			!strings.Contains(err.Error(), "proto.Message") {
			t.Errorf("ProtoSize err = %v, want proto.Message error", err)
		}
	})
}
