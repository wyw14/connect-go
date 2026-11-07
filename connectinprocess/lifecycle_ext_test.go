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
	"io"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connectinprocess"
	"connectrpc.com/connect/v2/internal/assert"
	pingv1 "connectrpc.com/connect/v2/internal/gen/connect/ping/v1"
	pingv1connect "connectrpc.com/connect/v2/internal/gen/connect/ping/v1/pingv1connect"
)

// inProcessLifecycleServer blocks Ping and CumSum on release and counts how
// often each handler starts and completes.
type inProcessLifecycleServer struct {
	pingv1connect.UnimplementedPingServiceHandler

	entered   chan struct{}
	release   chan struct{}
	started   atomic.Int64
	completed atomic.Int64
}

func (s *inProcessLifecycleServer) Ping(_ context.Context, req *pingv1.PingRequest) (*pingv1.PingResponse, error) {
	s.started.Add(1)
	s.entered <- struct{}{}
	<-s.release
	s.completed.Add(1)
	return &pingv1.PingResponse{Number: req.GetNumber(), Text: "ok"}, nil
}

func (s *inProcessLifecycleServer) CumSum(_ context.Context, stream pingv1connect.PingServiceCumSumServerStream) error {
	s.started.Add(1)
	s.entered <- struct{}{}
	for {
		req, err := stream.Receive()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			s.completed.Add(1)
			return err
		}
		if err := stream.Send(&pingv1.CumSumResponse{Sum: req.GetNumber()}); err != nil {
			s.completed.Add(1)
			return err
		}
	}
	<-s.release
	s.completed.Add(1)
	return nil
}

func newInProcessLifecycleClient(t *testing.T) (pingv1connect.PingServiceClient, *connect.Server, *inProcessLifecycleServer) {
	t.Helper()
	handler := &inProcessLifecycleServer{
		entered: make(chan struct{}, 4),
		release: make(chan struct{}),
	}
	server := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(server, handler)
	client := pingv1connect.NewPingServiceClient(connect.NewClient(connectinprocess.New(server)))
	return client, server, handler
}

// TestInProcessDrainRejectsNewCalls verifies the in-process entry shares the
// Server.Call acceptance boundary: new RPCs fail with CodeUnavailable while
// the accepted call still finishes exactly once.
func TestInProcessDrainRejectsNewCalls(t *testing.T) {
	t.Parallel()
	client, server, handler := newInProcessLifecycleClient(t)

	callDone := make(chan error, 1)
	go func() {
		_, err := client.Ping(t.Context(), &pingv1.PingRequest{Number: 7})
		callDone <- err
	}()
	<-handler.entered

	server.Drain()
	_, err := client.Ping(t.Context(), &pingv1.PingRequest{})
	assert.Equal(t, connect.CodeOf(err), connect.CodeUnavailable)

	close(handler.release)
	assert.Nil(t, <-callDone)
	assert.Nil(t, server.Shutdown(t.Context()))
	assert.Equal(t, handler.started.Load(), int64(1))
	assert.Equal(t, handler.completed.Load(), int64(1))
}

// TestInProcessDrainLongBidiStream keeps a bidi stream open across Drain and
// checks the old stream completes while new calls are refused.
func TestInProcessDrainLongBidiStream(t *testing.T) {
	t.Parallel()
	client, server, handler := newInProcessLifecycleClient(t)

	stream, err := client.CumSum(t.Context())
	if err != nil {
		t.Fatalf("CumSum: %v", err)
	}
	assert.Nil(t, stream.Send(&pingv1.CumSumRequest{Number: 5}))
	response, err := stream.Receive()
	assert.Nil(t, err)
	assert.Equal(t, response.GetSum(), int64(5))
	<-handler.entered

	server.Drain()
	_, err = client.Ping(t.Context(), &pingv1.PingRequest{})
	assert.Equal(t, connect.CodeOf(err), connect.CodeUnavailable)

	// End the request side, then release the handler so the response stream
	// can close and deliver io.EOF.
	assert.Nil(t, stream.CloseSend())
	close(handler.release)
	if _, err := stream.Receive(); !errors.Is(err, io.EOF) {
		t.Fatalf("Receive after CloseSend = %v, want io.EOF", err)
	}
	assert.Nil(t, server.Shutdown(t.Context()))
	assert.Equal(t, handler.started.Load(), int64(1))
	assert.Equal(t, handler.completed.Load(), int64(1))
}

// TestInProcessCancelActive verifies the derived context of an accepted call
// is canceled while the transport's parent context stays intact.
func TestInProcessCancelActive(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{}, 1)
	var completed atomic.Int64
	server := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(server, &pluggableInProcessPingServer{
		ping: func(ctx context.Context, _ *pingv1.PingRequest) (*pingv1.PingResponse, error) {
			entered <- struct{}{}
			<-ctx.Done()
			completed.Add(1)
			return nil, ctx.Err()
		},
	})
	client := pingv1connect.NewPingServiceClient(connect.NewClient(connectinprocess.New(server)))

	callDone := make(chan error, 1)
	go func() {
		_, err := client.Ping(t.Context(), &pingv1.PingRequest{})
		callDone <- err
	}()
	<-entered

	server.CancelActive()
	err := <-callDone
	assert.Equal(t, connect.CodeOf(err), connect.CodeCanceled)
	assert.Nil(t, server.Shutdown(t.Context()))
	assert.Equal(t, completed.Load(), int64(1))
}

// TestInProcessPanicReleasesSlot verifies a panicking handler still releases
// its active slot and leaves the server able to accept further calls.
func TestInProcessPanicReleasesSlot(t *testing.T) {
	t.Parallel()
	server := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(server, panicPingServer{})
	client := pingv1connect.NewPingServiceClient(connect.NewClient(connectinprocess.New(server)))

	_, err := client.Ping(t.Context(), &pingv1.PingRequest{})
	assert.NotNil(t, err)

	// The panicked call released its slot and did not drain the server:
	// further calls are still accepted (Fail fails on its own merits).
	_, err = client.Fail(t.Context(), &pingv1.FailRequest{})
	assert.Equal(t, connect.CodeOf(err), connect.CodeUnimplemented)
	assert.Nil(t, server.Shutdown(t.Context()))
}

// TestInProcessNestedCallIsolationUnderDrain verifies a client RPC issued
// from inside a handler keeps its own client CallInfo and is not attached to
// the inbound call, and that draining the front server does not reject the
// nested RPC dispatched to a different server.
func TestInProcessNestedCallIsolationUnderDrain(t *testing.T) {
	t.Parallel()
	probeServer := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(probeServer, pingServer{})
	probeClient := pingv1connect.NewPingServiceClient(connect.NewClient(connectinprocess.New(probeServer)))

	nestedDone := make(chan struct{})
	release := make(chan struct{})
	var observedProcedure atomic.Value // string
	frontServer := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(frontServer, &pluggableInProcessPingServer{
		ping: func(ctx context.Context, req *pingv1.PingRequest) (*pingv1.PingResponse, error) {
			info, ok := connect.CallInfoForServerContext(ctx)
			assert.True(t, ok)
			observedProcedure.Store(info.Spec.Procedure)
			// The outbound call must not inherit the inbound CallInfo and
			// must reach the probe server even once the front server drains.
			nested, err := probeClient.Ping(ctx, req)
			if err != nil {
				return nil, err
			}
			close(nestedDone)
			<-release
			return nested, nil
		},
	})
	frontClient := pingv1connect.NewPingServiceClient(connect.NewClient(connectinprocess.New(frontServer)))

	callDone := make(chan error, 1)
	go func() {
		_, err := frontClient.Ping(t.Context(), &pingv1.PingRequest{Number: 3})
		callDone <- err
	}()
	<-nestedDone

	// Drain the front server while its handler is still running. The nested
	// RPC targets the probe server, which stays open.
	frontServer.Drain()
	nestedAfterDrain, err := probeClient.Ping(t.Context(), &pingv1.PingRequest{Number: 4})
	assert.Nil(t, err)
	assert.Equal(t, nestedAfterDrain.GetNumber(), int64(4))

	_, err = frontClient.Ping(t.Context(), &pingv1.PingRequest{})
	assert.Equal(t, connect.CodeOf(err), connect.CodeUnavailable)

	close(release)
	assert.Nil(t, <-callDone)
	assert.Nil(t, frontServer.Shutdown(t.Context()))
	assert.Equal(t, observedProcedure.Load(), pingv1connect.PingServicePingProcedure)
	// The probe server was never drained and still serves calls.
	assert.Nil(t, probeServer.Shutdown(t.Context()))
}

// pluggableInProcessPingServer is a PingServiceHandler whose Ping method is
// supplied per test.
type pluggableInProcessPingServer struct {
	pingv1connect.UnimplementedPingServiceHandler

	ping func(context.Context, *pingv1.PingRequest) (*pingv1.PingResponse, error)
}

func (s *pluggableInProcessPingServer) Ping(ctx context.Context, req *pingv1.PingRequest) (*pingv1.PingResponse, error) {
	return s.ping(ctx, req)
}
