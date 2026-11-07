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

package connecthttp_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	"connectrpc.com/connect/v2/internal/assert"
	pingv1 "connectrpc.com/connect/v2/internal/gen/connect/ping/v1"
	pingv1connect "connectrpc.com/connect/v2/internal/gen/connect/ping/v1/pingv1connect"
	"connectrpc.com/connect/v2/internal/memhttp/memhttptest"
)

// newLifecycleHTTPClient mounts server on an in-memory HTTP server and returns
// a generated client talking to it over the wire, plus the underlying generic
// client for raw procedure calls.
func newLifecycleHTTPClient(t *testing.T, server *connect.Server) (pingv1connect.PingServiceClient, *connect.Client) {
	t.Helper()
	mux := http.NewServeMux()
	connecthttp.Mount(mux, server)
	httpServer := memhttptest.NewServer(t, mux)
	rawClient := connect.NewClient(connecthttp.NewTransport(httpServer.Client(), httpServer.URL()))
	return pingv1connect.NewPingServiceClient(rawClient), rawClient
}

// TestHTTPServerDrainRejectsNewCalls verifies the HTTP entry shares the
// Server.Call acceptance boundary: a drained server rejects new RPCs on the
// wire with CodeUnavailable, while the one accepted call runs to completion
// and Shutdown returns only after its handler exits.
func TestHTTPServerDrainRejectsNewCalls(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var started atomic.Int64
	var completed atomic.Int64
	server := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(server, &pluggablePingServer{
		ping: func(ctx context.Context, req *pingv1.PingRequest) (*pingv1.PingResponse, error) {
			started.Add(1)
			entered <- struct{}{}
			<-release
			completed.Add(1)
			return &pingv1.PingResponse{Number: req.GetNumber(), Text: "ok"}, nil
		},
	})
	client, _ := newLifecycleHTTPClient(t, server)

	callDone := make(chan error, 1)
	go func() {
		_, err := client.Ping(t.Context(), &pingv1.PingRequest{Number: 7})
		callDone <- err
	}()
	<-entered

	server.Drain()
	_, err := client.Ping(t.Context(), &pingv1.PingRequest{})
	assert.Equal(t, connect.CodeOf(err), connect.CodeUnavailable)

	// The accepted call still counts as active while its handler runs.
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	shutdownErr := server.Shutdown(ctx)
	cancel()
	var typed *connect.ShutdownError
	assert.True(t, errors.As(shutdownErr, &typed))
	assert.Equal(t, typed.Active, 1)
	assert.ErrorIs(t, shutdownErr, context.DeadlineExceeded)

	close(release)
	assert.Nil(t, <-callDone)
	assert.Nil(t, server.Shutdown(t.Context()))
	assert.Equal(t, started.Load(), int64(1))
	assert.Equal(t, completed.Load(), int64(1))
}

// TestHTTPServerDrainLongBidiStream keeps a bidirectional stream open across
// Drain: the stream stays usable until the client ends it, new RPCs are
// rejected, and Shutdown waits for the bidi handler to return.
func TestHTTPServerDrainLongBidiStream(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var completed atomic.Int64
	server := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(server, &pluggablePingServer{
		cumSum: func(ctx context.Context, stream pingv1connect.PingServiceCumSumServerStream) error {
			entered <- struct{}{}
			for {
				req, err := stream.Receive()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					return err
				}
				if err := stream.Send(&pingv1.CumSumResponse{Sum: req.GetNumber()}); err != nil {
					return err
				}
			}
			<-release
			completed.Add(1)
			return nil
		},
	})
	client, _ := newLifecycleHTTPClient(t, server)

	stream, err := client.CumSum(t.Context())
	if err != nil {
		t.Fatalf("CumSum: %v", err)
	}
	if err := stream.Send(&pingv1.CumSumRequest{Number: 5}); err != nil {
		// HTTP/1.x cannot carry a bidi stream (505 + connection close).
		t.Skipf("bidirectional streaming requires HTTP/2: %v", err)
	}
	response, err := stream.Receive()
	if err != nil {
		t.Skipf("bidirectional streaming requires HTTP/2: %v", err)
	}
	assert.Equal(t, response.GetSum(), int64(5))
	<-entered

	server.Drain()
	_, err = client.Ping(t.Context(), &pingv1.PingRequest{})
	assert.Equal(t, connect.CodeOf(err), connect.CodeUnavailable)

	// End the request side, then release the handler so the transport can
	// finalize the response stream and deliver io.EOF.
	assert.Nil(t, stream.CloseSend())
	close(release)
	for {
		if _, err := stream.Receive(); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatalf("Receive: %v", err)
		}
	}
	assert.Nil(t, server.Shutdown(t.Context()))
	assert.Equal(t, completed.Load(), int64(1))
}

// TestHTTPServerUnknownMethodCounted verifies the unknown-method fallback
// route keeps the same active-count semantics as a registered procedure.
func TestHTTPServerUnknownMethodCounted(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var completed atomic.Int64
	server := connect.NewServer()
	// A registered service is required for the service-level catch-all route.
	pingv1connect.RegisterPingServiceHandler(server, &pluggablePingServer{})
	server.SetUnknownHandler(func(ctx context.Context, spec connect.Spec, stream connect.ServerStream) error {
		entered <- struct{}{}
		var req pingv1.PingRequest
		if err := stream.Receive(&req); err != nil {
			return err
		}
		<-release
		completed.Add(1)
		return stream.Send(&pingv1.PingResponse{Number: req.GetNumber(), Text: "fallback"})
	})
	_, rawClient := newLifecycleHTTPClient(t, server)

	unknownSpec := connect.Spec{
		StreamType: connect.StreamTypeUnary,
		Procedure:  "/connect.ping.v1.PingService/DoesNotExist",
	}
	callDone := make(chan error, 1)
	go func() {
		callDone <- rawClient.CallUnary(
			t.Context(),
			unknownSpec,
			&pingv1.PingRequest{Number: 11},
			&pingv1.PingResponse{},
		)
	}()
	<-entered

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	err := server.Shutdown(ctx)
	cancel()
	var typed *connect.ShutdownError
	assert.True(t, errors.As(err, &typed))
	assert.Equal(t, typed.Active, 1)

	close(release)
	assert.Nil(t, <-callDone)
	assert.Nil(t, server.Shutdown(t.Context()))
	assert.Equal(t, completed.Load(), int64(1))
}

// TestHTTPServerCancelActive verifies canceling the accepted calls' derived
// contexts unblocks a cooperative handler over the wire, and the slot is
// released once the handler returns.
func TestHTTPServerCancelActive(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{}, 1)
	var completed atomic.Int64
	server := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(server, &pluggablePingServer{
		ping: func(ctx context.Context, _ *pingv1.PingRequest) (*pingv1.PingResponse, error) {
			entered <- struct{}{}
			<-ctx.Done()
			completed.Add(1)
			return nil, ctx.Err()
		},
	})
	client, _ := newLifecycleHTTPClient(t, server)

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
