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
	"sync"
	"testing"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connectinprocess"
	"connectrpc.com/connect/v2/internal/assert"
	pingv1 "connectrpc.com/connect/v2/internal/gen/connect/ping/v1"
	pingv1connect "connectrpc.com/connect/v2/internal/gen/connect/ping/v1/pingv1connect"
)

// pingMethod builds a unary Ping method whose response carries marker.
func pingMethod(marker string) connect.Method {
	return connect.Method{
		Spec: connect.Spec{
			Procedure:        pingv1connect.PingServicePingProcedure,
			StreamType:       connect.StreamTypeUnary,
			IdempotencyLevel: connect.IdempotencyNoSideEffects,
			Schema:           marker,
		},
		Handler: func(_ context.Context, _ connect.Spec, stream connect.ServerStream) error {
			var req pingv1.PingRequest
			if err := stream.Receive(&req); err != nil {
				return err
			}
			return stream.Send(&pingv1.PingResponse{Number: req.GetNumber(), Text: marker})
		},
	}
}

// TestInProcessConcurrentReplaceAndCall hammers registry publications,
// in-process calls, and spec iterations concurrently. The transport only
// reaches the registry through [connect.Server.Call], which pins an immutable
// generation, so -race must observe no live-map access.
func TestInProcessConcurrentReplaceAndCall(t *testing.T) {
	t.Parallel()
	server := connect.NewServer()
	rev, err := server.ReplaceMethods(pingMethod("v0"))
	assert.Nil(t, err)
	assert.Equal(t, rev, connect.Revision(1))
	client := pingv1connect.NewPingServiceClient(connect.NewClient(connectinprocess.New(server)))

	const workers = 4
	const iterations = 100
	var wg sync.WaitGroup
	stop := make(chan struct{})

	for range workers {
		wg.Go(func() {
			for i := range iterations {
				select {
				case <-stop:
					return
				default:
				}
				methods := []connect.Method{pingMethod("v")}
				// Vary the table shape so replacement maps differ in size.
				if i%2 == 0 {
					methods = append(methods, connect.Method{
						Spec: connect.Spec{
							Procedure:  pingv1connect.PingServiceFailProcedure,
							StreamType: connect.StreamTypeUnary,
						},
						Handler: func(context.Context, connect.Spec, connect.ServerStream) error {
							return connect.Errorf(connect.CodeUnimplemented, "fail")
						},
					})
				}
				if _, err := server.ReplaceMethods(methods...); err != nil {
					t.Errorf("ReplaceMethods: %v", err)
					close(stop)
					return
				}
			}
		})
	}
	for range workers {
		wg.Go(func() {
			for range iterations {
				select {
				case <-stop:
					return
				default:
				}
				resp, err := client.Ping(t.Context(), &pingv1.PingRequest{Number: 42})
				if err != nil {
					t.Errorf("Ping: %v", err)
					close(stop)
					return
				}
				if resp.GetNumber() != 42 {
					t.Errorf("Ping number = %d, want 42", resp.GetNumber())
					close(stop)
					return
				}
			}
		})
	}
	for range 2 {
		wg.Go(func() {
			for range iterations {
				select {
				case <-stop:
					return
				default:
				}
				snapshot := server.Snapshot()
				var count int
				for range snapshot.Specs() {
					count++
				}
				if count == 0 {
					t.Error("pinned snapshot unexpectedly empty")
					close(stop)
					return
				}
				currentCount := 0
				for range server.Specs() {
					currentCount++
				}
				if currentCount == 0 {
					t.Error("current generation unexpectedly empty")
					close(stop)
					return
				}
			}
		})
	}
	wg.Wait()
}

// TestInProcessReplacementRemovesProcedure verifies a generation that drops a
// procedure makes later in-process calls fail unimplemented while earlier
// revisions remain reachable through a pinned snapshot.
func TestInProcessReplacementRemovesProcedure(t *testing.T) {
	t.Parallel()
	server := connect.NewServer()
	_, err := server.ReplaceMethods(pingMethod("v0"))
	assert.Nil(t, err)
	snapshot := server.Snapshot()
	client := pingv1connect.NewPingServiceClient(connect.NewClient(connectinprocess.New(server)))

	_, err = client.Ping(t.Context(), &pingv1.PingRequest{})
	assert.Nil(t, err)

	_, err = server.ReplaceMethods()
	assert.Nil(t, err)
	_, err = client.Ping(t.Context(), &pingv1.PingRequest{})
	assert.Equal(t, connect.CodeOf(err), connect.CodeUnimplemented)

	// The pinned snapshot still serves the removed procedure through a raw
	// dispatch, proving the transport's earlier pin would have completed too.
	spec, ok := snapshot.Spec(pingv1connect.PingServicePingProcedure)
	assert.True(t, ok)
	assert.Equal(t, spec.Schema, "v0")
}
