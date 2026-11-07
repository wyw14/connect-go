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
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	"connectrpc.com/connect/v2/internal/assert"
	pingv1 "connectrpc.com/connect/v2/internal/gen/connect/ping/v1"
	"connectrpc.com/connect/v2/internal/memhttp"
	"connectrpc.com/connect/v2/internal/memhttp/memhttptest"
)

// countingMux implements [connecthttp.ServeMux] and records every registered
// pattern.
type countingMux struct {
	mu       sync.Mutex
	patterns []string
}

func (m *countingMux) Handle(pattern string, _ http.Handler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.patterns = append(m.patterns, pattern)
}

func unarySpec(procedure string, idempotency connect.IdempotencyLevel) connect.Spec {
	return connect.Spec{
		Procedure:        procedure,
		StreamType:       connect.StreamTypeUnary,
		IdempotencyLevel: idempotency,
	}
}

func echoHandler(marker string) connect.ServerFunc {
	return func(_ context.Context, _ connect.Spec, stream connect.ServerStream) error {
		var req pingv1.PingRequest
		if err := stream.Receive(&req); err != nil {
			return err
		}
		return stream.Send(&pingv1.PingResponse{Number: req.GetNumber(), Text: marker})
	}
}

func bidiEchoHandler(marker string) connect.ServerFunc {
	return func(_ context.Context, _ connect.Spec, stream connect.ServerStream) error {
		for {
			var req pingv1.PingRequest
			err := stream.Receive(&req)
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				return err
			}
			if err := stream.Send(&pingv1.PingResponse{Number: req.GetNumber(), Text: marker}); err != nil {
				return err
			}
		}
	}
}

func newDynamicServer(t *testing.T, server *connect.Server, options ...connecthttp.Option) (*memhttp.Server, *connect.Client) {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle("/", connecthttp.NewDynamicHandler(server, options...))
	httpServer := memhttptest.NewServer(t, mux)
	client := connect.NewClient(connecthttp.NewTransport(httpServer.Client(), httpServer.URL()))
	return httpServer, client
}

// TestDynamicHandlerRegistersSingleRoute verifies the dynamic handler needs
// exactly one mux registration no matter how many procedures are published.
func TestDynamicHandlerRegistersSingleRoute(t *testing.T) {
	t.Parallel()
	server := connect.NewServer()
	_, err := server.ReplaceMethods(
		connect.Method{Spec: unarySpec("/test.dynamic.Service/A", connect.IdempotencyUnknown), Handler: echoHandler("a")},
		connect.Method{Spec: unarySpec("/test.dynamic.Service/B", connect.IdempotencyUnknown), Handler: echoHandler("b")},
	)
	assert.Nil(t, err)

	mux := &countingMux{}
	mux.Handle("/", connecthttp.NewDynamicHandler(server))
	assert.Equal(t, len(mux.patterns), 1)
	assert.Equal(t, mux.patterns[0], "/")
}

// TestDynamicHandlerRoutesGenerations verifies procedures added, replaced,
// and removed at runtime are served without re-registering routes.
func TestDynamicHandlerRoutesGenerations(t *testing.T) {
	t.Parallel()
	const (
		procA = "/test.dynamic.Service/A"
		procB = "/test.dynamic.Service/B"
	)
	server := connect.NewServer()
	_, err := server.ReplaceMethods(connect.Method{
		Spec:    unarySpec(procA, connect.IdempotencyUnknown),
		Handler: echoHandler("a1"),
	})
	assert.Nil(t, err)
	httpServer, client := newDynamicServer(t, server)

	var res pingv1.PingResponse
	err = client.CallUnary(
		t.Context(),
		unarySpec(procA, connect.IdempotencyUnknown),
		&pingv1.PingRequest{Number: 1},
		&res,
	)
	assert.Nil(t, err)
	assert.Equal(t, res.GetText(), "a1")

	// B is an unknown method under a known service: the server answers
	// unimplemented rather than 404-ing the whole service.
	err = client.CallUnary(
		t.Context(),
		unarySpec(procB, connect.IdempotencyUnknown),
		&pingv1.PingRequest{},
		&pingv1.PingResponse{},
	)
	assert.Equal(t, connect.CodeOf(err), connect.CodeUnimplemented)

	// Publish a generation without A and with B.
	_, err = server.ReplaceMethods(connect.Method{
		Spec:    unarySpec(procB, connect.IdempotencyUnknown),
		Handler: echoHandler("b1"),
	})
	assert.Nil(t, err)

	err = client.CallUnary(
		t.Context(),
		unarySpec(procA, connect.IdempotencyUnknown),
		&pingv1.PingRequest{},
		&pingv1.PingResponse{},
	)
	assert.Equal(t, connect.CodeOf(err), connect.CodeUnimplemented)

	err = client.CallUnary(
		t.Context(),
		unarySpec(procB, connect.IdempotencyUnknown),
		&pingv1.PingRequest{Number: 2},
		&res,
	)
	assert.Nil(t, err)
	assert.Equal(t, res.GetText(), "b1")
	assert.Equal(t, res.GetNumber(), int64(2))

	// Sanity check: the mux behind the handler still answers non-RPC paths
	// with a plain 404.
	request, err := http.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		httpServer.URL()+"/not-an-rpc-path",
		strings.NewReader(""),
	)
	assert.Nil(t, err)
	request.Header.Set("Content-Type", "application/proto")
	response, err := httpServer.Client().Do(request)
	assert.Nil(t, err)
	defer response.Body.Close()
	assert.Equal(t, response.StatusCode, http.StatusNotFound)
}

// TestDynamicHandlerGetIdempotencyAcrossGenerations verifies Connect GET
// availability follows the pinned generation's idempotency level.
func TestDynamicHandlerGetIdempotencyAcrossGenerations(t *testing.T) {
	t.Parallel()
	const proc = "/test.get.Service/Echo"
	server := connect.NewServer()
	_, err := server.ReplaceMethods(connect.Method{
		Spec:    unarySpec(proc, connect.IdempotencyUnknown),
		Handler: echoHandler("v1"),
	})
	assert.Nil(t, err)
	httpServer, _ := newDynamicServer(t, server)

	doGet := func() int {
		request, err := http.NewRequestWithContext(
			t.Context(),
			http.MethodGet,
			httpServer.URL()+proc+`?encoding=json&message={}`,
			strings.NewReader(""),
		)
		assert.Nil(t, err)
		response, err := httpServer.Client().Do(request)
		assert.Nil(t, err)
		defer response.Body.Close()
		return response.StatusCode
	}

	// Non-idempotent unary: GET is not an allowed method.
	assert.Equal(t, doGet(), http.StatusMethodNotAllowed)

	// Same procedure, new generation, now side-effect free: GET works.
	_, err = server.ReplaceMethods(connect.Method{
		Spec:    unarySpec(proc, connect.IdempotencyNoSideEffects),
		Handler: echoHandler("v2"),
	})
	assert.Nil(t, err)
	assert.Equal(t, doGet(), http.StatusOK)

	// Same procedure becomes bidi: GET disappears again.
	_, err = server.ReplaceMethods(connect.Method{
		Spec: connect.Spec{
			Procedure:  proc,
			StreamType: connect.StreamTypeBidi,
		},
		Handler: bidiEchoHandler("v3"),
	})
	assert.Nil(t, err)
	assert.Equal(t, doGet(), http.StatusMethodNotAllowed)
}

// TestDynamicHandlerBidiRequiresHTTP2AcrossGenerations verifies the HTTP
// version gate follows the pinned generation's stream type.
func TestDynamicHandlerBidiRequiresHTTP2AcrossGenerations(t *testing.T) {
	t.Parallel()
	const proc = "/test.bidi.Service/Echo"
	server := connect.NewServer()
	_, err := server.ReplaceMethods(connect.Method{
		Spec:    unarySpec(proc, connect.IdempotencyUnknown),
		Handler: echoHandler("unary"),
	})
	assert.Nil(t, err)
	handler := connecthttp.NewDynamicHandler(server)

	postHTTP1 := func() *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, proc, strings.NewReader(""))
		request.Header.Set("Content-Type", "application/proto")
		handler.ServeHTTP(recorder, request)
		return recorder
	}

	// Unary over HTTP/1.1 is not rejected at the version gate.
	assert.NotEqual(t, postHTTP1().Code, http.StatusHTTPVersionNotSupported)

	_, err = server.ReplaceMethods(connect.Method{
		Spec: connect.Spec{
			Procedure:  proc,
			StreamType: connect.StreamTypeBidi,
		},
		Handler: bidiEchoHandler("bidi"),
	})
	assert.Nil(t, err)
	recorder := postHTTP1()
	assert.Equal(t, recorder.Code, http.StatusHTTPVersionNotSupported)
	assert.Equal(t, recorder.Header().Get("Connection"), "close")
}

// TestDynamicHandlerServicePolicy verifies the explicit service-prefix
// allowlist and the unknown-method versus unknown-service policies.
func TestDynamicHandlerServicePolicy(t *testing.T) {
	t.Parallel()
	const (
		allowedProc = "/test.allowed.Service/A"
		allowedSvc  = "/test.allowed.Service/"
	)
	server := connect.NewServer()
	_, err := server.ReplaceMethods(connect.Method{
		Spec:    unarySpec(allowedProc, connect.IdempotencyUnknown),
		Handler: echoHandler("ok"),
	})
	assert.Nil(t, err)
	// The prefix is accepted without a trailing slash and normalized.
	httpServer, client := newDynamicServer(
		t,
		server,
		connecthttp.WithDynamicServices("/test.allowed.Service"),
	)

	// Unknown service: plain 404 for an RPC-shaped request.
	request, err := http.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		httpServer.URL()+"/test.other.Service/X",
		strings.NewReader(""),
	)
	assert.Nil(t, err)
	request.Header.Set("Content-Type", "application/proto")
	response, err := httpServer.Client().Do(request)
	assert.Nil(t, err)
	defer response.Body.Close()
	assert.Equal(t, response.StatusCode, http.StatusNotFound)

	// Unknown method under the allowed service: RPC dispatch, unimplemented.
	err = client.CallUnary(
		t.Context(),
		unarySpec(allowedSvc+"Missing", connect.IdempotencyUnknown),
		&pingv1.PingRequest{},
		&pingv1.PingResponse{},
	)
	assert.Equal(t, connect.CodeOf(err), connect.CodeUnimplemented)

	// Unknown method GET: not dispatched, plain 404.
	request, err = http.NewRequestWithContext(
		t.Context(),
		http.MethodGet,
		httpServer.URL()+allowedSvc+"Missing?encoding=json&message={}",
		strings.NewReader(""),
	)
	assert.Nil(t, err)
	response, err = httpServer.Client().Do(request)
	assert.Nil(t, err)
	defer response.Body.Close()
	assert.Equal(t, response.StatusCode, http.StatusNotFound)

	// Non-RPC POST under an allowed service: plain 404.
	request, err = http.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		httpServer.URL()+allowedSvc+"Missing",
		strings.NewReader(""),
	)
	assert.Nil(t, err)
	request.Header.Set("Content-Type", "text/plain")
	response, err = httpServer.Client().Do(request)
	assert.Nil(t, err)
	defer response.Body.Close()
	assert.Equal(t, response.StatusCode, http.StatusNotFound)

	// The allowed procedure still serves normally.
	var res pingv1.PingResponse
	err = client.CallUnary(
		t.Context(),
		unarySpec(allowedProc, connect.IdempotencyUnknown),
		&pingv1.PingRequest{Number: 9},
		&res,
	)
	assert.Nil(t, err)
	assert.Equal(t, res.GetText(), "ok")
}

// TestDynamicHandlerPinsSnapshot verifies an HTTP request blocked in its
// handler finishes on the generation it entered even when that procedure is
// removed and re-added with a different handler mid-request.
func TestDynamicHandlerPinsSnapshot(t *testing.T) {
	t.Parallel()
	const proc = "/test.pinned.Service/Echo"
	entered := make(chan struct{})
	release := make(chan struct{})

	server := connect.NewServer()
	_, err := server.ReplaceMethods(connect.Method{
		Spec: unarySpec(proc, connect.IdempotencyUnknown),
		Handler: func(_ context.Context, _ connect.Spec, stream connect.ServerStream) error {
			var req pingv1.PingRequest
			if err := stream.Receive(&req); err != nil {
				return err
			}
			close(entered)
			<-release
			return stream.Send(&pingv1.PingResponse{Text: "old"})
		},
	})
	assert.Nil(t, err)
	_, client := newDynamicServer(t, server)

	type callResult struct {
		res *pingv1.PingResponse
		err error
	}
	resultCh := make(chan callResult, 1)
	go func() {
		res := &pingv1.PingResponse{}
		err := client.CallUnary(
			t.Context(),
			unarySpec(proc, connect.IdempotencyUnknown),
			&pingv1.PingRequest{},
			res,
		)
		resultCh <- callResult{res: res, err: err}
	}()
	<-entered

	// Remove and re-add the same procedure with a new handler while the
	// request is blocked inside the old one.
	_, err = server.ReplaceMethods()
	assert.Nil(t, err)
	_, err = server.ReplaceMethods(connect.Method{
		Spec:    unarySpec(proc, connect.IdempotencyUnknown),
		Handler: echoHandler("new"),
	})
	assert.Nil(t, err)

	close(release)
	result := <-resultCh
	assert.Nil(t, result.err)
	assert.Equal(t, result.res.GetText(), "old")

	// The next request observes the new generation.
	var res pingv1.PingResponse
	err = client.CallUnary(
		t.Context(),
		unarySpec(proc, connect.IdempotencyUnknown),
		&pingv1.PingRequest{},
		&res,
	)
	assert.Nil(t, err)
	assert.Equal(t, res.GetText(), "new")
}

// TestDynamicHandlerConcurrentGenerations exercises the per-revision handler
// cache under concurrent publications and requests, primarily for -race.
func TestDynamicHandlerConcurrentGenerations(t *testing.T) {
	t.Parallel()
	const proc = "/test.race.Service/Echo"
	server := connect.NewServer()
	_, err := server.ReplaceMethods(connect.Method{
		Spec:    unarySpec(proc, connect.IdempotencyUnknown),
		Handler: echoHandler("0"),
	})
	assert.Nil(t, err)
	_, client := newDynamicServer(t, server)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Go(func() {
		for range 100 {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := server.ReplaceMethods(connect.Method{
				Spec:    unarySpec(proc, connect.IdempotencyUnknown),
				Handler: echoHandler(""),
			}); err != nil {
				t.Errorf("ReplaceMethods: %v", err)
				close(stop)
				return
			}
		}
	})
	wg.Go(func() {
		for range 100 {
			select {
			case <-stop:
				return
			default:
			}
			var res pingv1.PingResponse
			if err := client.CallUnary(
				t.Context(),
				unarySpec(proc, connect.IdempotencyUnknown),
				&pingv1.PingRequest{},
				&res,
			); err != nil {
				t.Errorf("CallUnary: %v", err)
				close(stop)
				return
			}
		}
	})
	wg.Wait()
}
