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

package connecthttp

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"connectrpc.com/connect/v2"
)

// NewDynamicHandler returns an [http.Handler] that dispatches every request
// through server's current registry generation without installing a route per
// procedure. Register the returned handler once (for example at "/" on a
// [ServeMux]); [connect.Server.ReplaceMethods] publications then change the
// served procedures at runtime without re-registering anything on the mux.
//
// Each request pins one [*connect.RegistrySnapshot] before touching the wire.
// The snapshot's [connect.Spec] drives the HTTP version, Connect GET
// idempotency, and codec/protocol decisions, and the same snapshot's method
// serves the RPC. A request is therefore never parsed against one
// generation's spec and dispatched against another's, even when a new
// generation changes the stream type, idempotency level, or schema of a
// procedure while the request is in flight.
//
// Only the service prefixes allowed by [WithDynamicServices] are served.
// Without that option, every service in the current generation is allowed.
// Requests for unknown services get a plain 404. Requests for unknown methods
// under an allowed service follow the same policy as [Mount]: RPC-shaped POST
// requests reach the server's unknown handler or fail with
// [connect.CodeUnimplemented], while Connect GET and non-RPC requests get a
// plain 404.
func NewDynamicHandler(server *connect.Server, options ...Option) http.Handler {
	opts := defaultOptions()
	for _, opt := range options {
		opt.apply(&opts)
	}
	handler := &dynamicHandler{
		server: server,
		opts:   &opts,
	}
	if opts.dynamicServicesSet {
		handler.allowed = make(map[string]struct{}, len(opts.dynamicServices))
		for _, prefix := range opts.dynamicServices {
			if !strings.HasPrefix(prefix, "/") {
				panic(fmt.Sprintf("connecthttp: dynamic service prefix %q must start with '/'", prefix)) //nolint:forbidigo // construction-time misuse: panic surfaces the bug at startup
			}
			if !strings.HasSuffix(prefix, "/") {
				prefix += "/"
			}
			handler.allowed[prefix] = struct{}{}
		}
	}
	return handler
}

// dynamicHandler serves a whole [connect.Server] through one [http.Handler].
// It never mutates a registry generation: each request pins an immutable
// snapshot and the per-generation procedure handlers are cached and shared.
type dynamicHandler struct {
	server *connect.Server
	opts   *options

	// allowed is the explicit service-prefix allowlist from
	// WithDynamicServices. A nil map means auto mode: every service in the
	// pinned snapshot is allowed.
	allowed map[string]struct{}

	buildMu sync.Mutex
	built   atomic.Pointer[dynamicGeneration]
}

// dynamicGeneration is the cached set of procedure handlers built for one
// registry revision. The handlers capture the revision's snapshot, so the
// generation stays usable for in-flight requests after the cache moves on.
type dynamicGeneration struct {
	revision   connect.Revision
	procedures map[string]http.Handler
}

// ServeHTTP implements [http.Handler].
func (h *dynamicHandler) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	// Pin the registry first. Every decision below uses this snapshot.
	snapshot := h.server.Snapshot()
	spec, found := snapshot.Spec(request.URL.Path)
	if !found {
		if !h.serviceAllowed(snapshot, request.URL.Path) {
			// Unknown service: explicit policy, no RPC dispatch.
			http.NotFound(responseWriter, request)
			return
		}
		// Unknown method under an allowed service. The pinned snapshot
		// resolves the fallback, so this request never observes a generation
		// published after it arrived.
		newUnknownMethodHandler(snapshot.Call, h.opts).ServeHTTP(responseWriter, request)
		return
	}
	// Bidi requires HTTP/2. Check the pinned spec so a generation that changes
	// a procedure's stream type changes the answer on the next request.
	if spec.StreamType == connect.StreamTypeBidi && request.ProtoMajor < 2 {
		responseWriter.Header().Set("Connection", "close")
		responseWriter.WriteHeader(http.StatusHTTPVersionNotSupported)
		return
	}
	h.handlerFor(snapshot, spec).ServeHTTP(responseWriter, request)
}

// serviceAllowed reports whether path belongs to a service the dynamic
// handler is configured to serve.
func (h *dynamicHandler) serviceAllowed(snapshot *connect.RegistrySnapshot, path string) bool {
	idx := strings.LastIndexByte(path, '/')
	if idx <= 0 {
		return false
	}
	prefix := path[:idx+1]
	if h.allowed != nil {
		_, ok := h.allowed[prefix]
		return ok
	}
	// Auto mode: a service is known when any procedure in the pinned snapshot
	// shares its prefix.
	for spec := range snapshot.Specs() {
		if serviceIdx := strings.LastIndexByte(spec.Procedure, '/'); serviceIdx > 0 && spec.Procedure[:serviceIdx+1] == prefix {
			return true
		}
	}
	return false
}

// handlerFor returns the wire handler for spec, building and caching the
// handlers of the snapshot's revision on a cache miss.
func (h *dynamicHandler) handlerFor(snapshot *connect.RegistrySnapshot, spec connect.Spec) http.Handler {
	revision := snapshot.Revision()
	if built := h.built.Load(); built != nil && built.revision == revision {
		if handler, ok := built.procedures[spec.Procedure]; ok {
			return handler
		}
	}
	h.buildMu.Lock()
	defer h.buildMu.Unlock()
	if built := h.built.Load(); built != nil && built.revision == revision {
		return built.procedures[spec.Procedure]
	}
	procedures := make(map[string]http.Handler)
	for pinnedSpec := range snapshot.Specs() {
		// snapshot.Call binds the whole revision: parsing and dispatch use
		// the same generation for every request sharing this cache entry.
		procedures[pinnedSpec.Procedure] = newProcedureHandler(snapshot.Call, pinnedSpec, h.opts)
	}
	generation := &dynamicGeneration{revision: revision, procedures: procedures}
	// Never downgrade the cache to an older revision when publications race
	// with cache builds; the caller still gets its own revision's handler.
	if existing := h.built.Load(); existing == nil || revision > existing.revision {
		h.built.Store(generation)
	}
	return generation.procedures[spec.Procedure]
}
