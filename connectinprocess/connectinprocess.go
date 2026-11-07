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

// Package connectinprocess provides an in-process [connect.Transport]. The
// transport dispatches every RPC directly to a [connect.Server] inside the
// same process, completely bypassing the network. It is ideal for testing and
// specific production workloads.
//
// By default each direction is a rendezvous: Send blocks until the other
// side's Receive provides a destination message. With [WithBoundedBuffers],
// a transport instead keeps an explicitly bounded queue of independent
// message copies in each direction, so Send returns as soon as its copy is
// enqueued and never waits for the receiver to be ready.
package connectinprocess

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect/v2"
	"google.golang.org/protobuf/proto"
)

// CopyFunc copies a message from src into dst. The transport uses it to
// transfer each request and response between the client and server halves
// of an in-process RPC.
//
// The default is [ProtoCopy], a safe deep copy for Protobuf message. Override
// it with [WithCopyFunc].
//
// A CopyFunc must be safe to call from independent goroutines.
type CopyFunc func(dst, src any) error

// CloneFunc returns an independent copy of src. A buffered transport calls
// it while enqueuing each message, so the queue never retains a message the
// sender can subsequently mutate and the sender is free to reuse src once
// Send returns.
//
// The default is [ProtoClone], a safe deep clone for protobuf messages.
// Messages that are not protobuf messages require a custom CloneFunc. The
// transport never assumes [connect.Spec.Schema] is a protobuf descriptor.
//
// A CloneFunc must be safe to call from independent goroutines and must not
// retain any state shared with src.
type CloneFunc func(src any) (any, error)

// SizeFunc reports the size of msg in bytes. A buffered transport charges
// each enqueued copy's size against the direction's byte quota, so large
// messages cannot exhaust memory behind a generous message-count limit.
//
// The default is [ProtoSize], the exact serialized size of a protobuf
// message. Messages that are not protobuf messages require a custom
// SizeFunc.
//
// A SizeFunc must be safe to call from independent goroutines. Returning a
// negative size fails the Send without enqueuing anything.
type SizeFunc func(msg any) (int64, error)

// BufferConfig bounds the messages buffered in one direction of an RPC. The
// bounds cover every message that has been accepted by Send but not yet
// delivered to a Receive, counting both the number of messages and their
// total byte size as reported by the transport's SizeFunc. A Send that
// would exceed either limit blocks until quota is released; a single
// message larger than MaxBytes fails before it is enqueued.
type BufferConfig struct {
	// MaxMessages is the maximum number of queued message copies. It must
	// be positive.
	MaxMessages int
	// MaxBytes is the maximum total size, in bytes, of the queued message
	// copies. It must be positive and at least as large as any single
	// message sent in this direction.
	MaxBytes int64
}

func (c BufferConfig) validate(direction string) error {
	if c.MaxMessages <= 0 {
		return fmt.Errorf("connectinprocess: %s buffer MaxMessages must be positive, got %d", direction, c.MaxMessages)
	}
	if c.MaxBytes <= 0 {
		return fmt.Errorf("connectinprocess: %s buffer MaxBytes must be positive, got %d", direction, c.MaxBytes)
	}
	return nil
}

// Option configures a Transport built by [New].
type Option interface{ apply(*options) }

// New returns a [connect.Transport] that dispatches every RPC in-process
// to the given server. Pass the returned transport into
// [connect.NewClient] when constructing your generated service clients.
func New(server *connect.Server, opts ...Option) connect.Transport {
	resolved := options{
		copy:  ProtoCopy,
		clone: ProtoClone,
		size:  ProtoSize,
	}
	for _, opt := range opts {
		opt.apply(&resolved)
	}
	return &transport{
		server:         server,
		copy:           resolved.copy,
		clone:          resolved.clone,
		size:           resolved.size,
		bounded:        resolved.bounded,
		requestBuffer:  resolved.requestBuffer,
		responseBuffer: resolved.responseBuffer,
	}
}

// WithCopyFunc replaces the default [ProtoCopy] strategy. In buffered mode
// the CopyFunc transfers a queued copy into the receiver's destination
// message.
func WithCopyFunc(fn CopyFunc) Option {
	return optionFunc(func(o *options) { o.copy = fn })
}

// WithCloneFunc replaces the default [ProtoClone] strategy. It only takes
// effect when [WithBoundedBuffers] is enabled.
func WithCloneFunc(fn CloneFunc) Option {
	return optionFunc(func(o *options) { o.clone = fn })
}

// WithSizeFunc replaces the default [ProtoSize] strategy. It only takes
// effect when [WithBoundedBuffers] is enabled.
func WithSizeFunc(fn SizeFunc) Option {
	return optionFunc(func(o *options) { o.size = fn })
}

// WithBoundedBuffers enables explicit bounded buffering instead of the
// default rendezvous transfer. request bounds client-to-server messages and
// response bounds server-to-client messages; the two directions are limited
// independently.
//
// In this mode Send first asks the SizeFunc for the message's size, then
// blocks only while the queue is full or the pipe has ended, and finally
// forms an independent copy with the CloneFunc and enqueues it. Send
// returning nil means the copy is queued, not that the handler has
// processed it: the caller still owns the original message and may reuse or
// mutate it immediately. CloseSend stops new Sends but lets already
// accepted messages deliver in order before the receiver observes
// [io.EOF]. Context cancellation or [connect.ClientStream.Close] ends
// pending waits and releases the queues without draining them.
func WithBoundedBuffers(request, response BufferConfig) Option {
	return optionFunc(func(o *options) {
		o.bounded = true
		o.requestBuffer = request
		o.responseBuffer = response
	})
}

// ProtoCopy is the default [CopyFunc]. It uses [proto.Reset] followed by
// [proto.Merge] to perform a safe deep copy, so the client and server never
// share message state. It also bridges dynamic and generated messages of the
// same logical type.
func ProtoCopy(dst, src any) error {
	dstMsg, ok := dst.(proto.Message)
	if !ok {
		return fmt.Errorf("connectinprocess: ProtoCopy dst is %T, not a proto.Message", dst)
	}
	srcMsg, ok := src.(proto.Message)
	if !ok {
		return fmt.Errorf("connectinprocess: ProtoCopy src is %T, not a proto.Message", src)
	}
	proto.Reset(dstMsg)
	proto.Merge(dstMsg, srcMsg)
	return nil
}

// ProtoClone is the default [CloneFunc]. It returns [proto.Clone] of msg, an
// independent message that shares no state with the original. It works with
// both generated and dynamic protobuf messages and never inspects
// [connect.Spec.Schema].
func ProtoClone(msg any) (any, error) {
	src, ok := msg.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("connectinprocess: ProtoClone msg is %T, not a proto.Message", msg)
	}
	return proto.Clone(src), nil
}

// ProtoSize is the default [SizeFunc]. It returns [proto.Size] of msg, the
// exact number of bytes the message occupies when serialized.
func ProtoSize(msg any) (int64, error) {
	m, ok := msg.(proto.Message)
	if !ok {
		return 0, fmt.Errorf("connectinprocess: ProtoSize msg is %T, not a proto.Message", msg)
	}
	return int64(proto.Size(m)), nil
}

type transport struct {
	server *connect.Server
	copy   CopyFunc
	clone  CloneFunc
	size   SizeFunc

	bounded        bool
	requestBuffer  BufferConfig
	responseBuffer BufferConfig
}

func (t *transport) NewClientStream(ctx context.Context, spec connect.Spec) (connect.ClientStream, error) {
	if t.server == nil {
		return nil, errors.New("connectinprocess: nil server")
	}
	return t.newClientStream(ctx, spec)
}

type options struct {
	copy  CopyFunc
	clone CloneFunc
	size  SizeFunc

	bounded        bool
	requestBuffer  BufferConfig
	responseBuffer BufferConfig
}

type optionFunc func(*options)

func (f optionFunc) apply(o *options) { f(o) }
