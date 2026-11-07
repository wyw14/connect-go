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

package connectinprocess

import (
	"context"
	"errors"
	"io"
	"slices"
	"sync"

	"connectrpc.com/connect/v2"
)

// messagePipe hands messages in one direction of a streamPair. Every
// implementation honors the single-sender, single-receiver contract and
// delivers messages in send order.
type messagePipe interface {
	// send transfers msg to the receiver. In buffered mode it returns once
	// an independent copy of msg is accepted, not once the receiver has
	// processed it.
	send(ctx context.Context, msg any) error
	// receive waits for the next message and transfers it into dst. It
	// reports filled=true when dst was filled, even if the transfer itself
	// returned an error. Otherwise err is the context error or io.EOF once
	// the pipe has ended.
	receive(ctx context.Context, dst any) (filled bool, err error)
	// closeSend half-closes the sender side: already accepted messages stay
	// available and are delivered in order before io.EOF.
	closeSend()
	// abort tears the pipe down immediately, discarding queued messages and
	// ending every wait. It is idempotent.
	abort()
	// wake unblocks waits so they can re-check the context.
	wake()
}

// streamPair wires the client and server halves of an in-process RPC
// together. The server runs on its own goroutine, started by the client's
// first SendHeaders, Send, or Receive.
//
// Each direction is a messagePipe guarded independently of the pair's own
// mutex. The pair mutex only protects dispatch and response metadata, so a
// user Clone or Copy callback running on one direction never blocks the
// other direction.
type streamPair struct {
	t          *transport
	spec       connect.Spec
	clientInfo *connect.CallInfo
	serverInfo connect.CallInfo
	server     serverStream

	// ctx is the stream context, derived from the call context. The server
	// runs on it. Close cancels it via cancel.
	ctx    context.Context
	cancel context.CancelFunc

	mu         sync.Mutex
	request    messagePipe
	response   messagePipe
	started    bool
	stopWake   func() bool // releases the context.AfterFunc that wakes waiters
	headerSent bool
	serverErr  error
}

// dispatch starts the server goroutine once.
func (p *streamPair) dispatch() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started {
		return
	}
	p.started = true
	if p.clientInfo != nil {
		syncHeader(p.serverInfo.RequestHeader(), p.clientInfo.RequestHeader())
	}
	p.stopWake = context.AfterFunc(p.ctx, func() {
		p.request.wake()
		p.response.wake()
	})
	go p.run()
}

// run executes the server and then ends both pipes.
func (p *streamPair) run() {
	defer func() {
		p.mu.Lock()
		if r := recover(); r != nil {
			p.serverErr = connect.Errorf(connect.CodeInternal, "panic in server: %v", r)
		}
		p.flushHeaders()
		if p.clientInfo != nil {
			syncHeader(p.clientInfo.ResponseTrailer(), p.serverInfo.ResponseTrailer())
		}
		p.mu.Unlock()
		// The handler will never consume another request, so abandon
		// anything still queued client-to-server. Accepted responses stay
		// queued: the client drains them, in order, before observing the
		// server's final state and trailers.
		p.request.abort()
		p.response.closeSend()
	}()
	p.serverErr = p.t.server.Call(p.ctx, p.spec.Procedure, &p.serverInfo, &p.server)
}

// flushHeaders copies the response headers to the client once, as the
// server's first Send or SendHeaders puts them on the wire. Later changes to
// the server's headers are not sent. The caller must hold mu.
func (p *streamPair) flushHeaders() {
	if p.headerSent {
		return
	}
	p.headerSent = true
	if p.clientInfo != nil {
		syncHeader(p.clientInfo.ResponseHeader(), p.serverInfo.ResponseHeader())
	}
}

// clientStream is the client-side half of a streamPair.
type clientStream struct {
	streamPair

	// end is the result of the Receive that ended the stream. Receive
	// keeps returning it.
	end error
}

func (t *transport) newClientStream(ctx context.Context, spec connect.Spec) (*clientStream, error) {
	if t.bounded {
		if err := t.requestBuffer.validate("request"); err != nil {
			return nil, err
		}
		if err := t.responseBuffer.validate("response"); err != nil {
			return nil, err
		}
	}
	stream := &clientStream{}
	stream.ctx, stream.cancel = context.WithCancel(ctx)
	stream.t = t
	stream.spec = spec
	stream.clientInfo, _ = connect.CallInfoForClientContext(ctx)
	stream.serverInfo.Spec = spec
	stream.server.p = &stream.streamPair
	if t.bounded {
		stream.request = newBoundedPipe(t.requestBuffer, t.clone, t.size, t.copy)
		stream.response = newBoundedPipe(t.responseBuffer, t.clone, t.size, t.copy)
	} else {
		stream.request = newRendezvousPipe(&stream.mu, t.copy)
		stream.response = newRendezvousPipe(&stream.mu, t.copy)
	}
	if stream.clientInfo != nil {
		stream.clientInfo.Spec = spec
	}
	return stream, nil
}

func (s *clientStream) SendHeaders() error {
	s.dispatch()
	return nil
}

func (s *clientStream) Send(msg any) error {
	s.dispatch()
	return s.request.send(s.ctx, msg)
}

// CloseSend half-closes the request pipe: new Sends fail with io.EOF while
// already accepted requests are delivered to the handler in order first.
func (s *clientStream) CloseSend() error {
	s.request.closeSend()
	return nil
}

// Receive reads the single response of a unary or client-streaming RPC and
// then the end of the stream, as connecthttp does.
func (s *clientStream) Receive(dst any) error {
	if s.spec.StreamType != connect.StreamTypeUnary && s.spec.StreamType != connect.StreamTypeClient {
		return s.next(dst)
	}
	if s.end != nil {
		return io.EOF
	}
	err := s.next(dst)
	switch {
	case err == nil:
		if drainErr := s.next(dst); drainErr == nil {
			err = connect.Errorf(connect.CodeUnimplemented, "unary stream has multiple messages")
			s.finish(io.EOF)
		} else if !errors.Is(drainErr, io.EOF) {
			err = drainErr
		}
	case errors.Is(err, io.EOF):
		err = connect.Errorf(connect.CodeUnimplemented, "unary stream has zero messages")
	}
	return err
}

// Close cancels before discarding both pipes. Waiters woken by the teardown
// then report the cancellation instead of a clean end, and queued messages
// are released without being consumed.
func (s *clientStream) Close() error {
	s.cancel()
	s.request.abort()
	s.response.abort()
	return nil
}

// next reads the next response. The error that ends the stream is sticky.
func (s *clientStream) next(dst any) error {
	if s.end != nil {
		return s.end
	}
	s.dispatch()
	filled, err := s.response.receive(s.ctx, dst)
	if filled {
		return err
	}
	if errors.Is(err, io.EOF) && s.serverErr != nil {
		err = asClientErr(s.serverErr)
	}
	s.finish(err)
	return err
}

// finish ends the stream with err and releases the stream context.
func (s *clientStream) finish(err error) {
	s.end = err
	s.stopWake()
	s.cancel()
}

// serverStream is the server-side half of a streamPair.
type serverStream struct {
	p *streamPair
}

func (s *serverStream) SendHeaders() error {
	s.p.mu.Lock()
	defer s.p.mu.Unlock()
	s.p.flushHeaders()
	return nil
}

func (s *serverStream) Receive(dst any) error {
	_, err := s.p.request.receive(s.p.ctx, dst)
	return err
}

func (s *serverStream) Send(msg any) error {
	s.p.mu.Lock()
	s.p.flushHeaders()
	s.p.mu.Unlock()
	return s.p.response.send(s.p.ctx, msg)
}

// rendezvousPipe is the default, unbuffered pipe. Receive posts its dst and
// waits; Send copies its message straight into the posted dst while holding
// the streamPair mutex. The sender may reuse its message once Send returns.
type rendezvousPipe struct {
	mu     *sync.Mutex
	cond   sync.Cond
	copy   CopyFunc
	dst    any   // posted by a waiting Receive
	filled bool  // set once Send has copied into dst
	err    error // the copy result
	closed bool  // no more messages
}

func newRendezvousPipe(mu *sync.Mutex, copyFn CopyFunc) *rendezvousPipe {
	pipe := &rendezvousPipe{mu: mu, copy: copyFn}
	pipe.cond.L = mu
	return pipe
}

func (pp *rendezvousPipe) wake() {
	pp.cond.Broadcast()
}

// A rendezvous pipe holds no messages, so half-close and teardown are the
// same transition.
func (pp *rendezvousPipe) closeSend() { pp.close() }

func (pp *rendezvousPipe) abort() { pp.close() }

// close ends the pipe and wakes both sides.
func (pp *rendezvousPipe) close() {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	pp.closed = true
	pp.cond.Broadcast()
}

func (pp *rendezvousPipe) send(ctx context.Context, msg any) error {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if pp.closed {
			return io.EOF
		}
		if pp.dst != nil {
			break
		}
		pp.cond.Wait()
	}
	dst := pp.dst
	pp.dst = nil
	// Wake the receiver even if the copy panics. The receiver then fails
	// instead of using a partial copy. The panic itself propagates to the
	// sender, as in the default rendezvous transport.
	copied := false
	defer func() {
		if !copied {
			pp.err = connect.Errorf(connect.CodeInternal, "connectinprocess: copy panicked")
		}
		pp.filled = true
		pp.cond.Broadcast()
	}()
	pp.err = pp.copy(dst, msg)
	copied = true
	return nil
}

func (pp *rendezvousPipe) receive(ctx context.Context, dst any) (bool, error) {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	pp.dst = dst
	pp.cond.Broadcast()
	for {
		if pp.filled {
			pp.filled = false
			return true, pp.err
		}
		if err := ctx.Err(); err != nil {
			pp.dst = nil
			return false, err
		}
		if pp.closed {
			pp.dst = nil
			return false, io.EOF
		}
		pp.cond.Wait()
	}
}

// queuedMessage is one independent message copy held by a boundedPipe, with
// the byte quota it consumes.
type queuedMessage struct {
	msg  any
	size int64
}

// boundedPipe decouples sender and receiver with an explicitly bounded FIFO
// queue. Send forms an independent copy before enqueuing and never retains
// the sender's original message. Both the message count and the total byte
// size of every enqueued, not-yet-received copy count against the limits.
type boundedPipe struct {
	mu       sync.Mutex
	cond     sync.Cond
	maxMsgs  int
	maxBytes int64

	clone CloneFunc
	size  SizeFunc
	copy  CopyFunc

	queue []queuedMessage
	head  int

	// usedMsgs and usedBytes include copies reserved by a Send whose Clone
	// is still running, so quota can never be overcommitted.
	usedMsgs  int
	usedBytes int64

	sendClosed bool
	aborted    bool
}

func newBoundedPipe(cfg BufferConfig, clone CloneFunc, size SizeFunc, copyFn CopyFunc) *boundedPipe {
	pipe := &boundedPipe{
		maxMsgs:  cfg.MaxMessages,
		maxBytes: cfg.MaxBytes,
		clone:    clone,
		size:     size,
		copy:     copyFn,
	}
	pipe.cond.L = &pipe.mu
	return pipe
}

func (pp *boundedPipe) wake() {
	pp.cond.Broadcast()
}

func (pp *boundedPipe) closeSend() {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	if pp.aborted {
		return
	}
	pp.sendClosed = true
	pp.cond.Broadcast()
}

func (pp *boundedPipe) abort() {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	if pp.aborted {
		return
	}
	pp.aborted = true
	pp.sendClosed = true
	pp.queue = nil
	pp.head = 0
	pp.usedMsgs = 0
	pp.usedBytes = 0
	pp.cond.Broadcast()
}

// full reports whether a message of size would exceed either quota.
func (pp *boundedPipe) full(size int64) bool {
	return pp.usedMsgs >= pp.maxMsgs || pp.usedBytes+size > pp.maxBytes
}

func (pp *boundedPipe) send(ctx context.Context, msg any) error {
	size, err := pp.size(msg)
	if err != nil {
		return err
	}
	if size < 0 {
		return connect.Errorf(connect.CodeInternal, "connectinprocess: SizeFunc returned negative size %d", size)
	}
	// An oversize message can never fit, so fail before reserving quota or
	// forming a copy.
	if size > pp.maxBytes {
		return connect.Errorf(
			connect.CodeResourceExhausted,
			"connectinprocess: message of %d bytes exceeds the %d-byte buffer limit",
			size, pp.maxBytes,
		)
	}

	pp.mu.Lock()
	stopWake := context.AfterFunc(ctx, pp.cond.Broadcast)
	defer stopWake()
	for {
		if err := ctx.Err(); err != nil {
			pp.mu.Unlock()
			return err
		}
		if pp.sendClosed || pp.aborted {
			pp.mu.Unlock()
			return io.EOF
		}
		if !pp.full(size) {
			break
		}
		pp.cond.Wait()
	}
	pp.usedMsgs++
	pp.usedBytes += size
	pp.mu.Unlock()

	// The reservation is returned on every path that does not enqueue:
	// cancellation, close, Clone error, or a panicking Clone. Once aborted,
	// the counters are owned by abort and must not be touched.
	committed := false
	defer func() {
		if committed {
			return
		}
		pp.mu.Lock()
		if !pp.aborted {
			pp.usedMsgs--
			pp.usedBytes -= size
		}
		pp.cond.Broadcast()
		pp.mu.Unlock()
	}()

	// Clone runs without holding any pipe lock, so a slow copy cannot block
	// the other direction or the receiver.
	clone, err := pp.clone(msg)
	if err != nil {
		return err
	}

	pp.mu.Lock()
	defer pp.mu.Unlock()
	if pp.aborted || pp.sendClosed {
		return io.EOF
	}
	pp.queue = append(pp.queue, queuedMessage{msg: clone, size: size})
	committed = true
	pp.cond.Broadcast()
	return nil
}

func (pp *boundedPipe) receive(ctx context.Context, dst any) (bool, error) {
	pp.mu.Lock()
	stopWake := context.AfterFunc(ctx, pp.cond.Broadcast)
	defer stopWake()
	for {
		if pp.head < len(pp.queue) {
			item := pp.queue[pp.head]
			pp.queue[pp.head] = queuedMessage{}
			pp.head++
			if pp.head == len(pp.queue) {
				pp.queue = pp.queue[:0]
				pp.head = 0
			}
			if !pp.aborted {
				pp.usedMsgs--
				pp.usedBytes -= item.size
			}
			pp.cond.Broadcast()
			pp.mu.Unlock()
			// The user Copy callback runs without the pipe lock.
			return true, pp.copyInto(dst, item.msg)
		}
		if pp.aborted {
			pp.mu.Unlock()
			if err := ctx.Err(); err != nil {
				return false, err
			}
			return false, io.EOF
		}
		if err := ctx.Err(); err != nil {
			pp.mu.Unlock()
			return false, err
		}
		if pp.sendClosed {
			pp.mu.Unlock()
			return false, io.EOF
		}
		pp.cond.Wait()
	}
}

// copyInto transfers a queued copy into the receiver's dst. A panicking
// CopyFunc fails the Receive rather than crashing the receiver goroutine.
func (pp *boundedPipe) copyInto(dst, src any) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = connect.Errorf(connect.CodeInternal, "connectinprocess: copy panicked: %v", r)
		}
	}()
	return pp.copy(dst, src)
}

// asClientErr converts a server's returned error into the verdict the
// client observes, mirroring the serialization and deserialization
// of a wire transport.
//
// To accurately reflect the client-side state, it applies the following rules:
//   - Remote errors forwarded from an upstream call are scrubbed to
//     [CodeInternal] to prevent leaking their original code, message,
//     and details as the server's own verdict (see [Error.IsRemote]).
//   - Standard errors (non-[*Error]) are converted to [CodeUnknown],
//     except for context cancellation and deadline expiry, which retain
//     their respective codes.
//   - Causes are dropped, as a wire transport never serializes them. This
//     also keeps a cause matching io.EOF from reading as the clean end of
//     the stream.
//   - The resulting error is marked as remote, mimicking a client-side
//     decoded error.
//
// The server's original [*Error] is left untouched so it can be safely
// used for server-side logging.
func asClientErr(err error) error {
	if err == nil {
		return nil
	}
	if cerr, ok := errors.AsType[*connect.Error](err); ok && cerr != nil {
		if cerr.IsRemote() {
			return connect.NewError(connect.CodeInternal, "").WithRemote()
		}
		return withoutCause(cerr).WithRemote()
	}
	code := connect.CodeUnknown
	switch {
	case errors.Is(err, context.Canceled):
		code = connect.CodeCanceled
	case errors.Is(err, context.DeadlineExceeded):
		code = connect.CodeDeadlineExceeded
	}
	return connect.NewError(code, "").WithRemote()
}

// withoutCause returns a copy of err with its code, message, and details, but
// not its cause.
func withoutCause(err *connect.Error) *connect.Error {
	clone := connect.NewError(err.Code(), err.Message())
	for _, detail := range err.Details() {
		clone = clone.WithDetail(detail)
	}
	return clone
}

// syncHeader copies src into dst. The values are cloned because the server
// may keep changing src after the copy.
func syncHeader(dst, src *connect.Header) {
	for key, vals := range src.All() {
		dst.SetValues(key, slices.Clone(vals))
	}
}
