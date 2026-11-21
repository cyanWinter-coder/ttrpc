/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package ttrpc

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/containerd/ttrpc/internal"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// startStatusTestServer boots a server on a fresh unix listener and returns a
// client connected to it together with both cleanup helpers.
func startStatusTestServer(t *testing.T, desc *ServiceDesc) (*Client, func()) {
	t.Helper()

	server := mustServer(t)(NewServer())
	addr, listener := newTestListener(t)
	if desc != nil {
		server.RegisterService(statusTestService, desc)
	}

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(context.Background(), listener)
	}()

	client, clientCleanup := newTestClient(t, addr)

	cleanup := func() {
		clientCleanup()
		_ = server.Close()
		_ = listener.Close()
		select {
		case <-serveErr:
		case <-time.After(2 * time.Second):
		}
	}
	return client, cleanup
}

const statusTestService = "statusTestService"

// TestUnaryStatusError verifies the full server-to-client error semantics for
// unary calls: stable code, message, structured details and the typed error
// recovery on the client.
func TestUnaryStatusError(t *testing.T) {
	detail := &errdetails.BadRequest{
		FieldViolations: []*errdetails.BadRequest_FieldViolation{
			{Field: "foo", Description: "must not be empty"},
		},
	}
	st, err := status.New(codes.InvalidArgument, "bad foo").WithDetails(detail)
	if err != nil {
		t.Fatal(err)
	}

	client, cleanup := startStatusTestServer(t, &ServiceDesc{
		Methods: map[string]Method{
			"Fail": func(_ context.Context, _ func(any) error) (any, error) {
				return nil, st.Err()
			},
			"FailTyped": func(_ context.Context, _ func(any) error) (any, error) {
				se := NewStatusErrorf(codes.PermissionDenied, "forbidden: %s", "nope")
				withDetails, err := se.WithDetails(&errdetails.ErrorInfo{
					Domain: "ttrpc.test",
					Reason: "DENIED",
				})
				if err != nil {
					return nil, err
				}
				return nil, withDetails
			},
		},
	})
	defer cleanup()

	t.Run("grpc status with details", func(t *testing.T) {
		err := client.Call(context.Background(), statusTestService, "Fail",
			&internal.TestPayload{}, &internal.TestPayload{})
		if err == nil {
			t.Fatal("expected an error")
		}

		// The same typed error is recovered on the client.
		var se *StatusError
		if !errors.As(err, &se) {
			t.Fatalf("expected *StatusError, got %T: %v", err, err)
		}
		if se.Code() != codes.InvalidArgument {
			t.Fatalf("code = %v, want %v", se.Code(), codes.InvalidArgument)
		}
		if se.Message() != "bad foo" {
			t.Fatalf("message = %q, want %q", se.Message(), "bad foo")
		}

		// grpc/status helpers must accept the recovered error unchanged.
		gs, ok := status.FromError(err)
		if !ok || gs.Code() != codes.InvalidArgument {
			t.Fatalf("status.FromError failed: ok=%v status=%v", ok, gs)
		}
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("status.Code = %v", status.Code(err))
		}

		// Structured details round-trip.
		details := se.Details()
		if len(details) != 1 {
			t.Fatalf("expected 1 detail, got %d", len(details))
		}
		br, ok := details[0].(*errdetails.BadRequest)
		if !ok {
			t.Fatalf("expected *errdetails.BadRequest detail, got %T", details[0])
		}
		if !proto.Equal(br, detail) {
			t.Fatalf("detail mismatch: got %v, want %v", br, detail)
		}

		// The client reconstruction matches the server's status error by code,
		// message and details.
		if !errors.Is(err, statusErrorFromProto(st.Proto())) {
			t.Fatal("errors.Is did not match the reconstructed server status")
		}
	})

	t.Run("ttrpc typed status with details", func(t *testing.T) {
		err := client.Call(context.Background(), statusTestService, "FailTyped",
			&internal.TestPayload{}, &internal.TestPayload{})
		if err == nil {
			t.Fatal("expected an error")
		}
		var se *StatusError
		if !errors.As(err, &se) {
			t.Fatalf("expected *StatusError, got %T: %v", err, err)
		}
		if se.Code() != codes.PermissionDenied {
			t.Fatalf("code = %v, want %v", se.Code(), codes.PermissionDenied)
		}
		details := se.Details()
		if len(details) != 1 {
			t.Fatalf("expected 1 detail, got %d", len(details))
		}
		ei, ok := details[0].(*errdetails.ErrorInfo)
		if !ok {
			t.Fatalf("expected *errdetails.ErrorInfo detail, got %T", details[0])
		}
		if ei.Reason != "DENIED" || ei.Domain != "ttrpc.test" {
			t.Fatalf("unexpected detail: %v", ei)
		}
	})
}

// TestPlainErrorKeepsCompatibility verifies that a handler returning an
// ordinary, non-status error still behaves exactly as before: a codes.Unknown
// status carrying the error text, without a details payload.
func TestPlainErrorKeepsCompatibility(t *testing.T) {
	client, cleanup := startStatusTestServer(t, &ServiceDesc{
		Methods: map[string]Method{
			"Plain": func(_ context.Context, _ func(any) error) (any, error) {
				return nil, errors.New("something went wrong")
			},
		},
	})
	defer cleanup()

	err := client.Call(context.Background(), statusTestService, "Plain",
		&internal.TestPayload{}, &internal.TestPayload{})
	if err == nil {
		t.Fatal("expected an error")
	}
	var se *StatusError
	if !errors.As(err, &se) {
		t.Fatalf("expected *StatusError, got %T: %v", err, err)
	}
	if se.Code() != codes.Unknown {
		t.Fatalf("code = %v, want %v", se.Code(), codes.Unknown)
	}
	if se.Message() != "something went wrong" {
		t.Fatalf("message = %q", se.Message())
	}
	if len(se.Details()) != 0 {
		t.Fatalf("expected no details, got %v", se.Details())
	}
	// The rendered error keeps the historical grpc/status format.
	if want := "rpc error: code = Unknown desc = something went wrong"; err.Error() != want {
		t.Fatalf("error text = %q, want %q", err.Error(), want)
	}
}

// TestClientCancellation maps a request cancelled by the caller to
// codes.Canceled while keeping the context cause reachable. It must never be
// confused with a deadline or a connection failure.
func TestClientCancellation(t *testing.T) {
	proceed := make(chan struct{})
	client, cleanup := startStatusTestServer(t, &ServiceDesc{
		Methods: map[string]Method{
			"Block": func(_ context.Context, _ func(any) error) (any, error) {
				<-proceed
				return &internal.TestPayload{}, nil
			},
		},
	})
	defer cleanup()
	defer close(proceed)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- client.Call(ctx, statusTestService, "Block",
			&internal.TestPayload{}, &internal.TestPayload{})
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected an error after cancellation")
		}
		if status.Code(err) != codes.Canceled {
			t.Fatalf("code = %v, want %v", status.Code(err), codes.Canceled)
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatal("errors.Is(err, context.Canceled) must hold")
		}
		var se *StatusError
		if !errors.As(err, &se) {
			t.Fatalf("expected *StatusError, got %T", err)
		}
		if errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("cancellation must not be reported as a deadline")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("call did not return after cancellation")
	}
}

// TestServerDeadlineExceeded verifies that a deadline sent by the client and
// reached on the server comes back as codes.DeadlineExceeded, distinct from
// cancellation and connection failures, including when the handler's error is
// wrapped.
func TestServerDeadlineExceeded(t *testing.T) {
	client, cleanup := startStatusTestServer(t, &ServiceDesc{
		Methods: map[string]Method{
			"Slow": func(ctx context.Context, _ func(any) error) (any, error) {
				select {
				case <-time.After(5 * time.Second):
					return &internal.TestPayload{}, nil
				case <-ctx.Done():
					// Wrap the context error to prove errors.Is-based mapping.
					return nil, errors.Join(errors.New("handler aborted"), ctx.Err())
				}
			},
		},
	})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := client.Call(ctx, statusTestService, "Slow",
		&internal.TestPayload{}, &internal.TestPayload{})
	if err == nil {
		t.Fatal("expected a deadline error")
	}
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("code = %v, want %v", status.Code(err), codes.DeadlineExceeded)
	}
	if errors.Is(err, context.Canceled) {
		t.Fatal("deadline exceeded must not be reported as cancellation")
	}
	var se *StatusError
	if !errors.As(err, &se) {
		t.Fatalf("expected *StatusError, got %T: %v", err, err)
	}
	if se.Message() == "" {
		t.Fatal("expected the status to carry a message")
	}
}

// TestConnectionInterrupted maps a broken underlying connection to
// codes.Unavailable, while the sentinel ErrClosed identity and the original
// transport error stay available for diagnostics.
func TestConnectionInterrupted(t *testing.T) {
	client, cleanup := startStatusTestServer(t, &ServiceDesc{
		Methods: map[string]Method{
			"Echo": func(_ context.Context, unmarshal func(any) error) (any, error) {
				var req internal.TestPayload
				if err := unmarshal(&req); err != nil {
					return nil, err
				}
				return &req, nil
			},
		},
	})
	defer cleanup()

	// Force the receive loop to exit by closing the underlying connection out
	// from under the client.
	client.conn.Close()
	client.UserOnCloseWait(context.Background())

	err := client.Call(context.Background(), statusTestService, "Echo",
		&internal.TestPayload{}, &internal.TestPayload{})
	if err == nil {
		t.Fatal("expected an error after connection interruption")
	}
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("code = %v, want %v", status.Code(err), codes.Unavailable)
	}
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed in the error chain, got %v", err)
	}

	// ErrClosed itself is a stable status-aware sentinel.
	if status.Code(ErrClosed) != codes.Unavailable {
		t.Fatalf("ErrClosed code = %v, want %v", status.Code(ErrClosed), codes.Unavailable)
	}
	if ErrClosed.Error() != "ttrpc: closed" {
		t.Fatalf("ErrClosed text changed: %q", ErrClosed.Error())
	}
}

// TestStreamTerminalStatus verifies that a server-side stream which has
// already delivered part of its data fails with a final error matching the
// server's terminal status.
func TestStreamTerminalStatus(t *testing.T) {
	detail := &errdetails.LocalizedMessage{Locale: "en", Message: "stream gave up"}
	st, err := status.New(codes.Aborted, "stream aborted").WithDetails(detail)
	if err != nil {
		t.Fatal(err)
	}

	client, cleanup := startStatusTestServer(t, &ServiceDesc{
		Methods: map[string]Method{
			"Echo": func(_ context.Context, unmarshal func(any) error) (any, error) {
				var req internal.TestPayload
				if err := unmarshal(&req); err != nil {
					return nil, err
				}
				return &req, nil
			},
		},
		Streams: map[string]Stream{
			"Partial": {
				Handler: func(_ context.Context, ss StreamServer) (any, error) {
					if err := ss.SendMsg(&internal.EchoPayload{Seq: 1, Msg: "first"}); err != nil {
						return nil, err
					}
					if err := ss.SendMsg(&internal.EchoPayload{Seq: 2, Msg: "second"}); err != nil {
						return nil, err
					}
					// Terminal error after partial data.
					return nil, st.Err()
				},
				StreamingServer: true,
			},
		},
	})
	defer cleanup()

	cs, err := client.NewStream(context.Background(),
		&StreamDesc{StreamingServer: true}, statusTestService, "Partial", nil)
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 2; i++ {
		var got internal.EchoPayload
		if err := cs.RecvMsg(&got); err != nil {
			t.Fatalf("expected data message %d, got %v", i, err)
		}
		if got.Seq != int64(i) {
			t.Fatalf("seq = %d, want %d", got.Seq, i)
		}
	}

	terminal := cs.RecvMsg(&internal.EchoPayload{})
	if terminal == nil {
		t.Fatal("expected the terminal status error")
	}
	if status.Code(terminal) != codes.Aborted {
		t.Fatalf("terminal code = %v, want %v", status.Code(terminal), codes.Aborted)
	}
	var se *StatusError
	if !errors.As(terminal, &se) {
		t.Fatalf("expected *StatusError terminal error, got %T", terminal)
	}
	if se.Message() != "stream aborted" {
		t.Fatalf("terminal message = %q", se.Message())
	}
	details := se.Details()
	if len(details) != 1 {
		t.Fatalf("expected 1 detail, got %d", len(details))
	}
	lm, ok := details[0].(*errdetails.LocalizedMessage)
	if !ok || !proto.Equal(lm, detail) {
		t.Fatalf("unexpected detail: %v", details[0])
	}
	if errors.Is(terminal, io.EOF) {
		t.Fatal("a terminal status error must not be confused with io.EOF")
	}

	// The connection was not poisoned by the stream's terminal status: a
	// unary call on the same connection still succeeds.
	var ping, pong internal.TestPayload
	ping.Foo = "still alive"
	if err := client.Call(context.Background(), statusTestService, "Echo", &ping, &pong); err != nil {
		t.Fatalf("connection not usable after stream failure: %v", err)
	}
	if pong.Foo != ping.Foo {
		t.Fatalf("unexpected reply %q", pong.Foo)
	}
}

// TestUnknownAndCorruptDetailsDegrade verifies that wire statuses carrying
// unknown detail types or corrupted detail bytes degrade to an ordinary
// status instead of breaking the connection, which keeps serving subsequent
// requests on the same connection.
func TestUnknownAndCorruptDetailsDegrade(t *testing.T) {
	// A detail whose type URL is not registered in this binary.
	unknownAny, err := anypb.New(&errdetails.ErrorInfo{Domain: "x", Reason: "y"})
	if err != nil {
		t.Fatal(err)
	}
	unknownAny.TypeUrl = "type.googleapis.com/ttrpc.test.DoesNotExist"

	corruptAny := &anypb.Any{
		TypeUrl: "type.googleapis.com/google.rpc.ErrorInfo",
		Value:   []byte("this is not a valid protobuf payload"),
	}

	client, cleanup := startStatusTestServer(t, &ServiceDesc{
		Methods: map[string]Method{
			"UnknownDetail": func(_ context.Context, _ func(any) error) (any, error) {
				return nil, statusErrorFromProto(&spb.Status{
					Code:    int32(codes.ResourceExhausted),
					Message: "unknown detail attached",
					Details: []*anypb.Any{unknownAny},
				})
			},
			"CorruptDetail": func(_ context.Context, _ func(any) error) (any, error) {
				return nil, statusErrorFromProto(&spb.Status{
					Code:    int32(codes.Internal),
					Message: "corrupt detail attached",
					Details: []*anypb.Any{corruptAny},
				})
			},
			"Healthy": func(_ context.Context, unmarshal func(any) error) (any, error) {
				var req internal.TestPayload
				if err := unmarshal(&req); err != nil {
					return nil, err
				}
				req.Foo = "ok"
				return &req, nil
			},
		},
	})
	defer cleanup()

	for _, tc := range []struct {
		method string
		code   codes.Code
		msg    string
	}{
		{"UnknownDetail", codes.ResourceExhausted, "unknown detail attached"},
		{"CorruptDetail", codes.Internal, "corrupt detail attached"},
	} {
		t.Run(tc.method, func(t *testing.T) {
			err := client.Call(context.Background(), statusTestService, tc.method,
				&internal.TestPayload{}, &internal.TestPayload{})
			if err == nil {
				t.Fatal("expected an error")
			}
			if status.Code(err) != tc.code {
				t.Fatalf("code = %v, want %v", status.Code(err), tc.code)
			}
			var se *StatusError
			if !errors.As(err, &se) {
				t.Fatalf("expected *StatusError, got %T: %v", err, err)
			}
			if se.Message() != tc.msg {
				t.Fatalf("message = %q, want %q", se.Message(), tc.msg)
			}
			// The bad detail is surfaced as a decode error in place, it does
			// not invalidate the status or the connection.
			details := se.Details()
			if len(details) != 1 {
				t.Fatalf("expected 1 detail slot, got %d", len(details))
			}
			if dErr, ok := details[0].(error); !ok {
				t.Fatalf("expected decode error in detail slot, got %T", details[0])
			} else if dErr == nil {
				t.Fatal("expected a non-nil decode error")
			}
		})
	}

	// A subsequent call on the same connection still works, proving the
	// malformed details did not poison the transport.
	var reply internal.TestPayload
	if err := client.Call(context.Background(), statusTestService, "Healthy",
		&internal.TestPayload{Foo: "ping"}, &reply); err != nil {
		t.Fatalf("connection poisoned by bad details: %v", err)
	}
	if reply.Foo != "ok" {
		t.Fatalf("unexpected reply %q", reply.Foo)
	}
}

// TestSentinelStatusMapping locks in the distinct statuses of local
// conditions so they can never be mixed up.
func TestSentinelStatusMapping(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want codes.Code
	}{
		{"closed connection", ErrClosed, codes.Unavailable},
		{"stream buffer full", ErrStreamFull, codes.ResourceExhausted},
		{"context canceled", mapStatusError(context.Canceled), codes.Canceled},
		{"context deadline", mapStatusError(context.DeadlineExceeded), codes.DeadlineExceeded},
		// Unrecognized plain errors are passed through unchanged and therefore
		// classify as codes.Unknown, exactly like the historical behavior.
		{"unmapped plain error", mapStatusError(net.ErrClosed), codes.Unknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := status.Code(tc.err); got != tc.want {
				t.Fatalf("status.Code(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}

	// Sentinels keep their historical identity and error text.
	if ErrClosed.Error() != "ttrpc: closed" {
		t.Fatalf("ErrClosed text = %q", ErrClosed.Error())
	}
	if ErrStreamFull.Error() != "ttrpc: stream buffer full" {
		t.Fatalf("ErrStreamFull text = %q", ErrStreamFull.Error())
	}
	var se *StatusError
	if errors.As(mapStatusError(context.Canceled), &se) {
		if !errors.Is(se, context.Canceled) {
			t.Fatal("mapped cancellation must unwrap to context.Canceled")
		}
	} else {
		t.Fatal("mapped cancellation must be a *StatusError")
	}
}
