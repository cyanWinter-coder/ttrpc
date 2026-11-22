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
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/containerd/ttrpc/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestCallMetadataSnapshotFrozenBeforeInterceptors verifies that the metadata
// snapshot is frozen before the interceptor chain runs: interceptor mutations
// are visible within the call (repeated reads are consistent) but never write
// back to the caller's MD, and the wire request carries the frozen values.
func TestCallMetadataSnapshotFrozenBeforeInterceptors(t *testing.T) {
	var (
		ctx             = context.Background()
		server          = mustServer(t)(NewServer())
		testImpl        = &testingServer{}
		addr, listener  = newTestListener(t)
		message         = strings.Repeat("a", 16)
		interceptedVals = make(chan string, 2)
	)

	defer listener.Close()

	interceptor := func(ctx context.Context, req *Request, resp *Response, _ *UnaryClientInfo, invoker Invoker) error {
		before, ok := GetMetadataValue(ctx, "foo")
		if !ok {
			t.Errorf("metadata not visible inside interceptor")
		}
		interceptedVals <- before

		md, ok := GetMetadata(ctx)
		if !ok {
			t.Errorf("metadata map not attached to call context")
		} else {
			md.Set("foo", "mutated-by-interceptor")
			md.Set("interceptor-only", "yes")
		}

		// Repeated reads within the same call observe the same,
		// call-local values.
		after, _ := GetMetadataValue(ctx, "foo")
		interceptedVals <- after

		return invoker(ctx, req, resp)
	}

	client, cleanup := newTestClient(t, addr, WithUnaryClientInterceptor(interceptor))
	defer cleanup()

	registerTestingService(server, testImpl)

	go server.Serve(ctx, listener)
	defer server.Shutdown(ctx)

	parentMD := MD{}
	parentMD.Set("foo", "parent")
	callCtx := WithMetadata(ctx, parentMD)

	response := &internal.TestPayload{}
	if err := client.Call(callCtx, serviceName, "Test", &internal.TestPayload{Foo: message}, response); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if v := <-interceptedVals; v != "parent" {
		t.Errorf("interceptor saw %q, expected frozen %q", v, "parent")
	}
	if v := <-interceptedVals; v != "mutated-by-interceptor" {
		t.Errorf("repeated read inside call saw %q, expected call-local mutation", v)
	}

	// The wire request was built from the frozen snapshot.
	if response.Metadata != "parent" {
		t.Errorf("server received metadata %q, expected frozen %q", response.Metadata, "parent")
	}

	// Interceptor mutations must not write back to the caller's MD.
	if v, ok := parentMD.Get("foo"); !ok || v[0] != "parent" {
		t.Errorf("parent metadata mutated by call: %v", parentMD)
	}
	if _, ok := parentMD.Get("interceptor-only"); ok {
		t.Errorf("interceptor-added metadata leaked into parent MD: %v", parentMD)
	}
}

// TestCallMetadataSnapshotImmutableInFlight verifies that mutating the
// caller's MD while a call is in flight does not change what is sent:
// the snapshot is bound to the call at chain entry.
func TestCallMetadataSnapshotImmutableInFlight(t *testing.T) {
	var (
		ctx            = context.Background()
		server         = mustServer(t)(NewServer())
		testImpl       = &testingServer{}
		addr, listener = newTestListener(t)
		entered        = make(chan struct{})
		release        = make(chan struct{})
	)

	defer listener.Close()

	interceptor := func(ctx context.Context, req *Request, resp *Response, _ *UnaryClientInfo, invoker Invoker) error {
		close(entered)
		<-release // wait for the parent MD to be mutated behind our back
		return invoker(ctx, req, resp)
	}

	client, cleanup := newTestClient(t, addr, WithUnaryClientInterceptor(interceptor))
	defer cleanup()

	registerTestingService(server, testImpl)

	go server.Serve(ctx, listener)
	defer server.Shutdown(ctx)

	parentMD := MD{}
	parentMD.Set("foo", "early")
	callCtx := WithMetadata(ctx, parentMD)

	type result struct {
		err      error
		metadata string
	}
	results := make(chan result, 1)
	go func() {
		response := &internal.TestPayload{}
		err := client.Call(callCtx, serviceName, "Test", &internal.TestPayload{}, response)
		results <- result{err: err, metadata: response.Metadata}
	}()

	<-entered
	// Business code mutates the parent MD while the call is in flight.
	parentMD.Set("foo", "late")
	close(release)

	r := <-results
	if r.err != nil {
		t.Fatalf("unexpected error: %v", r.err)
	}
	if r.metadata != "early" {
		t.Errorf("server received metadata %q, expected in-flight snapshot %q", r.metadata, "early")
	}
}

// TestConcurrentCallsMetadataIsolation verifies that concurrent calls sharing
// one connection do not overwrite each other's headers, and that error
// responses are delivered to the call that caused them.
func TestConcurrentCallsMetadataIsolation(t *testing.T) {
	var (
		ctx             = context.Background()
		server          = mustServer(t)(NewServer())
		addr, listener  = newTestListener(t)
		client, cleanup = newTestClient(t, addr)
	)

	defer listener.Close()
	defer cleanup()

	echo := func(ctx context.Context, unmarshal func(any) error) (any, error) {
		var req internal.TestPayload
		if err := unmarshal(&req); err != nil {
			return nil, err
		}
		v, _ := GetMetadataValue(ctx, "foo")
		return &internal.TestPayload{Metadata: v}, nil
	}
	fail := func(ctx context.Context, unmarshal func(any) error) (any, error) {
		var req internal.TestPayload
		if err := unmarshal(&req); err != nil {
			return nil, err
		}
		v, _ := GetMetadataValue(ctx, "foo")
		return nil, status.Errorf(codes.NotFound, "call-%s", v)
	}
	server.RegisterService(serviceName, &ServiceDesc{
		Methods: map[string]Method{"Echo": echo, "Fail": fail},
	})

	go server.Serve(ctx, listener)
	defer server.Shutdown(ctx)

	const callers = 32
	var wg sync.WaitGroup
	errs := make(chan error, callers*2)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tag := fmt.Sprintf("caller-%d", i)
			md := MD{}
			md.Set("foo", tag)
			callCtx := WithMetadata(ctx, md)

			resp := &internal.TestPayload{}
			if err := client.Call(callCtx, serviceName, "Echo", &internal.TestPayload{}, resp); err != nil {
				errs <- fmt.Errorf("echo %s: %w", tag, err)
				return
			}
			if resp.Metadata != tag {
				errs <- fmt.Errorf("echo %s: server saw %q", tag, resp.Metadata)
				return
			}

			err := client.Call(callCtx, serviceName, "Fail", &internal.TestPayload{}, &internal.TestPayload{})
			if err == nil {
				errs <- fmt.Errorf("fail %s: expected error, got nil", tag)
				return
			}
			// The error response must carry this call's identity.
			if status.Code(err) != codes.NotFound || !strings.HasSuffix(err.Error(), "call-"+tag) {
				errs <- fmt.Errorf("fail %s: mismatched error: %v", tag, err)
				return
			}
			// The caller's MD must be untouched by both calls.
			if v, _ := md.Get("foo"); v[0] != tag || len(md) != 1 {
				errs <- fmt.Errorf("fail %s: parent MD mutated: %v", tag, md)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestStreamMetadataAndDeadlineFromContext verifies that NewStream binds a
// metadata snapshot and the context deadline to the stream request, so the
// server handler observes them through its own context.
func TestStreamMetadataAndDeadlineFromContext(t *testing.T) {
	var (
		ctx             = context.Background()
		server          = mustServer(t)(NewServer())
		addr, listener  = newTestListener(t)
		client, cleanup = newTestClient(t, addr)
		serviceName     = "streamMetaService"
	)

	defer listener.Close()
	defer cleanup()

	desc := &ServiceDesc{
		Streams: map[string]Stream{
			"Meta": {
				Handler: func(hctx context.Context, ss StreamServer) (any, error) {
					v, _ := GetMetadataValue(hctx, "foo")
					resp := internal.EchoPayload{Msg: v}
					if _, ok := hctx.Deadline(); ok {
						resp.Seq = 1
					}
					if err := ss.SendMsg(&resp); err != nil {
						return nil, err
					}
					// Drain until the client closes.
					for {
						var req internal.EchoPayload
						if err := ss.RecvMsg(&req); err != nil {
							if err == io.EOF {
								err = nil
							}
							return nil, err
						}
					}
				},
				StreamingClient: true,
				StreamingServer: true,
			},
		},
	}
	server.RegisterService(serviceName, desc)

	go server.Serve(ctx, listener)
	defer server.Shutdown(ctx)

	md := MD{}
	md.Set("foo", "stream-value")
	streamCtx, cancel := context.WithTimeout(WithMetadata(ctx, md), 10*time.Second)
	defer cancel()

	stream, err := client.NewStream(streamCtx, &StreamDesc{StreamingClient: true, StreamingServer: true}, serviceName, "Meta", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.CloseSend()

	var resp internal.EchoPayload
	if err := stream.RecvMsg(&resp); err != nil {
		t.Fatalf("recv: %v", err)
	}
	if resp.Msg != "stream-value" {
		t.Errorf("server saw metadata %q, expected %q", resp.Msg, "stream-value")
	}
	if resp.Seq != 1 {
		t.Error("server did not observe the call deadline from context")
	}
}

// TestStreamCancelPropagatesToServer verifies that cancelling the caller's
// context closes the stream on the server side through context/message
// propagation, without tearing down the shared connection.
func TestStreamCancelPropagatesToServer(t *testing.T) {
	var (
		ctx             = context.Background()
		server          = mustServer(t)(NewServer())
		addr, listener  = newTestListener(t)
		client, cleanup = newTestClient(t, addr)
		serviceName     = "streamCancelPropService"
	)

	defer listener.Close()
	defer cleanup()

	handlerStarted := make(chan struct{}, 1)
	handlerDone := make(chan error, 1)
	desc := &ServiceDesc{
		Methods: map[string]Method{
			"Echo": func(_ context.Context, unmarshal func(any) error) (any, error) {
				var req internal.EchoPayload
				if err := unmarshal(&req); err != nil {
					return nil, err
				}
				return &req, nil
			},
		},
		Streams: map[string]Stream{
			"Block": {
				Handler: func(hctx context.Context, ss StreamServer) (any, error) {
					handlerStarted <- struct{}{}
					var req internal.EchoPayload
					err := ss.RecvMsg(&req) // blocks until the client closes or cancels
					handlerDone <- err
					if err == io.EOF {
						return nil, nil
					}
					return nil, err
				},
				StreamingClient: true,
				StreamingServer: true,
			},
		},
	}
	server.RegisterService(serviceName, desc)

	go server.Serve(ctx, listener)
	defer server.Shutdown(ctx)

	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := client.NewStream(streamCtx, &StreamDesc{StreamingClient: true, StreamingServer: true}, serviceName, "Block", nil)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-handlerStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("server handler did not start within 2s")
	}

	// Cancel the call context: the server's RecvMsg must unblock without
	// the connection being closed.
	cancel()

	select {
	case err := <-handlerDone:
		if err != io.EOF {
			t.Errorf("server RecvMsg returned %v, expected io.EOF", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server handler did not observe stream cancellation within 2s")
	}

	// The shared connection is still usable for subsequent calls.
	var req, resp internal.EchoPayload
	req.Msg = "still alive"
	if err := client.Call(ctx, serviceName, "Echo", &req, &resp); err != nil {
		t.Fatalf("connection unusable after stream cancellation: %v", err)
	}
	if resp.Msg != req.Msg {
		t.Fatalf("unexpected echo reply: %q != %q", resp.Msg, req.Msg)
	}

	_ = stream // stream is done; nothing further to read
}
