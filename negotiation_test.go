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
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/containerd/ttrpc/internal"
)

// negotiatedTestPair wires a negotiation enabled server to a client that
// negotiates over the same connection. Extra server options (for example a
// short negotiation timeout) are applied before serving starts so no
// handshaker field is mutated concurrently with Serve.
func negotiatedTestPair(t testing.TB, serverOpts ...ServerOpt) (*Server, string, net.Listener) {
	t.Helper()

	server := mustServer(t)(NewServer(append(serverOpts, WithServerNegotiation())...))
	addr, listener := newTestListener(t)

	go func() { _ = server.Serve(context.Background(), listener) }()
	t.Cleanup(func() {
		_ = server.Shutdown(context.Background())
		_ = listener.Close()
	})
	return server, addr, listener
}

func dialNegotiatedClient(t testing.TB, addr string, cfg ClientNegotiation) (*Client, func()) {
	t.Helper()
	conn, err := net.Dial("unix", addr)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := NewClientWithNegotiation(ctx, conn, cfg)
	if err != nil {
		_ = conn.Close()
		t.Fatalf("negotiation failed: %v", err)
	}
	return client, func() {
		_ = client.Close()
	}
}

// TestNegotiationHappyPath covers requirement 1 and 2: version, services and
// capabilities are exchanged, the result is published once and an RPC round
// trip works against the negotiated view.
func TestNegotiationHappyPath(t *testing.T) {
	server, addr, _ := negotiatedTestPair(t)
	registerTestingService(server, &testingServer{})

	cfg := NewClientNegotiation(nil)
	client, cleanup := dialNegotiatedClient(t, addr, cfg)
	defer cleanup()

	result, ok := client.NegotiationResult()
	if !ok {
		t.Fatal("expected negotiated result on client")
	}
	if result.Version != ProtocolVersion {
		t.Fatalf("unexpected negotiated version: %d", result.Version)
	}
	if result.Capabilities&CapabilityStreaming == 0 {
		t.Fatalf("streaming capability missing from intersection: %b", result.Capabilities)
	}
	if !contains(result.Services, serviceName) {
		t.Fatalf("negotiated services %v do not contain %q", result.Services, serviceName)
	}
	if len(result.Services) != 1 {
		t.Fatalf("expected exactly one service in snapshot, got %v", result.Services)
	}

	var tp internal.TestPayload
	if err := client.Call(context.Background(), serviceName, "Test", &tp, &tp); err != nil {
		t.Fatalf("rpc after negotiation failed: %v", err)
	}
}

// TestNegotiationMissingRequiredService rejects the connection immediately
// when a required service is absent (requirement 1).
func TestNegotiationMissingRequiredService(t *testing.T) {
	server, addr, _ := negotiatedTestPair(t)
	registerTestingService(server, &testingServer{})

	conn, err := net.Dial("unix", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = NewClientWithNegotiation(ctx, conn, NewClientNegotiation([]string{"missingService"}))
	if err == nil {
		t.Fatal("expected negotiation failure for missing required service")
	}
	var ne *NegotiationError
	if !errors.As(err, &ne) || ne.Reason() != NegotiationReasonMissing {
		t.Fatalf("expected missing-service negotiation error, got %v", err)
	}

	// The single cleanup path must have closed the socket: a second read
	// must fail instead of blocking. Ignore the deadline-set error since
	// the socket may already be closed by cleanup.
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("expected the negotiated-away connection to be closed")
	}
}

// TestNegotiationSnapshotFrozen verifies requirement 2: services registered
// after the handshake do not change the published view, while the server's
// global service set keeps accepting new registrations for later
// connections.
func TestNegotiationSnapshotFrozen(t *testing.T) {
	server, addr, _ := negotiatedTestPair(t)
	registerTestingService(server, &testingServer{})

	client, cleanup := dialNegotiatedClient(t, addr, NewClientNegotiation(nil))
	defer cleanup()

	result, _ := client.NegotiationResult()
	if len(result.Services) != 1 {
		t.Fatalf("expected one service at handshake, got %v", result.Services)
	}

	// A late registration must not mutate the already published snapshot.
	const lateService = "lateService"
	server.Register(lateService, map[string]Method{
		"Late": func(context.Context, func(any) error) (any, error) { return nil, nil },
	})

	result2, _ := client.NegotiationResult()
	if len(result2.Services) != 1 || contains(result2.Services, lateService) {
		t.Fatalf("handshaked view changed after late registration: %v", result2.Services)
	}

	// A freshly negotiated connection sees the new service.
	client2, cleanup2 := dialNegotiatedClient(t, addr, NewClientNegotiation(nil))
	defer cleanup2()
	fresh, _ := client2.NegotiationResult()
	if !contains(fresh.Services, lateService) || !contains(fresh.Services, serviceName) {
		t.Fatalf("new connection did not observe late registration: %v", fresh.Services)
	}
}

// TestNegotiationIncompatibleVersion refuses peers whose version window
// does not intersect the server's supported range (requirement 1).
func TestNegotiationIncompatibleVersion(t *testing.T) {
	server, addr, _ := negotiatedTestPair(t)
	registerTestingService(server, &testingServer{})

	conn, err := net.Dial("unix", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := NewClientNegotiation(nil).WithVersion(99, 98)
	_, err = NewClientWithNegotiation(ctx, conn, cfg)
	var ne *NegotiationError
	if !errors.As(err, &ne) || ne.Reason() != NegotiationReasonVersion {
		t.Fatalf("expected version negotiation error, got %v", err)
	}
}

// TestNegotiationTruncatedExchange drives requirement 3: a peer that sends
// a partial offer and disappears must release the connection through the
// single cleanup path with a truncated/timeout reason, without waiting for
// any process exit.
func TestNegotiationTruncatedExchange(t *testing.T) {
	server, addr, _ := negotiatedTestPair(t, WithNegotiationTimeout(200*time.Millisecond))
	registerTestingService(server, &testingServer{})

	conn, err := net.Dial("unix", addr)
	if err != nil {
		t.Fatal(err)
	}

	// Send a valid frame header promising a payload, then only half of it
	// and close the write side.
	payload, err := negotiationOffer{
		version:      ProtocolVersion,
		minVersion:   MinSupportedProtocolVersion,
		capabilities: CapabilityStreaming,
	}.marshal()
	if err != nil {
		t.Fatal(err)
	}
	ch := newChannel(conn)
	if err := ch.send(negotiationStreamID, messageTypeNegotiation, 0, payload[:len(payload)/2]); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()

	// The server side must reap the connection. Poll the server's
	// connection set instead of sleeping on a process boundary.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if server.countConnection() == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("truncated negotiation leaked a server connection: %d remain", server.countConnection())
}

// TestNegotiationTimeoutReleasesResources verifies requirement 3 for the
// timeout branch: a silent peer is reaped promptly through the single
// cleanup path and leaves no goroutine blocked on the socket.
func TestNegotiationTimeoutReleasesResources(t *testing.T) {
	server, addr, _ := negotiatedTestPair(t, WithNegotiationTimeout(100*time.Millisecond))
	registerTestingService(server, &testingServer{})

	conn, err := net.Dial("unix", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	// Never write anything: the server negotiation must time out.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if server.countConnection() == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if server.countConnection() != 0 {
		t.Fatalf("timed out negotiation leaked a server connection")
	}
}

// TestNegotiationConcurrentClose exercises requirement 4: racing the
// terminal transitions (fail and close, close and close) produces exactly
// one observable result and runs cleanup once.
func TestNegotiationConcurrentClose(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})

	_, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	session := newNegotiationSession(serverConn, cancel)
	if err := session.begin(); err != nil {
		t.Fatal(err)
	}

	const racers = 16
	var wg sync.WaitGroup
	results := make(chan error, racers)
	wg.Add(racers)
	for i := 0; i < racers; i++ {
		i := i
		go func() {
			defer wg.Done()
			if i%2 == 0 {
				results <- session.fail(NegotiationReasonTimeout, context.DeadlineExceeded)
			} else {
				session.close()
				results <- nil
			}
		}()
	}
	wg.Wait()
	close(results)

	terminal := 0
	for range results {
		terminal++
	}
	if terminal != racers {
		t.Fatalf("expected %d racer outcomes, got %d", racers, terminal)
	}

	session.mu.Lock()
	state := session.state
	recorded := session.err
	session.mu.Unlock()
	if state != negotiationStateFailed && state != negotiationStateClosed {
		t.Fatalf("expected a single terminal state, got %s", state)
	}
	if recorded == nil {
		t.Fatal("expected a recorded terminal error")
	}

	// Idempotency: repeating the transitions neither panics nor changes the
	// result.
	if err := session.fail(NegotiationReasonTruncated, errors.New("late")); err != recorded {
		t.Fatalf("repeat fail did not return the recorded error: %v != %v", err, recorded)
	}
	session.close()

	// The socket must be closed exactly once and stay closed.
	if _, err := serverConn.Read(make([]byte, 1)); err == nil {
		t.Fatal("expected session cleanup to close the socket")
	}
}

// TestNegotiationDuplicateBegin covers requirement 4 for repeated
// handshakes: driving the state machine twice cannot produce two
// negotiations or views.
func TestNegotiationDuplicateBegin(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})

	_, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	session := newNegotiationSession(serverConn, cancel)
	if err := session.begin(); err != nil {
		t.Fatal(err)
	}
	err := session.begin()
	var ne *NegotiationError
	if !errors.As(err, &ne) || ne.Reason() != NegotiationReasonDuplicate {
		t.Fatalf("expected duplicate negotiation error, got %v", err)
	}
}

// TestNegotiationPublishedOnce verifies the publish-once guarantee
// (requirement 2) at the state machine level.
func TestNegotiationPublishedOnce(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})

	_, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	session := newNegotiationSession(serverConn, cancel)
	if err := session.begin(); err != nil {
		t.Fatal(err)
	}

	first := &NegotiationResult{Version: 1, Services: []string{"a"}}
	second := &NegotiationResult{Version: 2, Services: []string{"b"}}
	session.publish(first)
	session.publish(second)

	got, err := session.wait(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != first || got.Version != 1 {
		t.Fatalf("published view changed: got %+v", got)
	}
}

// TestNegotiationTerminalRace drives requirement 4 hard: many goroutines
// race publish against fail and close while the session is still running.
// Exactly one terminal outcome must become observable to every waiter.
func TestNegotiationTerminalRace(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})

	_, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	session := newNegotiationSession(serverConn, cancel)
	if err := session.begin(); err != nil {
		t.Fatal(err)
	}

	const racers = 24
	var wg sync.WaitGroup
	wg.Add(racers)
	for i := 0; i < racers; i++ {
		i := i
		go func() {
			defer wg.Done()
			switch i % 3 {
			case 0:
				session.publish(&NegotiationResult{Version: 1})
			case 1:
				_ = session.fail(NegotiationReasonTimeout, context.DeadlineExceeded)
			default:
				session.close()
			}
		}()
	}
	wg.Wait()

	// Every concurrent waiter observes the identical terminal outcome.
	outcomes := make(chan error, 8)
	for i := 0; i < 8; i++ {
		go func() {
			_, err := session.wait(context.Background())
			outcomes <- err
		}()
	}
	var first error
	for i := 0; i < 8; i++ {
		err := <-outcomes
		if first == nil {
			first = err
		} else if !errors.Is(err, first) && err.Error() != first.Error() {
			t.Fatalf("divergent terminal outcomes: %v vs %v", first, err)
		}
	}

	session.mu.Lock()
	state := session.state
	session.mu.Unlock()
	if state != negotiationStateSucceeded && state != negotiationStateFailed &&
		state != negotiationStateClosed {
		t.Fatalf("session never reached a terminal state: %s", state)
	}
}

// TestWithoutNegotiationNoBytesAndRPCWorks guards requirement 5: the default
// (non-negotiation) path must not write handshake bytes and existing
// services, config and handshaker behavior remain compatible.
func TestWithoutNegotiationNoBytesAndRPCWorks(t *testing.T) {
	server := mustServer(t)(NewServer())
	registerTestingService(server, &testingServer{})

	addr, listener := newTestListener(t)
	go func() { _ = server.Serve(context.Background(), listener) }()
	t.Cleanup(func() {
		_ = server.Shutdown(context.Background())
		_ = listener.Close()
	})

	conn, err := net.Dial("unix", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	// A plain NewClient must negotiate nothing.
	client := NewClient(conn)
	t.Cleanup(func() { _ = client.Close() })
	if _, ok := client.NegotiationResult(); ok {
		t.Fatal("plain client unexpectedly carries a negotiation result")
	}

	var tp internal.TestPayload
	if err := client.Call(context.Background(), serviceName, "Test", &tp, &tp); err != nil {
		t.Fatalf("plain client rpc failed: %v", err)
	}
}

// TestNegotiationComposesWithHandshaker ensures negotiation wraps rather
// than replaces an existing Handshaker such as the unix credential checks
// (requirement 5).
func TestNegotiationComposesWithHandshaker(t *testing.T) {
	var innerCalled bool
	inner := handshakerFunc(func(_ context.Context, conn net.Conn) (net.Conn, any, error) {
		innerCalled = true
		return conn, "inner-data", nil
	})

	server := mustServer(t)(NewServer(WithServerHandshaker(inner), WithServerNegotiation()))
	nh, ok := server.config.handshaker.(*serverNegotiationHandshaker)
	if !ok {
		t.Fatalf("expected composed negotiation handshaker, got %T", server.config.handshaker)
	}
	if nh.inner == nil {
		t.Fatal("negotiation handshaker lost the inner handshaker")
	}

	addr, listener := newTestListener(t)
	go func() { _ = server.Serve(context.Background(), listener) }()
	t.Cleanup(func() {
		_ = server.Shutdown(context.Background())
		_ = listener.Close()
	})
	registerTestingService(server, &testingServer{})

	_, cleanup := dialNegotiatedClient(t, addr, NewClientNegotiation(nil))
	defer cleanup()

	if !innerCalled {
		t.Fatal("inner handshaker was not invoked before negotiation")
	}
}

// TestNegotiationWireRoundTrip checks the binary offer/ack codecs against
// each other, including empty lists and truncation detection.
func TestNegotiationWireRoundTrip(t *testing.T) {
	offer := negotiationOffer{
		version:      7,
		minVersion:   3,
		capabilities: CapabilityStreaming | Capability(42),
		required:     []string{"svc.a", "svc.b", ""},
	}
	raw, err := offer.marshal()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := unmarshalNegotiationOffer(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.version != offer.version || decoded.minVersion != offer.minVersion ||
		decoded.capabilities != offer.capabilities ||
		len(decoded.required) != len(offer.required) {
		t.Fatalf("offer codec mismatch: %+v != %+v", decoded, offer)
	}
	for i := range offer.required {
		if decoded.required[i] != offer.required[i] {
			t.Fatalf("required[%d] mismatch: %q != %q", i, decoded.required[i], offer.required[i])
		}
	}

	ack := negotiationAck{
		version:      5,
		capabilities: CapabilityStreaming,
		services:     []string{"svc.a"},
		status:       negotiationStatusOK,
		message:      "",
	}
	rawAck, err := ack.marshal()
	if err != nil {
		t.Fatal(err)
	}
	decodedAck, err := unmarshalNegotiationAck(rawAck)
	if err != nil {
		t.Fatal(err)
	}
	if decodedAck.version != ack.version || decodedAck.status != ack.status ||
		decodedAck.capabilities != ack.capabilities ||
		len(decodedAck.services) != 1 || decodedAck.services[0] != "svc.a" {
		t.Fatalf("ack codec mismatch: %+v != %+v", decodedAck, ack)
	}

	if _, err := unmarshalNegotiationOffer(raw[:len(raw)-1]); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected ErrUnexpectedEOF on short offer payload, got %v", err)
	}
	if _, err := unmarshalNegotiationAck(rawAck[:len(rawAck)-1]); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected ErrUnexpectedEOF on short ack payload, got %v", err)
	}
}

// TestNegotiationClientCancel covers requirement 3 and 4 on the client:
// canceling the caller context while the server withholds its ack produces
// a single deterministic timeout result, closes the socket and starts no
// client goroutine.
func TestNegotiationClientCancel(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	// The inner handshaker holds the server side before it reads the
	// negotiation offer, so the client blocks waiting for its ack.
	blocking := handshakerFunc(func(ctx context.Context, conn net.Conn) (net.Conn, any, error) {
		select {
		case <-release:
			return conn, nil, nil
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
	})

	server := mustServer(t)(NewServer(WithServerHandshaker(blocking), WithServerNegotiation()))
	addr, listener := newTestListener(t)
	go func() { _ = server.Serve(context.Background(), listener) }()
	t.Cleanup(func() {
		_ = server.Shutdown(context.Background())
		_ = listener.Close()
	})

	conn, err := net.Dial("unix", addr)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err = NewClientWithNegotiation(ctx, conn, NewClientNegotiation(nil))
	var ne *NegotiationError
	if !errors.As(err, &ne) || ne.Reason() != NegotiationReasonTimeout {
		t.Fatalf("expected timeout negotiation error, got %v", err)
	}

	// The single cleanup path closed the socket without any process exit.
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("expected the canceled negotiation connection to be closed")
	}
}

func contains(items []string, want string) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}

// TestNegotiationCapabilityEnforcement verifies capability bits actually
// constrain the connection: when the negotiated intersection lacks
// streaming, the client refuses to open a stream and the server rejects a
// streaming frame. A non-negotiated connection stays unrestricted.
func TestNegotiationCapabilityEnforcement(t *testing.T) {
	server, addr, _ := negotiatedTestPair(t)
	registerTestingService(server, &testingServer{})

	// Negotiate without advertising the streaming capability.
	noStreamCfg := NewClientNegotiation(nil).WithCapabilities(0)
	client, cleanup := dialNegotiatedClient(t, addr, noStreamCfg)
	defer cleanup()

	result, ok := client.NegotiationResult()
	if !ok || result.Capabilities&CapabilityStreaming != 0 {
		t.Fatalf("expected streaming to be absent, got %+v", result)
	}

	_, err := client.NewStream(context.Background(), &StreamDesc{StreamingClient: true},
		serviceName, "Test", nil)
	if !errors.Is(err, ErrProtocol) {
		t.Fatalf("expected ErrProtocol when opening a stream without the capability, got %v", err)
	}

	// A streaming request frame forged onto the same connection must be
	// rejected by the server as well.
	if err := client.send(1, messageTypeRequest, flagRemoteOpen, []byte{}); err != nil {
		t.Fatal(err)
	}
	// Drive one unary call to flush the rejection through the receive loop.
	var tp internal.TestPayload
	err = client.Call(context.Background(), serviceName, "Test", &tp, &tp)
	if err == nil {
		t.Fatal("expected server to reject streaming frame outside negotiated capabilities")
	}

	// Sanity: unary RPC still works on the capability restricted connection.
	if err := client.Call(context.Background(), serviceName, "Test",
		&internal.TestPayload{}, &internal.TestPayload{}); err != nil {
		t.Fatalf("unary rpc on a non-streaming negotiated connection failed: %v", err)
	}
}

// TestNegotiationGoroutineReap is a guard that a failed negotiation does not
// leave the package's reader goroutines or connections behind. It drives
// deterministic server-side refusals (a required service that is never
// registered), since an in-band client cannot stay silent: the client
// handshaker always writes its offer first.
func TestNegotiationGoroutineReap(t *testing.T) {
	server, addr, _ := negotiatedTestPair(t)
	registerTestingService(server, &testingServer{})

	before := runtime.NumGoroutine()
	cfg := NewClientNegotiation([]string{"never-registered"})
	for i := 0; i < 8; i++ {
		conn, err := net.Dial("unix", addr)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err = NewClientWithNegotiation(ctx, conn, cfg)
		cancel()
		if err == nil {
			t.Fatal("expected negotiation refusal")
		} else if !strings.Contains(err.Error(), "negotiation") {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	// The refused connections must be reaped without any process exit.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if server.countConnection() == 0 && runtime.NumGoroutine() <= before+2 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("resources did not settle: connections=%d goroutines before=%d now=%d",
		server.countConnection(), before, runtime.NumGoroutine())
}
