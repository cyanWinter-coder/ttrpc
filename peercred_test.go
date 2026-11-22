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
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/containerd/ttrpc/internal"
)

// registerPeerQueryService registers a service whose "Who" method reports the
// peer credentials the request arrived with, plus a "Ping" method that answers
// with a fixed value. Both are used to look at the credential binding from the
// client side.
func registerPeerQueryService(srv *Server) {
	srv.Register("peer", map[string]Method{
		"Who": func(ctx context.Context, unmarshal func(any) error) (any, error) {
			var req internal.TestPayload
			if err := unmarshal(&req); err != nil {
				return nil, err
			}
			creds, ok := PeerCredentialsFromContext(ctx)
			if !ok {
				// Encode "absent" distinctly from "uid 0" so a
				// handler-visible regression cannot pass as root.
				return &internal.TestPayload{Foo: "absent"}, nil
			}
			return &internal.TestPayload{
				Metadata: strconv.FormatUint(creds.ConnectionID, 10),
				Deadline: int64(creds.UID)<<32 | int64(creds.GID),
			}, nil
		},
		"Ping": func(_ context.Context, _ func(any) error) (any, error) {
			return &internal.TestPayload{Foo: "pong"}, nil
		},
	})
}

// startPeerServer starts a server with a Unix listener on a private temp path
// and returns the address it listens on.
func startPeerServer(t *testing.T, opts ...ServerOpt) (string, func()) {
	t.Helper()

	server := mustServer(t)(NewServer(opts...))
	registerPeerQueryService(server)

	dir := t.TempDir()
	addr := dir + "/peer.sock"
	listener, err := net.Listen("unix", addr)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	serveCtx, cancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(serveCtx, listener)
	}()

	return addr, func() {
		cancel()
		server.Close()
		listener.Close()
		select {
		case <-serveErr:
		case <-time.After(5 * time.Second):
			t.Error("server did not stop")
		}
	}
}

func dialPeer(t *testing.T, addr string) (*Client, net.Conn) {
	t.Helper()

	conn, err := net.Dial("unix", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return NewClient(conn), conn
}

// TestPeerCredentialsCaptureErrorRejectsConnection checks that a capture
// failure is terminal for the connection: the peer is never served, so it can
// never be treated as anonymous.
func TestPeerCredentialsCaptureErrorRejectsConnection(t *testing.T) {
	captureErr := errors.New("no credentials for you")
	addr, stop := startPeerServer(t, WithPeerCredentialsCapture(
		func(net.Conn) (PeerCredentials, error) { return PeerCredentials{}, captureErr },
		nil,
	))
	defer stop()

	client, conn := dialPeer(t, addr)
	defer conn.Close()
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var resp internal.TestPayload
	if err := client.Call(ctx, "peer", "Ping", &internal.TestPayload{}, &resp); err == nil {
		t.Fatal("connection with failed credential capture was served")
	}
}

// TestPeerCredentialsReleasedExactlyOnce checks that credential entries are
// created and released one-for-one as connections come and go, including when
// connections close concurrently with a server shutdown.
func TestPeerCredentialsReleasedExactlyOnce(t *testing.T) {
	server := mustServer(t)(NewServer(WithPeerCredentialsCapture(
		func(net.Conn) (PeerCredentials, error) { return PeerCredentials{UID: 1, GID: 2}, nil },
		nil,
	)))
	registerPeerQueryService(server)

	dir := t.TempDir()
	addr := dir + "/peer.sock"
	listener, err := net.Listen("unix", addr)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	serveCtx, cancel := context.WithCancel(context.Background())
	go server.Serve(serveCtx, listener)

	const n = 16
	clients := make([]*Client, 0, n)
	conns := make([]net.Conn, 0, n)
	for i := 0; i < n; i++ {
		c, conn := dialPeer(t, addr)
		var resp internal.TestPayload
		if err := c.Call(context.Background(), "peer", "Ping", &internal.TestPayload{}, &resp); err != nil {
			t.Fatalf("client %d: %v", i, err)
		}
		clients = append(clients, c)
		conns = append(conns, conn)
	}

	waitFor(t, 5*time.Second, "all connections registered", func() bool {
		return server.credentials.len() == n
	})

	// Close half of the connections and confirm their entries go away.
	for i := 0; i < n/2; i++ {
		clients[i].Close()
		conns[i].Close()
	}
	waitFor(t, 5*time.Second, "half released", func() bool {
		return server.credentials.len() == n/2
	})

	// Shut down; every remaining entry must be released.
	cancel()
	server.Close()
	listener.Close()
	waitFor(t, 5*time.Second, "all released", func() bool {
		return server.credentials.len() == 0
	})

	server.credentials.mu.Lock()
	removed := server.credentials.removed
	server.credentials.mu.Unlock()
	if removed != n {
		t.Fatalf("credential entries removed %d times, want exactly %d", removed, n)
	}
}

func TestPeerCredentialsCacheReleaseIsGenerationScoped(t *testing.T) {
	cache := newCredentialCache()
	cache.add(PeerCredentials{ConnectionID: 1, UID: 1000})
	cache.add(PeerCredentials{ConnectionID: 2, UID: 1000})

	// The stale connection (generation 1) closes after a newer connection
	// (generation 2) was opened by the same process.
	if !cache.remove(1) {
		t.Fatal("first release of generation 1 did not report removal")
	}
	if cache.remove(1) {
		t.Fatal("repeated release of generation 1 reported removal")
	}
	if _, ok := cache.lookup(2); !ok {
		t.Fatal("closing the old connection removed the new connection's credentials")
	}
	if cache.len() != 1 {
		t.Fatalf("cache holds %d entries, want 1", cache.len())
	}
}

func TestPeerCredentialsCacheConcurrentRelease(t *testing.T) {
	cache := newCredentialCache()
	const gens = 64
	for i := uint64(1); i <= gens; i++ {
		cache.add(PeerCredentials{ConnectionID: i, UID: 1000})
	}

	var (
		wg      sync.WaitGroup
		removed int64
	)
	for i := uint64(1); i <= gens; i++ {
		// Four goroutines race to release each generation; only one may
		// win, regardless of interleaving.
		for r := 0; r < 4; r++ {
			wg.Add(1)
			go func(id uint64) {
				defer wg.Done()
				if cache.remove(id) {
					atomic.AddInt64(&removed, 1)
				}
			}(i)
		}
	}
	wg.Wait()

	if removed != gens {
		t.Fatalf("released %d generations, want exactly %d", removed, gens)
	}
	if cache.len() != 0 {
		t.Fatalf("cache not empty: %d", cache.len())
	}
}

func TestPeerCredentialsContextRoundTrip(t *testing.T) {
	creds := PeerCredentials{UID: 1234, GID: 5678, PID: 99, ConnectionID: 7}
	ctx := WithPeerCredentials(context.Background(), creds)

	got, ok := PeerCredentialsFromContext(ctx)
	if !ok {
		t.Fatal("credentials missing from context")
	}
	if got != creds {
		t.Fatalf("got %+v, want %+v", got, creds)
	}

	if _, ok := PeerCredentialsFromContext(context.Background()); ok {
		t.Fatal("bare context reported credentials")
	}
}

func TestPeerCredentialsHandshakeBinding(t *testing.T) {
	if creds, ok := peerCredentialsFromHandshake(nil); ok {
		t.Fatalf("nil handshake produced credentials: %+v", creds)
	}
	if creds, ok := peerCredentialsFromHandshake("not credentials"); ok {
		t.Fatalf("unexpected credentials from foreign payload: %+v", creds)
	}
	got, ok := peerCredentialsFromHandshake(PeerCredentials{UID: 42, GID: 43, ConnectionID: 999})
	if !ok {
		t.Fatal("PeerCredentials handshake payload was not recognized")
	}
	if got.ConnectionID != 0 {
		t.Fatalf("handshake-supplied identity %d was not cleared", got.ConnectionID)
	}
}

func TestPeerCredentialsVerifierHelpers(t *testing.T) {
	creds := PeerCredentials{UID: 1000, GID: 2000}

	if err := RequirePeerUID(1000)(creds); err != nil {
		t.Fatalf("matching uid rejected: %v", err)
	}
	if err := RequirePeerUID(-1)(creds); err != nil {
		t.Fatalf("wildcard uid rejected: %v", err)
	}
	err := RequirePeerUID(0)(creds)
	if !errors.Is(err, ErrPeerCredentialsRejected) {
		t.Fatalf("mismatched uid: got %v, want ErrPeerCredentialsRejected", err)
	}

	if err := RequirePeerUIDGID(1000, 2000)(creds); err != nil {
		t.Fatalf("matching uid:gid rejected: %v", err)
	}
	if err := RequirePeerUIDGID(1000, -1)(creds); err != nil {
		t.Fatalf("wildcard gid rejected: %v", err)
	}
	if err := RequirePeerUIDGID(-1, 2000)(creds); err != nil {
		t.Fatalf("wildcard uid rejected: %v", err)
	}
	if err := RequirePeerUIDGID(1000, 0)(creds); !errors.Is(err, ErrPeerCredentialsRejected) {
		t.Fatalf("mismatched gid: got %v, want ErrPeerCredentialsRejected", err)
	}
}

func TestWithPeerCredentialsCaptureRejectsNil(t *testing.T) {
	if _, err := NewServer(WithPeerCredentialsCapture(nil, nil)); err == nil {
		t.Fatal("nil capture function was accepted")
	}
}

func TestWithPeerCredentialsCaptureOnlyOnce(t *testing.T) {
	_, err := NewServer(
		WithUnixPeerCredentials(nil),
		WithUnixPeerCredentials(nil),
	)
	if err == nil {
		t.Fatal("configuring credential capture twice was accepted")
	}
}

// TestPeerCredentialsConcurrentAcceptUniqueGenerations drives many concurrent
// connects against a single server and confirms every connection gets a
// distinct identity and that all credential entries are released once the
// clients go away.
func TestPeerCredentialsConcurrentAcceptUniqueGenerations(t *testing.T) {
	server := mustServer(t)(NewServer(WithPeerCredentialsCapture(
		func(net.Conn) (PeerCredentials, error) { return PeerCredentials{UID: 7, GID: 8}, nil },
		nil,
	)))
	registerPeerQueryService(server)

	dir := t.TempDir()
	addr := dir + "/conc.sock"
	listener, err := net.Listen("unix", addr)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	serveCtx, cancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(serveCtx, listener) }()
	defer func() {
		cancel()
		server.Close()
		listener.Close()
		<-serveErr
	}()

	const n = 32
	var (
		wg  sync.WaitGroup
		mu  sync.Mutex
		ids = make(map[string]struct{}, n)
	)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client, conn := dialPeer(t, addr)
			defer func() { client.Close(); conn.Close() }()

			var resp internal.TestPayload
			if err := client.Call(context.Background(), "peer", "Who", &internal.TestPayload{}, &resp); err != nil {
				t.Errorf("call: %v", err)
				return
			}
			if resp.Foo == "absent" {
				t.Error("handler observed no credentials")
				return
			}
			mu.Lock()
			if _, dup := ids[resp.Metadata]; dup {
				t.Errorf("duplicate connection identity %q", resp.Metadata)
			}
			ids[resp.Metadata] = struct{}{}
			mu.Unlock()
		}()
	}
	wg.Wait()

	if len(ids) != n {
		t.Fatalf("saw %d distinct identities, want %d", len(ids), n)
	}
	waitFor(t, 5*time.Second, "all concurrent entries released", func() bool {
		return server.credentials.len() == 0
	})
}

// waitFor polls until cond is true, failing the test at the deadline.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}
