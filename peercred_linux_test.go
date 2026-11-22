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
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/containerd/ttrpc/internal"
	"golang.org/x/sys/unix"
)

func euid() int { return os.Geteuid() }
func egid() int { return os.Getegid() }

// TestUnixPeerCredentialsBoundPerConnection verifies the property the
// credential binding exists for: every accepted connection captures its own
// peer identity once, and that identity is frozen for the connection's
// lifetime. A request can never observe the credentials of a later connection.
func TestUnixPeerCredentialsBoundPerConnection(t *testing.T) {
	addr, stop := startPeerServer(t, WithUnixPeerCredentials(nil))
	defer stop()

	ctx := context.Background()

	clientA, connA := dialPeer(t, addr)
	defer func() { clientA.Close(); connA.Close() }()
	clientB, connB := dialPeer(t, addr)
	defer func() { clientB.Close(); connB.Close() }()

	who := func(c *Client) *internal.TestPayload {
		resp := &internal.TestPayload{}
		if err := c.Call(ctx, "peer", "Who", &internal.TestPayload{}, resp); err != nil {
			t.Fatalf("who: %v", err)
		}
		return resp
	}

	a := who(clientA)
	b := who(clientB)

	if a.Foo == "absent" || b.Foo == "absent" {
		t.Fatalf("handler did not observe credentials: A=%q B=%q", a.Foo, b.Foo)
	}
	if a.Metadata == "" || b.Metadata == "" {
		t.Fatal("handler observed an empty connection identity")
	}
	if a.Metadata == b.Metadata {
		t.Fatalf("two connections share connection identity %q", a.Metadata)
	}

	gotUID := uint32(a.Deadline >> 32)
	gotGID := uint32(a.Deadline & 0xffffffff)
	if gotUID != uint32(euid()) || gotGID != uint32(egid()) {
		t.Fatalf("captured uid:gid %d:%d, want %d:%d", gotUID, gotGID, euid(), egid())
	}

	// Repeated requests on one connection must keep reporting the same
	// frozen identity, not a value re-read per request.
	for i := 0; i < 3; i++ {
		if again := who(clientA); again.Metadata != a.Metadata {
			t.Fatalf("connection identity changed between requests: %q then %q", a.Metadata, again.Metadata)
		}
	}
}

// TestUnixPeerCredentialsReconnectGetsNewIdentity verifies that a reconnect
// from the same process is a new connection identity, and that the old
// connection cannot affect it.
func TestUnixPeerCredentialsReconnectGetsNewIdentity(t *testing.T) {
	addr, stop := startPeerServer(t, WithUnixPeerCredentials(nil))
	defer stop()

	ctx := context.Background()

	first, conn := dialPeer(t, addr)
	var who internal.TestPayload
	if err := first.Call(ctx, "peer", "Who", &internal.TestPayload{}, &who); err != nil {
		t.Fatalf("first call: %v", err)
	}
	oldID := who.Metadata
	first.Close()
	conn.Close()

	second, conn2 := dialPeer(t, addr)
	defer func() { second.Close(); conn2.Close() }()

	var who2 internal.TestPayload
	if err := second.Call(ctx, "peer", "Who", &internal.TestPayload{}, &who2); err != nil {
		t.Fatalf("second call: %v", err)
	}
	if who2.Foo == "absent" {
		t.Fatal("reconnected client observed no credentials")
	}
	if who2.Metadata == oldID {
		t.Fatalf("reconnect reused connection identity %q", oldID)
	}
}

// TestUnixPeerCredentialsLegacyHandshakeCompatibility ensures servers using the
// historical UnixSocketRequireSameUser handshaker keep working unchanged, and
// that the credentials it captured are now visible to handlers.
func TestUnixPeerCredentialsLegacyHandshakeCompatibility(t *testing.T) {
	addr, stop := startPeerServer(t, WithServerHandshaker(UnixSocketRequireSameUser()))
	defer stop()

	client, conn := dialPeer(t, addr)
	defer func() { client.Close(); conn.Close() }()

	var resp internal.TestPayload
	if err := client.Call(context.Background(), "peer", "Who", &internal.TestPayload{}, &resp); err != nil {
		t.Fatalf("call: %v", err)
	}
	if resp.Foo == "absent" {
		t.Fatal("legacy handshaker credentials were not bound to the connection")
	}
	if gotUID := uint32(resp.Deadline >> 32); gotUID != uint32(euid()) {
		t.Fatalf("legacy handshake captured uid %d, want %d", gotUID, euid())
	}
}

// TestUnixPeerCredentialsLegacyHandshakeReject still rejects peers that the
// handshaker denied, preserving the historical behaviour.
func TestUnixPeerCredentialsLegacyHandshakeReject(t *testing.T) {
	notUs := (euid() + 1) % 65535
	if notUs == euid() {
		notUs = (notUs + 1) % 65535
	}
	addr, stop := startPeerServer(t, WithServerHandshaker(UnixSocketRequireUidGid(notUs, -1)))
	defer stop()

	client, conn := dialPeer(t, addr)
	defer func() { client.Close(); conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var resp internal.TestPayload
	if err := client.Call(ctx, "peer", "Ping", &internal.TestPayload{}, &resp); err == nil {
		t.Fatal("connection rejected by the legacy handshaker was served")
	}
}

// TestUnixPeerCredentialsVerifierRejects checks the verifier runs against the
// frozen snapshot and rejects mismatches.
func TestUnixPeerCredentialsVerifierRejects(t *testing.T) {
	notUs := (euid() + 1) % 65535
	if notUs == euid() {
		notUs = (notUs + 1) % 65535
	}
	addr, stop := startPeerServer(t, WithUnixPeerCredentials(RequirePeerUID(notUs)))
	defer stop()

	client, conn := dialPeer(t, addr)
	defer func() { client.Close(); conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var resp internal.TestPayload
	if err := client.Call(ctx, "peer", "Ping", &internal.TestPayload{}, &resp); err == nil {
		t.Fatal("connection with rejected credentials was served")
	}
}

// TestUnixPeerCredentialsVerifierAccepts checks a matching verifier does not
// interfere with serving.
func TestUnixPeerCredentialsVerifierAccepts(t *testing.T) {
	addr, stop := startPeerServer(t, WithUnixPeerCredentials(RequirePeerUIDGID(euid(), egid())))
	defer stop()

	client, conn := dialPeer(t, addr)
	defer func() { client.Close(); conn.Close() }()

	var resp internal.TestPayload
	if err := client.Call(context.Background(), "peer", "Ping", &internal.TestPayload{}, &resp); err != nil {
		t.Fatalf("matching credentials were rejected: %v", err)
	}
}

// TestUnixPeerCredentialsCaptureOnSocketPair checks the capture function
// directly against a real Unix socket pair, including that a closed peer's
// credentials remain readable (the kernel snapshots them at connect time).
func TestUnixPeerCredentialsCaptureOnSocketPair(t *testing.T) {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatalf("socketpair: %v", err)
	}
	ourFile := os.NewFile(uintptr(fds[0]), "ours")
	theirFile := os.NewFile(uintptr(fds[1]), "theirs")
	defer ourFile.Close()
	defer theirFile.Close()

	ourConn, err := net.FileConn(ourFile)
	if err != nil {
		t.Fatalf("fileconn: %v", err)
	}
	defer ourConn.Close()
	theirConn, err := net.FileConn(theirFile)
	if err != nil {
		t.Fatalf("fileconn: %v", err)
	}

	creds, err := captureUnixPeerCredentials(ourConn)
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	if creds.UID != uint32(euid()) || creds.GID != uint32(egid()) {
		t.Fatalf("captured uid:gid %d:%d, want %d:%d", creds.UID, creds.GID, euid(), egid())
	}
	if creds.PID != int32(os.Getpid()) {
		t.Fatalf("captured pid %d, want %d", creds.PID, os.Getpid())
	}

	// Closing the peer must not make the snapshot disappear or change.
	theirConn.Close()
	theirFile.Close()
	again, err := captureUnixPeerCredentials(ourConn)
	if err != nil {
		t.Fatalf("capture after peer close: %v", err)
	}
	if again != creds {
		t.Fatalf("credentials changed after peer close: %+v then %+v", creds, again)
	}
}

// TestUnixPeerCredentialsCaptureNonUnixConn covers a connection that is not a
// Unix socket at all.
func TestUnixPeerCredentialsCaptureNonUnixConn(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	if _, err := captureUnixPeerCredentials(server); !errors.Is(err, ErrPeerCredentialsUnavailable) {
		t.Fatalf("expected ErrPeerCredentialsUnavailable for a non-Unix connection, got %v", err)
	}
}

// TestUnixPeerCredentialsUnsupportedOption checks that an option the kernel
// does not implement for this socket fails the capture instead of producing
// zeroed credentials that look like uid 0.
func TestUnixPeerCredentialsUnsupportedOption(t *testing.T) {
	old := soPeerCred
	// SO_ERROR is a real socket option, but its value is a single int, not
	// a ucred. Reading it through the ucred reader must fail rather than
	// zero-fill the missing fields into a would-be uid 0.
	soPeerCred = unix.SO_ERROR
	defer func() { soPeerCred = old }()

	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatalf("socketpair: %v", err)
	}
	ourFile := os.NewFile(uintptr(fds[0]), "ours")
	theirFile := os.NewFile(uintptr(fds[1]), "theirs")
	defer ourFile.Close()
	defer theirFile.Close()

	ourConn, err := net.FileConn(ourFile)
	if err != nil {
		t.Fatalf("fileconn: %v", err)
	}
	defer ourConn.Close()

	_, err = captureUnixPeerCredentials(ourConn)
	if err == nil {
		t.Fatal("capture with a non-ucred option returned credentials")
	}
	if !errors.Is(err, ErrPeerCredentialsUnavailable) && !errors.Is(err, ErrPeerCredentialsTruncated) {
		t.Fatalf("unexpected error kind: %v", err)
	}
}

// TestUnixPeerCredentialsLengthMismatch exercises the strict size check with a
// shorter value than a ucred.
func TestUnixPeerCredentialsLengthMismatch(t *testing.T) {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatalf("socketpair: %v", err)
	}
	ourFile := os.NewFile(uintptr(fds[0]), "ours")
	theirFile := os.NewFile(uintptr(fds[1]), "theirs")
	defer ourFile.Close()
	defer theirFile.Close()

	ourConn, err := net.FileConn(ourFile)
	if err != nil {
		t.Fatalf("fileconn: %v", err)
	}
	defer ourConn.Close()

	// A 4-byte integer option reports a 4-byte length; the ucred reader
	// must reject it rather than zero-fill the missing fields.
	raw, ok := ourConn.(*net.UnixConn)
	if !ok {
		t.Fatalf("unexpected conn type %T", ourConn)
	}
	rs, err := raw.SyscallConn()
	if err != nil {
		t.Fatalf("syscallconn: %v", err)
	}

	var ucredErr error
	if cerr := rs.Control(func(fd uintptr) {
		_, ucredErr = getsockoptUcredStrict(int(fd), unix.SO_TYPE)
	}); cerr != nil {
		t.Fatalf("control: %v", cerr)
	}
	if !errors.Is(ucredErr, ErrPeerCredentialsTruncated) {
		t.Fatalf("expected ErrPeerCredentialsTruncated for a short option, got %v", ucredErr)
	}
}

// TestUnixPeerCredentialsHandleReuseIsolation covers the reported failure mode
// directly: after a connection is closed, a new connection from the same
// process must capture its own credentials and get a new identity, while the
// closed connection's release must not disturb the live one.
func TestUnixPeerCredentialsHandleReuseIsolation(t *testing.T) {
	server := mustServer(t)(NewServer(WithUnixPeerCredentials(nil)))
	registerPeerQueryService(server)

	dir := t.TempDir()
	addr := dir + "/iso.sock"
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

	ctx := context.Background()

	// Establish a live connection B that must survive the churn on A.
	clientB, connB := dialPeer(t, addr)
	defer func() { clientB.Close(); connB.Close() }()

	var bWho internal.TestPayload
	if err := clientB.Call(ctx, "peer", "Who", &internal.TestPayload{}, &bWho); err != nil {
		t.Fatalf("B who: %v", err)
	}

	// Open and close many short-lived connections, as a process that
	// repeatedly connects would.
	for i := 0; i < 10; i++ {
		c, conn := dialPeer(t, addr)
		var who internal.TestPayload
		if err := c.Call(ctx, "peer", "Who", &internal.TestPayload{}, &who); err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		if who.Metadata == bWho.Metadata {
			t.Fatalf("iteration %d: short-lived connection reused B's identity", i)
		}
		c.Close()
		conn.Close()
	}

	// B must still answer with its original, frozen identity.
	var bAgain internal.TestPayload
	if err := clientB.Call(ctx, "peer", "Who", &internal.TestPayload{}, &bAgain); err != nil {
		t.Fatalf("B who after churn: %v", err)
	}
	if bAgain.Metadata != bWho.Metadata {
		t.Fatalf("B identity changed from %q to %q after churn", bWho.Metadata, bAgain.Metadata)
	}
	if bAgain.Deadline != bWho.Deadline {
		t.Fatalf("B credentials changed from %d to %d after churn", bWho.Deadline, bAgain.Deadline)
	}

	waitFor(t, 5*time.Second, "only B's credentials remain", func() bool {
		return server.credentials.len() == 1
	})
	only, ok := server.credentials.lookup(mustParseUint64(t, bWho.Metadata))
	if !ok {
		t.Fatal("B's credentials were released while B is still connected")
	}
	if only.UID != uint32(euid()) || only.GID != uint32(egid()) {
		t.Fatalf("B's credentials changed: %+v", only)
	}
}

func mustParseUint64(t *testing.T, s string) uint64 {
	t.Helper()
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		t.Fatalf("parse connection id %q: %v", s, err)
	}
	return v
}
