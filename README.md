# ttrpc

[![Build Status](https://github.com/containerd/ttrpc/actions/workflows/ci.yml/badge.svg)](https://github.com/containerd/ttrpc/actions/workflows/ci.yml)

GRPC for low-memory environments.

The existing grpc-go project requires a lot of memory overhead for importing
packages and at runtime. While this is great for many services with low density
requirements, this can be a problem when running a large number of services on
a single machine or on a machine with a small amount of memory.

Using the same GRPC definitions, this project reduces the binary size and
protocol overhead required. We do this by eliding the `net/http`, `net/http2`
and `grpc` package used by grpc replacing it with a lightweight framing
protocol. The result are smaller binaries that use less resident memory with
the same ease of use as GRPC.

Please note that while this project supports generating either end of the
protocol, the generated service definitions will be incompatible with regular
GRPC services, as they do not speak the same protocol.

# Protocol

See the [protocol specification](./PROTOCOL.md).

# Usage

It's recommended to use [`protobuild`](https://github.com/containerd/protobuild)
to build the protobufs for this project, but this will work with protoc
directly, if required.

# Differences from GRPC

- The protocol stack has been replaced with a lighter protocol that doesn't
  require http, http2 and tls.
- The client and server interface are identical whereas in GRPC there is a
  client and server interface that are different.
- The Go stdlib context package is used instead.

# Stream receive budgets

By default, ttrpc bounds each stream's receive buffer by message count. If a
stream's consumer falls behind and its buffer stays full, the stream is closed
with `ErrStreamFull` after a short timeout so one slow stream cannot stall the
shared connection forever.

For explicit control over the cumulative *unconsumed payload bytes* a single
stream is allowed to retain, opt into a per-stream receive budget:

- `WithStreamReceiveBudget(bytes)` on the client.
- `WithServerStreamReceiveBudget(bytes)` on the server.

When a stream reaches its budget, only that stream's inbound delivery is
back-pressured; its in-flight frame is held in a single bounded overflow slot
while the connection continues servicing other streams in bounded round-robin
order. Consuming a message (`RecvMsg`) releases the budget and immediately
resumes delivery, so a slow stream can delay but never starve a healthy stream
sharing the connection. Cancellation or closing a stream wakes both sides and
leaves a single consistent terminal state.

A budget of zero (the default) preserves the legacy timeout-based behavior, so
existing code and the stream client/server APIs are unaffected. Note that
ttrpc uses a single framed connection without protocol-level flow control, so
outbound writes remain serialized; the receive budget bounds inbound memory and
scheduling, it does not turn the transport into a multiplexed flow-controlled
channel.

# Status

TODO:

- [ ] Add testing under concurrent load to ensure
- [ ] Verify connection error handling

# Project details

ttrpc is a containerd sub-project, licensed under the [Apache 2.0 license](./LICENSE).
As a containerd sub-project, you will find the:
 * [Project governance](https://github.com/containerd/project/blob/main/GOVERNANCE.md),
 * [Maintainers](https://github.com/containerd/project/blob/main/MAINTAINERS),
 * and [Contributing guidelines](https://github.com/containerd/project/blob/main/CONTRIBUTING.md)

information in our [`containerd/project`](https://github.com/containerd/project) repository.
