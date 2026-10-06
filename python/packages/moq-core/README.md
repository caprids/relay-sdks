# Generic MoQ core (Python)

An independently buildable package for MOQT sessions and arbitrary track objects. It has no runtime
dependencies and does not import Relay product code or any transport library.

`create_moqt_session` owns a supplied, established transport. Subscribe, fetch, and track-status
requests use callback handlers, preserve uninterpreted payloads and object properties, and expose
protocol lifecycle notifications. `MoqTrackSource` describes borrowed track access without shutdown.

`create_moqt_publish_session` serves a namespace and its registered tracks over a separate
transport. Track properties are explicit, with no default media timescale. `create_subgroup_writer`
writes arbitrary object payloads and metadata; the SDK supplies the bounded, single-object-group
publishing policy above it.

The transport seam is `moq_core.transport.MoqtTransport`: the structural subset of a WebTransport
session the drivers need, spelled for asyncio. `mux_relay` supplies the aioquic implementation and
tests supply in-memory pairs.

This package follows MOQT draft 22, and draft 20 for a peer that selects it, and receives subscription objects over reliable streams. It does
not implement application timing, decoding, datagram reception, or automatic session recovery.

The implementation is a port of `ts/packages/moq-core`, which is itself an attributed snapshot of the
Video.js protocol modules under Apache-2.0. See `NOTICE` and `UPSTREAM.json`.

For Relay application usage and local installation, start with the [Python quickstart](../../README.md#first-round-trip).

The distribution on PyPI is `mux-moq-core`; the import name is `moq_core`.

## Stability

Before 1.0, a minor version can break the API. This package releases together with `mux-relay` at
the same version; see the [changelog](../../CHANGELOG.md).
