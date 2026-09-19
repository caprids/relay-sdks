# Generic MoQ core

An independently buildable package for MOQT sessions and arbitrary track objects. It has no runtime
dependencies and does not import Relay product code or Video.js.

`createMoqtSession` owns the supplied established transport. Subscribe, fetch, and track-status
requests use callback handlers, preserve uninterpreted payloads and object properties, and expose
protocol lifecycle notifications. `MoqTrackSource` describes borrowed track access without shutdown.

The implementation and regression tests originate in Video.js under Apache-2.0. This initial
extraction preserves the wire implementation; ESM imports, test-runner imports, and the default SETUP implementation identifier are adapted.
See `NOTICE` and `UPSTREAM.json`. The original Video.js modules remain pending dependency migration.

This package follows MOQT draft 20 and receives subscription objects over reliable streams. It does
not implement application timing, decoding, datagram reception, or automatic session recovery.

`createMoqtPublishSession` serves a namespace and its registered tracks over a separate transport.
The extracted publish driver excludes SPF actors and media dependencies. Track properties are
explicit, with no default media timescale. `createSubgroupWriter` writes arbitrary object payloads
and metadata; the SDK supplies the bounded, single-object-group publishing policy above it.

For Relay application usage and local installation, start with the [JavaScript quickstart](../../README.md#first-round-trip).
