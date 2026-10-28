# Generic MoQ core

An independently buildable package for MOQT sessions and arbitrary track objects. It has no runtime
dependencies and does not import Relay product code or Video.js.

`createMoqtSession` owns the supplied established transport. Subscribe, fetch, and track-status
requests use callback handlers, preserve uninterpreted payloads and object properties, and expose
protocol lifecycle notifications. `MoqTrackSource` describes borrowed track access without shutdown.

The implementation and regression tests originate in Video.js under Apache-2.0. This initial
extraction preserves the wire implementation; ESM imports, test-runner imports, and the default SETUP implementation identifier are adapted.
See `NOTICE` and `UPSTREAM.json`. The original Video.js modules remain pending dependency migration.

This package follows MOQT draft 22, and draft 20 for a peer that selects it, and receives subscription objects over reliable streams. It does
not implement application timing, decoding, datagram reception, or automatic session recovery.

`createMoqtPublishSession` serves a namespace and its registered tracks over a separate transport.
The extracted publish driver excludes SPF actors and media dependencies. Track properties are
explicit, with no default media timescale. `createSubgroupWriter` writes arbitrary object payloads
and metadata; the SDK supplies the bounded, single-object-group publishing policy above it.

Two publish-session hooks exist for embedders that track more than the SDK does. A registered
track's `getLargestObject` puts `LARGEST_OBJECT` in SUBSCRIBE_OK and REQUEST_OK; the session keeps no
objects, so a nonempty fill request then gets a fill fetch stream that is reset after its
FETCH_HEADER. `end({ after })` holds a track's end until a last group is written, also during an
orderly close.

The package index also exports the wire constants and pure helpers an embedder needs around a
session: `TRACK_PROPERTY`, `compareLocations`, `isEmptyFetchRange`, `encodeFetchHeader`,
`isRetryableRequestErrorCode`, `isRetryablePublishDoneStatus`, `utf8Decode`, and the close bounds
`CLOSE_FLUSH_TIMEOUT_MS` and `CLOSE_END_ACK_TIMEOUT_MS`.

## Standing in for a relay

A test that plays the relay's end against a publish session needs two things the client surface leaves out.

`createMoqtSession(transport, { perspective: 'server' })` allocates the server's Request IDs, odd and starting at 1,
and requires the peer's to be even (§6.4.2.1). The default, `'client'`, allocates even IDs from 0 and requires odd
ones. A publish session is always the client and takes each SUBSCRIBE's Request ID as the subscription's Track Alias,
so a server-perspective session's first two subscriptions get aliases 1 and 3.

`@caprid/moq-core/wire` exports the codec for driving a session byte by byte, under the same semver as the index:
`StreamReader` and `utf8Encode`; `ControlMessageDeframer`, `decodeControlMessage` and the `ControlMessage` type;
`encodeSetup`, `encodeSubscribe`, `encodeSubscribeOk`, `encodeSubscribeNamespace`, `encodePublish`,
`encodePublishDone`, `encodeRequestError` and `encodeGoaway`; `STREAM_TYPE`, `isSubgroupHeaderType`,
`readSubgroupHeader`, `readSubgroupObjects` and `readFetchHeader`; `encodeVarint` and `decodeVarint`. Import from it
rather than from `dist/protocol/*`, whose layout is not part of the API.

```ts
import { createMoqtPublishSession, createMoqtSession } from '@caprid/moq-core';
import { encodeSubscribe } from '@caprid/moq-core/wire';

const publisher = createMoqtPublishSession(pair.client);
const relay = createMoqtSession(pair.server, { perspective: 'server' });
// relay.subscribe(...) sends Request IDs 1, 3, 5, ...; a raw SUBSCRIBE from encodeSubscribe needs an odd one too.
```

For Relay application usage and local installation, start with the [JavaScript quickstart](../../GUIDE.md#first-round-trip).

## Stability

Before 1.0, a minor version can break the API. This package releases together with `@caprid/relay`
at the same version; see the [changelog](../../CHANGELOG.md).
