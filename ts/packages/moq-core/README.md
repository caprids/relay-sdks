# Generic MoQ core

`@caprid/moq-core` runs MOQT sessions that carry arbitrary track objects. It has no runtime dependencies and does not
import Relay product code or Video.js.

The core speaks MOQT draft 22, and draft 20 with a peer that selects it. It receives subscription objects over reliable
streams. It does not implement application timing, decoding, datagram reception, or automatic session recovery.

## Subscribe side

`createMoqtSession` takes an established transport and owns it from then on. Subscribe, fetch, and track-status requests
take callback handlers. They pass payloads and object properties through uninterpreted and report protocol lifecycle
notifications. `MoqTrackSource` is borrowed access to a session's `subscribe`, `fetch`, and `trackStatus`: a holder can
make requests but cannot close the transport.

## Publish side

`createMoqtPublishSession` serves a namespace and its registered tracks over a separate transport. Track properties are
explicit, so a track has no default media timescale. `createSubgroupWriter` writes arbitrary object payloads and
metadata. The publishing policy above it, such as `@caprid/relay`'s bounded single-object groups, belongs to the SDK.

Two publish-session hooks serve embedders that track more than the SDK does:

- A registered track's `getLargestObject` puts `LARGEST_OBJECT` in SUBSCRIBE_OK and REQUEST_OK. The session keeps no
  objects, so it answers a nonempty fill request with a fill fetch stream that it resets after the FETCH_HEADER.
- `end({ after })` holds a track's end until a last group is written. It also holds during an orderly close.

## Wire helpers

The package index also exports the wire constants and pure helpers that an embedder needs around a session:
`TRACK_PROPERTY`, `compareLocations`, `isEmptyFetchRange`, `encodeFetchHeader`, `isRetryableRequestErrorCode`,
`isRetryablePublishDoneStatus`, `utf8Decode`, and the close bounds `CLOSE_FLUSH_TIMEOUT_MS` and
`CLOSE_END_ACK_TIMEOUT_MS`.

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

## Provenance

The implementation and its regression tests come from Video.js, under Apache-2.0. `UPSTREAM.json` records the source
revision, the original file hashes, and every change made since, and `NOTICE` carries the attribution. Video.js still
has its own copy of these modules. Moving Video.js onto this package is a separate step.

For Relay application usage and local installation, start with the
[JavaScript quickstart](https://github.com/caprids/relay-sdks/blob/main/ts/GUIDE.md#first-round-trip).

## Stability

Before 1.0, a minor version can break the API. This package releases together with `@caprid/relay` at the same version.
See the [changelog](https://github.com/caprids/relay-sdks/blob/main/ts/CHANGELOG.md).
