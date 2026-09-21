# Relay SDKs

The Relay SDKs (TypeScript `@mux/relay`, Python `mux_relay`) publish to and subscribe from the Mux
MoQ Relay under one shared behaviour contract. Public names live in
[spec/vocabulary.md](spec/vocabulary.md) and are not repeated here. This glossary holds the
internal, relay-side and test vocabulary that the code, its tests and its reviews use.

## Language

### Publishing internals

**Write slot**:
A track's single place for a pending operation. Only one write or open group may occupy it at a
time; a second is refused, never queued.
_Avoid_: in-flight slot, lock, mutex, binding

**Operation**:
One pending write or open group occupying a track's write slot, from the moment it begins until it
completes or fails. Its steps are the phases `spec/behavior.md` records: waiting-for-demand,
opening-stream, writing, closing.
_Avoid_: request, job, transaction, publish

### Test vocabulary

**Relay hub**:
The in-memory fake relay the product tests run against over a transport pair. It speaks MOQT to a
publisher connection and a subscriber connection and owns the relay behaviour the core does not.
_Avoid_: mock relay, relay stub, test server
