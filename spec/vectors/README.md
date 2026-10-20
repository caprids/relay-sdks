# Test vectors

Language-neutral vectors. Every SDK loads these files in its test suite, so an encoder or decoder
change that breaks the wire format, or an argument rule that drifts in one language, fails in every
language at once.

Conventions:

- Bytes are lowercase hex strings with no separators.
- Integer values are decimal strings, so 64-bit values survive JSON in every language.
- Each file names its source. Prefer examples from the MOQT draft text; boundary cases derived from
  the encoding rules say so.

| File | Covers |
| --- | --- |
| `media.json` | Exact MSF catalog UTF-8 JSON (with a Media Timeline track entry), Media Timeline records, documents and retention, LOC properties, MSF source URLs, H.264 avcC and access units, Opus packet durations from the TOC byte (RFC 6716 §3.1), interrupts built and read, transcript segments built and read and a transcript's steps, and rejected inputs |
| `clock.json` | The broadcast clock's origin, source mapping, capture-clock and sample timelines, and the playout clock's modes and wait/show/drop decisions, per `../media-layer.md` "Shared timeline and playout clock", and the origin's Unix-epoch time, per its "Media Timeline track" |
| `varint.json` | MOQT variable-length integers, draft-ietf-moq-transport-22 §8.1 |
| `setup-options.json` | The PATH and AUTHORITY setup options the core sends for an endpoint URL and token, by framing, per `../transport-adapter.md` TA-6; TypeScript only until Python speaks native QUIC (#199) |
| `location-filter.json` | The LOCATION_FILTER parameter in both drafts the SDKs speak: draft-ietf-moq-transport-22 §9.20.9 and its draft-20 §5.1.2 spelling, the draft-20-only normalization, and rejected values |
| `arguments.json` | Namespace and track-name arguments the SDKs accept or reject locally, per the Arguments table in `../behavior.md`; properties and payloads are not portable through JSON and stay as mirrored tables in each suite |
| `room-endpoint.json` | Room-rooted endpoint URLs `roomEndpoint` / `room_endpoint` builds, and the endpoints and rooms it refuses, per "Room-rooted endpoints" in `../behavior.md` |

Planned: the rest of control message framing and parameters, subgroup and fetch stream headers, object status
markers. The regression tests under `ts/packages/moq-core/src/tests` are the seed for those.

Media vectors use decimal strings for numeric inputs and hex for binary data; `json` is the exact
expected UTF-8 catalog text. Property type IDs are small JSON integers. Both SDKs consume every case.

The AVC codec-header vectors apply to both languages. Annex B construction/error and access-unit
conversion vectors are Python-only; TypeScript consumes supplied avcC and length-prefixed packets.
