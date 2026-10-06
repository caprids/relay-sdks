# MOQT draft-20 → draft-22

What changed between [draft-ietf-moq-transport-20] and [draft-ietf-moq-transport-22] (1 October 2026,
the latest revision on 2026-10-06), what that means for the SDKs, and what the relays speak.
Researched for [#274](https://github.com/muxinc/relay-sdks/issues/274). The draft text is the
authority. Section numbers are draft-22's unless they say "-20".

## Method

The three drafts' text versions (`https://www.ietf.org/archive/id/draft-ietf-moq-transport-{20,21,22}.txt`)
were compared four ways:

1. Every wire figure (`Name { … }`), extracted and diffed.
2. Every IANA table row (message types, setup options, parameters, properties, error codes,
   stream types, object status).
3. Every sentence containing MUST, SHOULD, MAY, REQUIRED, RECOMMENDED or PROTOCOL_VIOLATION,
   paired by similarity and word-diffed.
4. The change log, draft-22 Appendix A.1 (since -21) and A.2 (since -20).

The in-progress relay port to -22 (`relay-rs` worktrees `moqt-22-*`, `docs/moqt-22-delta.md`)
reaches the same wire conclusion.

## Wire changes

| Change | -20 | -22 | SDK impact |
| --- | --- | --- | --- |
| LOCATION_FILTER parameter (`0x21`) value (§9.20.9; #1913, #1953) | length-prefixed; 0 to 4 fields, the count selects the meaning | a Location Filter Type, then the fields it selects; no length | the one codec change, below |
| ALPN / WebTransport protocol identifier (§6.2) | `moqt-20` | `moqt-22` | offer both |

Nothing else changed on the wire. Draft-21 is editorial only, so it is wire-identical to -20.
The rest of the wire is unchanged from -20, including:

- message type codes (Table 5);
- parameter, setup option and property codepoints;
- session termination, REQUEST_ERROR, PUBLISH_DONE and stream reset codes;
- subgroup and datagram Type Flags, fetch serialization flags and End of Range markers;
- the varint, Location, Key-Value-Pair and Track Namespace structures;
- request IDs and the request-stream model, GOAWAY, PUBLISH_DONE and its Stream Count sentinel.

### LOCATION_FILTER

```
-20: LOCATION_FILTER Parameter { Type = 0x21, Length (vi64), [StartGroup], [StartObject], [EndGroupDelta], [EndObject] }
-22: LOCATION_FILTER Parameter { Type = 0x21, Location Filter Type (vi64), [StartGroup], [StartObject], [EndGroupDelta], [EndObject] }
```

| Meaning | -22 type and fields | -20 fields |
| --- | --- | --- |
| no filter | `0x00` | none (length 0) |
| relative start `{Largest.Group + 1 − StartGroup, 0}`, open-ended | `0x01` StartGroup | StartGroup |
| absolute start, open-ended | `0x02` StartGroup StartObject | StartGroup StartObject, unless both are 0 |
| absolute start, ends with group StartGroup + EndGroupDelta | `0x03` + EndGroupDelta | three fields |
| absolute range, ends at `{StartGroup + EndGroupDelta, EndObject}` | `0x04` + EndObject | four fields |
| Next Object `{Largest.Group, Largest.Object + 1}` | `0x05` | StartGroup = StartObject = 0 |

- A -22 type outside `0x00` to `0x05` is a PROTOCOL_VIOLATION.
- -20 had no spelling for an open-ended absolute start at `{0,0}`. It defined that as equivalent to
  no filter, because `{0,0}` with two fields meant Next Object. -22 can say it: `0x02 0 0`.
- When the field count equals the type and every field is a one-byte varint (under 128), the two
  encodings are the same bytes. They differ for Next Object, for an open-ended absolute `{0,0}`,
  and for any field of 128 or more (`spec/vectors/location-filter.json` has both spellings of each
  shape).
- §3.4 still says a LOCATION_FILTER inside FILL_PARAMETERS can be "zero-length". Under -22 that
  has to mean type `0x00`.

## Editorial and semantic changes

None of these change bytes on the wire.

- **Restructure**: the document was reorganized, so almost every section moved; see the citation
  map below. Error codes now sit together in §12 (Error Handling). The Connection URL, Stream
  Cancellation and Examples sections were removed.
- **Per-request names** (§1.5): REQUEST_OK and REQUEST_ERROR get shorthand names per request,
  such as SUBSCRIBE_ERROR, PUBLISH_NAMESPACE_OK and REQUEST_UPDATE_OK. They are the same messages
  and codes. The SDKs keep the wire names; `spec/behavior.md` quotes §9.5 with the new names.
- **Paused**: "Forward State" is now a subscription being *paused* (§3.1.1, FORWARD=0). The
  parameter is unchanged.
- **Delivery Mode** replaces "Object Forwarding Preference" (subgroup or datagram).
- **Allowed parameters per message** (#1916): each control message now lists the parameters it
  allows. The rule itself is from -20 (§9.20.1): a parameter in a message type that does not allow
  it is a PROTOCOL_VIOLATION. See the conformance check below.
- **FETCH_OK End Location is inclusive** (#1910).
- **Auth token cache overflow** (AUTH_TOKEN_CACHE_OVERFLOW) applies only to registrations outside
  SETUP (#1927).
- **Extension support** (§6.3.2, #1921): each extension defines how SETUP declares support.
  SOLICIT, the one extension the SDKs send, already works this way.
- **Fetch semantics** get their own section (§3.2), covering gaps (including descending order) and
  relay handling. A relay meeting unknown-status objects in a fetch range issues upstream FETCHes
  for them. -20's sentence that it "MUST pause subsequent delivery" until the status is confirmed is
  gone. Relay-side only.
- **Scope**: relay-to-relay communication is out of scope.
- **Parsing serialized names** (§8.8): the canonical-form rules are restated as one MUST, with the
  same meaning.

## Conformance check of the cores

Each SDK core was read against the -22 wire figures, the parameter definitions and the normative
sentences above.

- **Fixed: LOCATION_FILTER.** Both cores encoded and decoded only the -20 form. They now take the
  negotiated protocol: `encodeLocationFilter` / `decodeLocationFilter`, the parameter codec and
  every message encoder or decoder that carries parameters take a trailing `protocol` argument,
  defaulting to `moqt-22`. Each session reads it from the transport (`transport.protocol` in
  TypeScript, the adapter's `protocol_id` in Python). An unknown -22 type is a PROTOCOL_VIOLATION,
  and so is a value with bytes left over. A REQUEST_UPDATE that `update()` queues before acceptance
  is pre-encoded for the connection's protocol.
- **Fixed: version negotiation.** Both SDKs offer `["moqt-22", "moqt-20"]` and accept either.
  `spec/protocol.json` pins `protocolId` `moqt-22` and `protocolIds` with both.
- **Fixed: citations.** Every draft-20 section citation in the cores, SDKs, tests and `spec/` now
  uses -22 numbering. Two quotes from the draft changed wording, and `spec/behavior.md` follows
  them.
- **Unchanged, conformant**: everything else on the wire, per the table above. On the sending
  side, every message the SDKs send by default carries only parameters -22 allows there.
  SUBSCRIBE_OK, PUBLISH_OK and REQUEST_UPDATE_OK carry none. FETCH, TRACK_STATUS, SUBSCRIBE and
  REQUEST_UPDATE carry what the application passes.
- **Open (from -20, not new in -22)**: on receipt, neither core checks a message's parameters
  against the per-message list. Both decode any known parameter in any message, except inside
  FILL_PARAMETERS, which they check. -20 had the same MUST, and -22 makes the lists explicit.
  Enforcing it would close sessions the relay keeps today, so it is tracked separately rather than
  folded into this move.

## What the relays speak

- **Production** (mux.global, moq-relay 0.17.0), probed from sjc and fra on 2026-10-06:
  - Over native QUIC it selects `moqt-22`, `moqt-21`, `moqt-20` or `moqt-19` when offered alone,
    and refuses `moqt-23` and `moqt-99`.
  - Offered `[moqt-22, moqt-20]`, it selects `moqt-22`. Chromium's WebTransport gets the same
    answer through `WT-Protocol`.
  - moq-relay has negotiated `moqt-22` since 0.15.0 (upstream `5b573fdcf`, moq-dev/moq#3858), so
    every relay version the SDKs support speaks it.
- **Local**: the `moqdev/moq-relay:0.17.0` image from the `spec/relay-interop.md` recipe answers
  the same way, and the Python live smoke test passes against it.
- **The relay's -22 LOCATION_FILTER is the -20 one.** moq-net treats every draft from 20 on as
  "draft-20" for this parameter (`Filter::is_draft20` and `impl Param for Filter` in
  `rs/moq-net/src/ietf/filter.rs`, same on `main`). Measured live on production on `moqt-22`:
  - a filter whose two spellings coincide is accepted: none, type `0x00`, relative-group 1, or an
    absolute start;
  - a draft-22 Next Object (`21 05`) closes the session with PROTOCOL_VIOLATION (0x3)
    `short buffer`;
  - the relay's own upstream SUBSCRIBE sends `21 02 00 00` and `21 01 01`, its -20 Next Object and
    relative fill. Under -22 these decode as an absolute `{0,0}` start and a relative start of 1.
    No SDK publisher acts on filters, so that is harmless.

  Details are in `spec/relay-interop.md` ("Draft-22 LOCATION_FILTER framing") and
  [#275](https://github.com/muxinc/relay-sdks/issues/275).

## Decision

The SDKs speak -22 strictly and default to it: they offer `moqt-22` first and `moqt-20` as a
fallback, and keep -20 support whole. Against today's moq-relay this works for every filter whose
spellings coincide, which includes no filter, the SDK default. A Next Object filter breaks it, so
the live smoke tests' late subscriber now asks for a relative start of 0 instead. With
`moqt-20` forced, the production smoke test still passes, Next Object included.

## Citation map

The draft-20 sections the repository cited and where they are in draft-22 (`3.5*` and `10.12*`
split: the error-code text moved to §12).

| -20 | -22 | title |
| --- | --- | --- |
| 1.4.1 / 1.4.2 / 1.4.3 | 8.1 / 8.2 / 8.3 | Variable-Length Integers / Location / Key-Value-Pair |
| 2.4.2 | 12.1 | Malformed Tracks |
| 3.1 / 3.1.3 / 3.1.4 / 3.1.5 | 6.2 / 6.1.2 / 6.2.1 / 6.2.2 | Session establishment / Dereferencing a MOQT URI / WebTransport / Native QUIC |
| 3.2 | 6.3.2 | Extension Negotiation |
| 3.3 | 6.3 (control streams), 6.4.2 (request streams) | Session initialization; Bidirectional Request Streams |
| 3.3.2 / 3.3.3 | 6.4.2.2 / 6.4.2.3 | Graceful Request Stream Closure / Request Cancellation and Rejection |
| 3.3.4 | 12.5 | Stream Reset Error Codes |
| 3.4 | 6.4.1 | Unidirectional Streams |
| 3.5* | 6.6 (termination), 12.2 (codes) | Termination; Session Termination Codes |
| 5.1 / 5.1.1 / 5.1.2 / 5.1.3 / 5.1.4 | 3.1 / 3.1.2 / 3.3.1 / 3.4 / 3.3.2 | Subscriptions / State Management / Location Filters / Fill Semantics / Range Filters |
| 8 | 5.2 | Delivery Timeouts and Data Reliability |
| 9.3 / 9.4 | 7.3 / 7.4 | Multiple Publishers / Subscriber Interactions |
| 10 | 9 | Control Messages |
| 10.1 | 6.4.2.1 | Request ID |
| 10.2, 10.2.n | 9.20, 9.20.n | Control Message Parameters (n unchanged: 9.20.15 FILL_PARAMETERS, 9.20.21 INCLUDE_PROPERTIES) |
| 10.3, 10.3.1.n | 9.1, 9.1.n | SETUP and its options |
| 10.4 / 10.5 / 10.6 | 9.2 / 9.3 / 9.4 | GOAWAY / REQUEST_OK / REQUEST_ERROR (codes: 12.3) |
| 10.7 / 10.8 / 10.9 / 10.9.1 | 9.6 / 9.7 / 9.5 / 9.5.1 | SUBSCRIBE / SUBSCRIBE_OK / REQUEST_UPDATE / Updating Subscriptions |
| 10.10 / 10.11 / 10.12* | 9.10 / 9.8 / 9.9 (status codes: 12.4) | PUBLISH_STATE_NOTIFY / PUBLISH / PUBLISH_DONE |
| 10.13 / 10.14 / 10.15 | 9.11 / 9.12 / 9.13 | FETCH / FETCH_OK / TRACK_STATUS |
| 10.16 / 10.17 / 10.18 / 10.19 | 9.14 / 9.16 / 9.17 / 9.15 | PUBLISH_NAMESPACE / NAMESPACE / NAMESPACE_DONE / SUBSCRIBE_NAMESPACE |
| 11.1 | 3.1.3 | Track Alias |
| 11.2.1.1 / 11.2.1.2 | 11.1.1 / 11.1.2 | Object Status / Object Properties |
| 11.4 (STOP_SENDING) / 11.4.2 / 11.4.3 | 11.3.2 / 11.3.1 / 11.3.2 | Subgroup Header / Closing Subgroup Streams |
| 11.4.4 / 11.4.4.1 / 11.4.4.2 | 11.4.1 / 11.4.1.1 / 11.4.1.2 | Fetch Header / Flags / End of Range |
| 12.1 / 12.2 / 12.4 / 12.5 | 10.1 / 10.2 / 10.4 / 10.5 | Properties: delivery timeouts, Default Publisher Priority and Group Order |
| 13.6 / 13.6.1 | 15.6 / 15.6.1 | Timeouts / Idle Connection Handling |
| 14 | 13 | Grease |
| 15.7 / 15.11.2 / 15.11.4 | 16.7 / 16.11.2 / 16.11.4 | IANA registries |
| Table 2 / Table 6 / Table 7 | Table 4 / Table 7 / Table 8 | Example Integer Encodings / FILL_PARAMETERS scope / End of Range flags |
| Appendix A.4 | Appendix A.6 | Since draft-16 |

[draft-ietf-moq-transport-20]: https://www.ietf.org/archive/id/draft-ietf-moq-transport-20.html
[draft-ietf-moq-transport-22]: https://www.ietf.org/archive/id/draft-ietf-moq-transport-22.html
