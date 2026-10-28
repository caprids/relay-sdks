# Relay SDK specification

The SDKs in this repository share no code. They share this directory, which is how they stay in
lockstep.

| File | Purpose | How it is enforced |
| --- | --- | --- |
| `protocol.json` | Machine-readable pins: the MOQT draft (`draft-ietf-moq-transport-22`, wire identifier `moqt-22`, with `moqt-20` as the offered fallback), the default Relay endpoint, the authentication convention, the size limits, the relay's property limits, the LOC and MSF drafts, and the media defaults | Every SDK has a test asserting its constants equal these values |
| `behavior.md` | The observable semantics every SDK must exhibit, in language-neutral terms | Review, plus the live smoke test assertion list it defines |
| [vocabulary.md](vocabulary.md) | Public product and media format names across languages, with the rationale for each | Export-surface tests, paired media tests, and API review |
| [transport-adapter.md](transport-adapter.md) | The transport adapter contract: what every adapter the SDKs run over (browser WebTransport, native QUIC in Node and on aioquic, QMux over WebSocket, test pairs) must do, as numbered `TA-n` rules under the MOQT and QUIC specifications | The transport adapter conformance suite (#193), whose tests are named after the rules |
| [publisher-recovery.md](publisher-recovery.md) | Observed publisher replacement limits and application recovery guidance | Relay limitations. The opt-in supervised connections implement the recovery policy, and the connection primitives do not |
| [relay-interop.md](relay-interop.md) | moq-relay's wire encodings, deviations from the draft, and measured behavior, by relay version, with a local reproduction | Measurements and the probe tasks in `mise.toml`, not a contract |
| [media-layer.md](media-layer.md) | The LOC packaging, MSF catalog, source URL, and decoder-configuration work the SDKs take over from media publishers, measured against the Python Video.js example | Stage 1 implemented. Of stages 2 and 3, the catalog, document, and frame-writer services, the catalog reader, and the frame reader ship. The `register_media` handle remains proposed |
| `vectors/` | Encoding test vectors every protocol layer must pass | Each SDK's test suite loads the JSON and checks its encoder and decoder |

## Changing a pin

Edit `protocol.json` first. Every SDK's pin test then fails until that SDK is updated, so a draft
bump or an endpoint change lands as one commit touching the spec and every language together, or
not at all. Never change the constant in one SDK without changing the spec.

## Adding behavior

Add or amend the statement in `behavior.md` before or alongside the first SDK that implements it,
and mark it as not yet implemented in the others. If the behavior is observable against a relay, add
it to the smoke test assertion list so every SDK's live test checks it.

## Adding vectors

Vector files are plain JSON with hex-encoded bytes and decimal-string values, so any language can
load them without precision loss. See `vectors/README.md`. Prefer vectors taken from the MOQT draft
text. Derive boundary cases from the encoding rules and say so in the file.
