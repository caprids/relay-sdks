# Relay SDK specification

The SDKs in this repository share no code. They share this directory, which is how they stay in
lockstep.

| File | Purpose | How it is enforced |
| --- | --- | --- |
| `protocol.json` | Machine-readable pins: the MOQT draft (`draft-ietf-moq-transport-20`, wire identifier `moqt-20`), default Relay endpoint, authentication convention, object size limit | Every SDK has a test asserting its constants equal these values |
| `behavior.md` | The observable semantics every SDK must exhibit, in language-neutral terms | Review, plus the live smoke test assertion list it defines |
| [publisher-recovery.md](publisher-recovery.md) | Observed publisher replacement limits and application recovery guidance | Relay limitations and application policy, not an automatic SDK guarantee |
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
text; derive boundary cases from the encoding rules and say so in the file.
