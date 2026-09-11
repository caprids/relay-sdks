# Test vectors

Language-neutral encoding vectors. Every SDK's protocol layer loads these files in its test suite,
so an encoder or decoder change that breaks the wire format fails in every language at once.

Conventions:

- Bytes are lowercase hex strings with no separators.
- Integer values are decimal strings, so 64-bit values survive JSON in every language.
- Each file names its source. Prefer examples from the MOQT draft text; boundary cases derived from
  the encoding rules say so.

| File | Covers |
| --- | --- |
| `varint.json` | MOQT variable-length integers, draft-ietf-moq-transport-20 §1.4.1 |

Planned: control message framing and parameters, subgroup and fetch stream headers, object status
markers. The regression tests under `ts/packages/moq-core/src/tests` are the seed for those.
