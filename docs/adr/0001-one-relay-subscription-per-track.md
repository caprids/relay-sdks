---
status: accepted
date: 2026-09-21
---

# One relay subscription per track on a publisher connection

MOQT draft-ietf-moq-transport-20 lets the peer hold several concurrent subscriptions to the same
track (§5.1: "An endpoint MAY have multiple concurrent subscriptions to the same Track") and requires
the publisher to send every object once per subscription, on its own streams under its own alias.
The Relay SDKs model demand as one relay subscription per track: `hasDemand` is a boolean, a new
binding replaces the previous one, and an operation in flight when that happens fails with
`SUBSCRIPTION_CHANGED` instead of being sent twice. We chose this because the tested relay aggregates
every downstream viewer into a single upstream subscription (§9.4 "Relays MAY aggregate"; observed in
[issue #24](https://github.com/muxinc/relay-sdks/issues/24)) and keeps that subscription for about
30 seconds after the last viewer leaves, so per-subscription fan-out on the publisher side would
add interface and state for a case the relay never produces.

## Consequences

- The write slot (see `CONTEXT.md`) tracks one alias. A second live subscription from the relay is
  indistinguishable from a replacement.
- Fan-out to several subscriptions, if a relay ever needs it, belongs in the generic core's publish
  session, not in the product write slot.
- Revisit if a relay is observed holding two live subscriptions to one track on one publisher
  connection, or if `demandChanges()` consumers need to distinguish "replaced" from "added".
