# mux-relay

The Relay Python SDK: `connect_relay_subscriber` for subscribing, `connect_relay_publisher` for publishing, and
the WebTransport-over-HTTP/3 transport (aioquic) that reaches a relay natively with no browser.

Start with the [Python quickstart](../../README.md#first-round-trip) for credentials, a local
installation, and a complete publish/subscribe run. Packages are unpublished; install both local
packages as described in the [local consumption guide](../../README.md#use-the-local-packages-in-your-application).

The [shared specification](../../../spec/README.md) contains the protocol pins, behavior contract,
and test vectors used by every Relay SDK.

For stable `RelayError.code` / `phase` values, preserved exception subclasses and transport causes,
and recovery without assuming delivery acknowledgment or safe replay, see
[Errors and recovery](../../README.md#errors-and-recovery).
