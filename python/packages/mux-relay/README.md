# mux-relay

The Relay Python SDK: `connect_relay` for subscribing, `connect_relay_publisher` for publishing, and
the WebTransport-over-HTTP/3 transport (aioquic) that reaches a relay natively with no browser.

The usage guide lives one level up in `python/README.md`; the protocol pins, behavior contract, and
test vectors shared with every other Relay SDK live in the repository's `spec/` directory.

For stable `RelayError.code` / `phase` values, preserved exception subclasses and transport causes,
and recovery without assuming delivery acknowledgment or safe replay, see
[Errors and recovery](../../README.md#errors-and-recovery).
