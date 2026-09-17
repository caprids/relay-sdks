# moq-relay interoperability

Both SDKs speak only `moqt-20`. Their `LARGEST_OBJECT` message parameter is two bare
varints (Group, Object), matching moq-relay **0.14.17** and draft-20 §10.2.
[Upstream fix](https://github.com/moq-dev/moq/pull/3561).

Older moq-relay releases used a length prefix for this parameter. That encoding cannot
be safely auto-detected: coordinate adoption of these SDK builds with the relay upgrade.
Old SDK builds fail on subscriptions to populated tracks after the relay upgrade; these
SDK builds misdecode or reject the old relay's populated-track responses. `INCLUDE_PROPERTIES`
remains length-prefixed.

Publishers still retain no history and advertise no `LARGEST_OBJECT`. The relay's new
Next Object + current-group `FILL_PARAMETERS` pull therefore has no cached publisher
content to fill; the publisher serves subsequent writes. Adding publisher retention and
backfill is a separate feature. The test fixtures now send the 0.14.17 pull parameters.

## Publisher restarts and resubscription

Against moq-relay 0.14.17, re-subscribing to the same namespace and track after
`on_done` / `onDone` can replay cached objects and immediately receive another
`PUBLISH_DONE`. An application that re-subscribes from every done callback can
therefore generate a request loop while the publisher is absent. See
[issue #21](https://github.com/muxinc/relay-sdks/issues/21) for reproduction evidence
and relay follow-up.

Each `subscribe()` creates an independent request, including its starting location.
It does not resume the preceding subscription. The SDK does not retry automatically
or suppress objects received by new subscriptions.

Applications that follow a restarting publisher should apply these policies:

- Keep at most one pending retry per logical track. Schedule retries for done
  notifications as well as retryable request errors; a delay used only for 404
  responses does not bound an accepted-subscription/done loop. Cancel pending retries
  when the application stops, changes source, or closes the connection.
- Use capped exponential backoff with jitter and an application retry budget. For
  example, start around 500 ms and grow to a 5 s cap. Do not reset backoff solely on
  `SUBSCRIBE_OK` or a cached object: neither proves that a new publisher is live.
  Reset only on application-defined progress, such as advancing sequence numbers
  from the expected publisher generation. Backoff limits request load; it does not
  eliminate replay or guarantee recovery.
- Treat done as a control notification, not a data-drained signal. Trailing objects
  may arrive afterward. Define how long to drain a retired subscription before
  cancelling it, or explicitly accept losing trailing objects when cancelling
  immediately. Bound the number and lifetime of retired subscriptions.
- If continuity or duplicate suppression matters, put a publisher generation and
  sequence number in the payload and define the application's replay policy.
  Group/object IDs alone are insufficient across restarts because a new publisher
  can restart its counters. Do not discard all lower group IDs as stale.
- Choose subscription location filters to match the application's history needs.
  A new subscription's filter is not an exactly-once resume token across publisher
  generations, and opening another connection is not a cache invalidation guarantee.

Record done status and reason for diagnosis. Both `track ended` (2) and `internal
error` (0) have been observed around publisher shutdown; neither identifies a new
publisher generation. Preserve actual errors rather than interpreting all status-0
responses as harmless shutdowns.

## Reproduce against a local relay

Requires Docker, OpenSSL, the repository's installed dependencies, and Playwright Chromium
(`cd ts && pnpm exec playwright install chromium`). Run from the repository root with a
local Docker daemon. These commands create an anonymous test relay on loopback and a
short-lived ECDSA certificate trusted explicitly by the test clients.

```sh
interop_dir=$(mktemp -d)
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -keyout "$interop_dir/key.pem" -out "$interop_dir/cert.pem" -days 7 \
  -subj /CN=localhost -addext subjectAltName=DNS:localhost

docker run -d --name relay-sdk-interop \
  -p 127.0.0.1:4443:4443/udp -p 127.0.0.1:4443:4443/tcp \
  -v "$interop_dir:/tls:ro" moqdev/moq-relay:0.14.17 \
  --server-bind '[::]:4443' --tls-cert /tls/cert.pem --tls-key /tls/key.pem \
  --web-http-listen '[::]:4443' --auth-public ''

export RELAY_ANONYMOUS=1 RELAY_URL=https://localhost:4443
unset RELAY_TOKEN
export RELAY_CA_FILE="$interop_dir/cert.pem"
export RELAY_CERTIFICATE_HASH=$(openssl x509 -in "$interop_dir/cert.pem" -outform DER | openssl dgst -sha256 -r | cut -d ' ' -f 1)
mise run python:test:relay
mise run ts:test:relay

docker rm -f relay-sdk-interop
rm -r "$interop_dir"
unset RELAY_ANONYMOUS RELAY_URL RELAY_CA_FILE RELAY_CERTIFICATE_HASH
```

Both tests publish binary, text, and empty payloads, then connect a second subscriber
**after** the relay has received content. They require `SUBSCRIBE_OK` to contain
`LARGEST_OBJECT` and verify that the late subscriber receives the next live object.
This exercises the relay's encoder independently of the SDK codecs and in-process hubs.
Successful output includes `lateSubscriberLargestObject`.

`RELAY_ANONYMOUS=1` skips the token prompt; if `RELAY_TOKEN` is set, it is still used.
`RELAY_CA_FILE` adds the local certificate as a Python trust root.
`RELAY_CERTIFICATE_HASH` pins its SHA-256 DER hash for browser WebTransport. These are
smoke-test options; normal SDK TLS verification remains unchanged.
