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
