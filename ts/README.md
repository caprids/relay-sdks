# @mux/relay

The Relay TypeScript SDK. `connectRelaySubscriber` subscribes to track objects and
`connectRelayPublisher` publishes them, through Relay, Mux's Media over QUIC (MoQ) relay service.
Payloads are bytes the SDK does not interpret; no player, media catalog, or Video.js is needed.

## Requirements

- Browser WebTransport in a secure context (HTTPS or localhost). This is the default transport.
- A plain Node script cannot connect with the default transport. Node 24+ runs the build tools and
  the example launcher. Other runtimes must pass a compatible `createTransport` factory.
- A Relay endpoint and a short-lived Relay JWT authorized for the namespace you use. The SDK
  consumes tokens; it does not issue them.

## Install

The package is unpublished and its name is provisional. Build it from a checkout of this repository
and serve it with its `@mux/moq-core` dependency, as described in
[Use the local packages in your application](GUIDE.md#use-the-local-packages-in-your-application).

## Publish and subscribe

```ts
import { connectRelayPublisher, connectRelaySubscriber, readText } from '@mux/relay';

const url = 'https://sjc.relay.mux.global';
const namespace = ['team', 'quickstart', 'unique-run']; // a namespace your token authorizes
const getToken = () => token; // a Relay JWT allowed to publish and subscribe there

const publisher = await connectRelayPublisher({ url, namespace, getToken });
const ticker = publisher.registerTrack({ track: 'ticker' });
await publisher.announced;

const subscriber = await connectRelaySubscriber({ url, getToken });
let received: (text: string) => void;
const arrived = new Promise<string>((resolve) => { received = resolve; });
const subscription = subscriber.subscribe(
  { namespace, track: 'ticker' },
  { onObject: (object) => { if (object.status === 'normal') received(readText(object)); } },
);
await subscription.subscribed; // a fresh namespace can briefly reject this with error 16; retry it

await ticker.writeText('hello from @mux/relay'); // waits for the subscriber's demand
console.log(await arrived);
subscriber.close();
await publisher.close();
```

The [guide's first round trip](GUIDE.md#first-round-trip) runs the complete version of this in a
browser, with credential handling, the subscribe retry, a deadline, and cleanup on failure.

## Documentation

- [TypeScript guide](GUIDE.md): the quickstart, subscription and publishing lifecycles, async
  consumption, and publishing media.
- API reference: run `mise run ts:docs` to build it from the source.
- [Errors and recovery](GUIDE.md#errors-and-recovery): stable `RelayError` codes and phases, and
  recovering without assuming delivery or safe replay.
- [Shared specification](../spec/README.md): the protocol pins, behavior contract, and test vectors
  every Relay SDK implements.
