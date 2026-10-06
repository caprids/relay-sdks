/**
 * The live-relay scenario, runtime-neutral: it uses only APIs that browsers and Node both provide, and
 * gets everything runtime-specific from its launcher. `scripts/test-relay.mjs` is the Chromium launcher
 * and loads this module in the page; a Node launcher passes its own `createTransport`.
 *
 * It resolves the result object on success and rejects with the first failure. Reporting the outcome,
 * redacting the token and choosing the exit code are the launcher's business.
 */
import {
  connectRelayPublisher,
  connectRelaySubscriber,
  connectSupervisedPublisher,
  connectSupervisedSubscriber,
} from '@mux/relay';

/**
 * @param {object} options
 * @param {string} options.endpoint The relay's `https://` URL, without a token.
 * @param {string} [options.subEndpoint] The subscribers' relay, for a cross-relay run; defaults to `endpoint`.
 * @param {string | undefined} options.token The Relay token, or undefined to connect anonymously.
 * @param {string[]} options.namespace The fresh namespace the scenario publishes under.
 * @param {((url: string, protocols: string[]) => unknown) | undefined} options.createTransport The
 *   transport factory both connections use, or undefined for the SDK's default.
 * @param {(value: object) => unknown} options.report Receives each phase as it passes; may return a promise.
 */
export async function runRelayScenario({
  endpoint,
  subEndpoint = endpoint,
  token,
  namespace,
  createTransport,
  report,
}) {
  let publisher;
  let subscriber;
  let watcher;
  const supervisedPublishers = [];
  let lateSubscriber;
  let layerPublisher;
  let layerSwitch;
  let lateLargestObject;
  let discovery;
  const discovered = [];
  const announcedNamespaces = new Set();
  const lifetime = new AbortController();
  const deadline = setTimeout(() => lifetime.abort(new Error('Live relay test exceeded 45 seconds')), 45_000);
  const received = { events: [], status: [], lateStatus: [] };
  const errors = [];
  const expectedEvents = [[0, 255, 128, 1], Array.from(new TextEncoder().encode('hello from Relay SDK')), []];
  let closing = false;
  const closedOnPurpose = new Set();
  const observeClosed = (label, connection) => {
    void connection.closed.then(({ error }) => {
      if (!closing && !closedOnPurpose.has(label)) errors.push(`${label} closed: ${String(error ?? 'peer closure')}`);
    });
  };
  const waitFor = async (predicate, label) => {
    const until = performance.now() + 10_000;
    while (!predicate()) {
      if (errors.length > 0) throw new Error(errors.join('; '));
      lifetime.signal.throwIfAborted();
      if (performance.now() >= until) throw new Error(`Timed out waiting for ${label}`);
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
  };
  const waitForCount = (track, count) => waitFor(() => received[track].length >= count, `${track}: ${count} objects`);
  const warmUp = new TextEncoder().encode('relay-sdk-smoke warm-up');
  const isWarmUp = (payload) => payload.length === warmUp.length && payload.every((byte, i) => byte === warmUp[i]);
  const warmed = new Set();
  // SUBSCRIBE_OK says the subscriber's relay took the request, not that the path to the publisher is
  // ready. A relay on another hop first subscribes only to learn the track's info and cancels that within
  // about 50 ms (spec/relay-interop.md), so the writes it takes are lost. Repeat a marker the handlers
  // drop until one arrives; the assertion payloads are then written once.
  const warmUpTrack = async (track, writer) => {
    const until = performance.now() + 10_000;
    let writes = 0;
    while (!warmed.has(track)) {
      if (errors.length > 0) throw new Error(errors.join('; '));
      if (performance.now() >= until) throw new Error(`Timed out waiting for ${track}: warm-up object`);
      try {
        await writer.write(warmUp);
      } catch (error) {
        if (error?.code !== 'SUBSCRIPTION_CHANGED') throw error;
      }
      writes += 1;
      const sent = performance.now();
      while (!warmed.has(track) && performance.now() - sent < 250) {
        lifetime.signal.throwIfAborted();
        await new Promise((resolve) => setTimeout(resolve, 20));
      }
    }
    return writes;
  };
  try {
    publisher = await connectRelayPublisher({
      url: endpoint,
      createTransport,
      namespace,
      getToken: () => token,
      signal: lifetime.signal,
    });
    observeClosed('publisher', publisher);
    await report({ phase: 'publisher connected', namespace });
    const events = publisher.registerTrack({ track: 'events', writeTimeoutMs: 20_000 });
    const status = publisher.registerTrack({ track: 'status', writeTimeoutMs: 20_000 });
    subscriber = await connectRelaySubscriber({
      url: subEndpoint,
      getToken: () => token,
      signal: lifetime.signal,
      createTransport,
    });
    observeClosed('subscriber', subscriber);
    await report({ phase: 'subscriber connected' });
    // Discovery: the subscriber watches the parent prefix, where other runs may announce too, so
    // only this run's namespace counts.
    const prefix = namespace.length > 1 ? namespace.slice(0, -1) : namespace;
    const ours = JSON.stringify(namespace);
    const discoveryStarted = performance.now();
    discovery = subscriber.namespaceEvents({ prefix, signal: lifetime.signal });
    void (async () => {
      try {
        for await (const event of discovery) {
          if (event.type === 'announced') announcedNamespaces.add(JSON.stringify(event.namespace));
          if (JSON.stringify(event.namespace) === ours) {
            discovered.push({ type: event.type, ms: Math.round(performance.now() - discoveryStarted) });
          }
        }
        if (!closing) errors.push('discovery ended early');
      } catch (error) {
        if (!closing) errors.push(`discovery: ${String(error)}`);
      }
    })();
    // Assertion 2. The announce left with the first registered track; the relay routes subscribes to
    // the namespace about a round trip later. The supervised subscriber's wants retry the rejection it
    // may still answer, and report only what they do not retry.
    await publisher.announced;
    await report({ phase: 'announced' });
    await waitFor(() => discovered.some((event) => event.type === 'announced'), 'discovery: announced');
    await report({ phase: 'discovered', afterMs: discovered[0].ms });
    watcher = await connectSupervisedSubscriber({
      url: subEndpoint,
      getToken: () => token,
      signal: lifetime.signal,
      createTransport,
      onStatus: (status, error) => {
        if (status === 'reconnecting' && !closing) errors.push(`supervised subscriber reconnecting: ${String(error)}`);
      },
    });
    await report({ phase: 'supervised subscriber connected' });
    const subscribed = new Set();
    for (const track of ['events', 'status']) {
      watcher.want(
        track,
        { namespace, track },
        {
          onSubscribed: () => {
            subscribed.add(track);
            void report({ phase: 'subscribed', track });
          },
          onObject: (object) => {
            if (object.status !== 'normal') return;
            if (isWarmUp(object.payload)) warmed.add(track);
            else received[track].push(Array.from(object.payload));
          },
          onError: (error) => errors.push(`${track}: ${String(error)}`),
          onSubgroupReset: ({ error }) => errors.push(`${track} reset: ${String(error)}`),
          onRestarted: ({ reason }) => {
            if (!closing) errors.push(`${track} resubscribed unexpectedly: ${reason}`);
          },
        },
      );
    }
    await waitFor(() => subscribed.size === 2, 'both wants subscribed');
    const warmUpWrites = { events: await warmUpTrack('events', events), status: await warmUpTrack('status', status) };
    await report({ phase: 'warmed up', writes: warmUpWrites });
    for (const payload of expectedEvents) await events.write(new Uint8Array(payload));
    await status.write(new Uint8Array([42]));
    await waitForCount('events', 3);
    await waitForCount('status', 1);
    await report({
      phase: 'received initial payloads',
      events: received.events.length,
      status: received.status.length,
    });
    // A fresh subscriber must decode the relay's SUBSCRIBE_OK for a track with content.
    lateSubscriber = await connectRelaySubscriber({
      url: subEndpoint,
      getToken: () => token,
      signal: lifetime.signal,
      createTransport,
    });
    observeClosed('late subscriber', lateSubscriber);
    // Start at the next group, the next live object. Not Next Object: moq-relay 0.17.0 frames LOCATION_FILTER the
    // draft-20 way on moqt-22 too, and closes the session on draft-22's Next Object spelling (spec/relay-interop.md,
    // "Draft-22 LOCATION_FILTER framing"). A relative start of 0 is the same bytes in both drafts.
    lateSubscriber.subscribe(
      { namespace, track: 'status', parameters: { locationFilter: { type: 'relative-group', groupsBeforeNext: 0 } } },
      {
        onSubscribed: (ok) => {
          lateLargestObject = ok.parameters.largestObject;
          if (!lateLargestObject) errors.push('late SUBSCRIBE_OK omitted LARGEST_OBJECT');
        },
        onObject: (object) => {
          if (object.status === 'normal' && !isWarmUp(object.payload))
            received.lateStatus.push(Array.from(object.payload));
        },
        onError: (error) => errors.push(`late status: ${error.errorCode} ${error.reason}`),
      },
    );
    await waitFor(() => lateLargestObject !== undefined, 'late SUBSCRIBE_OK with LARGEST_OBJECT');
    // The track's end would make its want resubscribe and replay the cached group (spec/publisher-recovery.md).
    watcher.unwant('events');
    events.close();
    await status.write(new Uint8Array([43]));
    await waitForCount('status', 2);
    await waitForCount('lateStatus', 1);
    if (JSON.stringify(received.lateStatus) !== '[[43]]') throw new Error('Late subscriber payload mismatch');
    const samePayloads = (actual, expected) =>
      JSON.stringify(actual.map(JSON.stringify).sort()) === JSON.stringify(expected.map(JSON.stringify).sort());
    if (!samePayloads(received.events, expectedEvents) || !samePayloads(received.status, [[42], [43]])) {
      throw new Error(`Payload mismatch: ${JSON.stringify(received)}`);
    }
    if (errors.length > 0) throw new Error(errors.join('; '));
    // A clean publisher close withdraws the announce; the subscriber's discovery hears it. The want on
    // its last track goes first, so the publisher's done does not start a resubscription.
    watcher.unwant('status');
    const closedAt = performance.now();
    closedOnPurpose.add('publisher');
    await publisher.close();
    await waitFor(() => discovered.some((event) => event.type === 'unannounced'), 'discovery: unannounced');
    const withdrawnAfterMs = Math.round(performance.now() - closedAt);
    await report({ phase: 'undiscovered', afterMs: withdrawnAfterMs });
    if (JSON.stringify(discovered.map((event) => event.type)) !== '["announced","unannounced"]') {
      throw new Error(`Discovery mismatch: ${JSON.stringify(discovered)}`);
    }

    // Assertion 8. A want following a supervised publisher's root moves to a newer generation that
    // announces while the old one is still up, the way a restarted publisher replaces a crashed one
    // whose announce the relay still holds.
    const root = [...namespace, 'supervised'];
    const beats = [];
    const restarts = [];
    let currentNamespace;
    watcher.want(
      'beat',
      { namespace: root, track: 'beat', generations: true },
      {
        onSubscribed: (_, context) => {
          currentNamespace = context.namespace;
        },
        onObject: (object) => {
          if (object.status === 'normal') beats.push(new TextDecoder().decode(object.payload));
        },
        onRestarted: ({ reason }) => restarts.push(reason),
        onError: (error) => errors.push(`beat: ${String(error)}`),
      },
    );
    const startGeneration = async (label) => {
      const supervised = await connectSupervisedPublisher({
        url: endpoint,
        createTransport,
        namespace: root,
        getToken: () => token,
        signal: lifetime.signal,
      });
      supervisedPublishers.push(supervised);
      return { supervised, label, writer: supervised.registerTrack({ track: 'beat', writeTimeoutMs: 2_000 }) };
    };
    // Write until this generation's beats arrive: a SUBSCRIBE_OK does not prove the path is ready.
    const beatUntilReceived = async ({ writer, label }) => {
      const until = performance.now() + 15_000;
      for (let n = 0; !beats.some((beat) => beat.startsWith(`${label}:`)); n += 1) {
        if (errors.length > 0) throw new Error(errors.join('; '));
        if (performance.now() >= until) throw new Error(`Timed out waiting for generation ${label}'s beats`);
        try {
          await writer.write(new TextEncoder().encode(`${label}:${n}`));
        } catch (error) {
          if (!['SUBSCRIPTION_CHANGED', 'WRITE_TIMEOUT'].includes(error?.code)) throw error;
        }
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
    };
    const first = await startGeneration('first');
    await report({ phase: 'first generation', namespace: first.supervised.generation.namespace });
    await beatUntilReceived(first);
    const second = await startGeneration('second');
    const switchStarted = performance.now();
    await beatUntilReceived(second);
    const switchedAfterMs = Math.round(performance.now() - switchStarted);
    if (JSON.stringify(currentNamespace) !== JSON.stringify(second.supervised.generation.namespace)) {
      throw new Error(`Want stayed on ${JSON.stringify(currentNamespace)}`);
    }
    if (!restarts.includes('generation')) throw new Error(`No generation restart: ${JSON.stringify(restarts)}`);
    await report({ phase: 'followed newer generation', afterMs: switchedAfterMs, restarts });
    watcher.unwant('beat');
    if (errors.length > 0) throw new Error(errors.join('; '));

    // Assertion 9. A switchable subscription moves from one layer to another make-before-break: the
    // layers carry the same frame numbers, one group per frame, and the frames the handlers receive
    // leave no gap across the cutover (issue #264). The frame number stands in for a media timestamp:
    // the switch waits for a new-layer frame within a few of the latest the old layer delivered, as a
    // player compares timestamps. The first object can be stale: the relay caches the layer's latest
    // group, and the publisher skips a layer nobody watches. A subscriber on another relay can also
    // lose the first object (issue #243). The drain delivers older frames still in flight on the old layer: across relays
    // groups arrive out of order, and cancelling at the cutover can cut one off.
    layerPublisher = await connectRelayPublisher({
      url: endpoint,
      createTransport,
      namespace: [...namespace, 'layers'],
      getToken: () => token,
      signal: lifetime.signal,
    });
    observeClosed('layer publisher', layerPublisher);
    const layers = ['video-lo', 'video-hi'].map((track) =>
      layerPublisher.registerTrack({ track, writeTimeoutMs: 2_000 }),
    );
    // Subscribe once discovery reports the announce, so the relay cannot refuse it as unroutable.
    await layerPublisher.announced;
    await waitFor(
      () => announcedNamespaces.has(JSON.stringify([...namespace, 'layers'])),
      'discovery: layers announced',
    );
    const layerErrors = [];
    const lostWrites = [];
    let frameNumber = 0;
    let writingLayers = true;
    const layerWrites = (async () => {
      while (writingLayers) {
        const payload = new TextEncoder().encode(String(frameNumber++));
        await Promise.all(
          layers
            .filter((layer) => layer.hasDemand)
            .map((layer) =>
              layer.write(payload).catch((error) => {
                if (!['SUBSCRIPTION_CHANGED', 'WRITE_TIMEOUT'].includes(error?.code)) throw error;
                lostWrites.push([
                  layer === layers[0] ? 'video-lo' : 'video-hi',
                  Number(new TextDecoder().decode(payload)),
                  error.code,
                ]);
              }),
            ),
        );
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
    })().catch((error) => {
      layerErrors.push(`layer write: ${String(error)}`);
    });
    const frames = [];
    const frameOf = (object) => Number(new TextDecoder().decode(object.payload));
    let switches = 0;
    const switchable = subscriber.subscribeSwitchable(
      { namespace: [...namespace, 'layers'], track: 'video-lo' },
      {
        onObject: (object, track) => {
          if (object.status === 'normal') frames.push([track, frameOf(object)]);
        },
        onSwitched: () => {
          switches += 1;
        },
        onError: (error, track) => layerErrors.push(`${track}: ${error.errorCode} ${error.reason}`),
        onSubgroupReset: ({ groupId }, track) => layerErrors.push(`${track} reset group ${groupId}`),
      },
    );
    try {
      await switchable.subscribed;
      await waitFor(() => frames.length >= 10, 'video-lo: 10 frames');
      const switchStartedAt = performance.now();
      const switched = await switchable.switchTo('video-hi', {
        until: (object) => {
          const latest = Math.max(...frames.map(([, n]) => n));
          return frameOf(object) <= latest + 1 && frameOf(object) >= latest - 5;
        },
        drainMs: 1_000,
      });
      const cutoverMs = Math.round(performance.now() - switchStartedAt);
      const atCutover = frames.length;
      await waitFor(() => frames.length >= atCutover + 10, 'video-hi: 10 frames');
      writingLayers = false;
      await layerWrites;
      if (layerErrors.length > 0) throw new Error(layerErrors.join('; '));
      const numbers = [...new Set(frames.map(([, n]) => n))].sort((a, b) => a - b);
      const missing = [];
      for (let n = numbers[0]; n <= numbers.at(-1); n += 1) if (!numbers.includes(n)) missing.push(n);
      const tracks = [...new Set(frames.map(([track]) => track))];
      if (missing.length > 0 || switches !== 1 || JSON.stringify(tracks) !== '["video-lo","video-hi"]') {
        throw new Error(
          `Layer switch left a gap: missing ${JSON.stringify(missing)}, ${switches} switches, lost writes ${JSON.stringify(lostWrites)}, ${JSON.stringify(frames)}`,
        );
      }
      const firstHi = frames.findIndex(([track]) => track === 'video-hi');
      layerSwitch = {
        cutoverMs,
        from: switched.from,
        to: switched.to,
        lastOld: frames[firstHi - 1][1],
        firstNew: frames[firstHi][1],
        frames: frames.length,
      };
      await report({ phase: 'switched layers', ...layerSwitch });
    } finally {
      writingLayers = false;
      switchable.cancel();
    }

    return {
      endpoint,
      subscriberEndpoint: subEndpoint,
      namespace,
      events: received.events.length,
      status: received.status.length,
      warmUpWrites,
      binaryAndEmptyPayloads: 'passed',
      independentTrackClose: 'passed',
      lateSubscriberLargestObject: lateLargestObject,
      discovery: { announced: 'passed', withdrawnAfterMs },
      generationSwitch: { afterMs: switchedAfterMs, restarts },
      layerSwitch,
    };
  } finally {
    closing = true;
    clearTimeout(deadline);
    discovery?.close();
    watcher?.close();
    try {
      await Promise.all([
        publisher?.close(),
        layerPublisher?.close(),
        ...supervisedPublishers.map((supervised) => supervised.close()),
      ]);
    } finally {
      subscriber?.close();
      lateSubscriber?.close();
    }
  }
}
