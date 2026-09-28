# Recovering after a publisher stops or restarts

Replacing a publisher connection restores an upstream source for new subscriptions. It does
**not** move existing subscribers to that source (moq-relay 0.15.8 never does), and a new
subscription need not succeed immediately.
These are relay lifecycle limits tracked in [#34](https://github.com/muxinc/relay-sdks/issues/34).
The SDKs do not automatically reconnect or resubscribe.

## What a subscriber can observe

| Signal | What it establishes |
| --- | --- |
| `onSubscribed` / `on_subscribed` | The relay accepted a subscription. It does not establish that the publisher is still producing live data. |
| `onObject` / `on_object` | An object arrived. It may be cached; arrival alone does not establish freshness. |
| `onPublisherDone` / `on_publisher_done` | The relay sent `PUBLISH_DONE`. Trailing object streams may still arrive until cancellation or connection closure. |
| `onSubgroupReset` / `on_subgroup_reset` | One subgroup was reset. It does not establish that the entire publisher has stopped. |
| Connection `closed` | This subscriber's transport/session ended. A publisher's separate connection can die while the subscriber connection remains open. |

`requestTimeoutMs` / `request_timeout` bounds only the initial subscription response. Once
`SUBSCRIBE_OK` arrives, this timer is disarmed. It is not a time-to-first-object deadline or an
inactivity detector. Transport keepalive likewise does not prove the publisher is alive.

Measured on mux.global's moq-relay 0.15.8 on 2026-09-28
([publisher restarts](relay-interop.md#publisher-restarts-and-resubscription)):

- A publisher that closes cleanly, or whose connection closes, ends existing subscriptions within
  0.1 s. The done is usually status 2 `track ended`, sometimes 0 `internal error`.
- After a publisher crash, existing subscriptions sit silent for about 30-35 s, the relay's QUIC
  idle timeout, before done(0). The subscriber connection stays open throughout. New subscriptions
  in that interval are accepted but receive only the dead publisher's cached last object.
- Existing subscriptions are never moved to a replacement publisher on the same namespace, however
  the old one ended.
- After a clean end, new subscriptions fail with request error 16 `unroutable` until the
  replacement announces, and are served by it at once from then on.
- While a namespace stays announced after one of its tracks ended cleanly, re-subscribing to that
  track replays its cached last group followed by another done, one round trip later each time.

See [issue #34](https://github.com/muxinc/relay-sdks/issues/34) and
[issue #21](https://github.com/muxinc/relay-sdks/issues/21) for the measurements; they do not
define a recovery deadline.

An orderly publisher close helps the relay learn about shutdown sooner, but neither graceful
closure nor successful replacement writes promise transparent handoff. Write completion only
means the transport accepted the bytes.

## Application recovery policy

A live application that needs faster recovery must define freshness and own the recovery loop.
A useful policy is:

1. Track time to first usable object and time since the last **fresh** object with a monotonic local
   clock. Choose thresholds for the content's expected cadence and tolerated interruption. A quiet
   catalog, sparse event track, paused source, or long GOP is not evidence of failure by itself.
   Where silence is valid, use an application heartbeat or an external publisher-health signal.
2. When progress stops, report a stalled state and schedule one recovery attempt. Do not wait only
   for `on_publisher_done`: it can be delayed while the relay detects a dead upstream connection. Keep an
   overall recovery deadline and a cancellation path; do not create a new loop on every callback.
3. Retire the old subscription before accumulating replacement attempts. For live playback,
   cancelling it deliberately discards any trailing data so the application can prefer a fresh
   random-access point. A consumer that needs trailing data must choose its own bounded drain
   policy before cancelling; `on_publisher_done` alone does not prove the data is drained.
4. Request the desired live track again. The subscriber connection can stay open; if it has closed,
   open a replacement connection first. A new connection is not a guaranteed bypass for relay state.
   During a known publisher transition, classify the fresh-namespace rejection (16 `unroutable`, or
   `404` on older relays) and initial-response timeouts as potentially transient and retry within
   the recovery budget. A subscription accepted before the replacement announces may serve only the
   old publisher's cached object, so check freshness rather than trusting acceptance. Do not
   re-subscribe to a track the publisher ended while it is still announced: that only replays the
   finished track. A status-2 done alone does not prove this, because moq-relay sends one after the
   subscription's own `update()` too; in that case re-subscribing is the recovery. Authentication, invalid input, and protocol failures need their
   own handling rather than an unlimited retry loop.
5. Apply backoff and jitter across failed attempts **and accepted-but-stale attempts**. Cached
   objects followed by `on_publisher_done` must not reset the budget or create an immediate resubscribe loop.
   Reset only after meaningful fresh progress. See
   [the cached-track resubscription issue](https://github.com/muxinc/relay-sdks/issues/21).
6. On a media publisher generation change, reacquire the catalog/configuration and resume at a
   usable keyframe. Group/object IDs can restart; they are not an application continuity identifier.
   If continuity matters, include a publisher generation and sequence in the application payload
   and define how to reject replay, handle gaps, and identify a new generation.

The SDK cannot choose these thresholds or interpret these payloads for a raw byte track. This
policy belongs in the application or player; changing a request timeout will not implement it.
The simple examples retry initial announcement races, but are not complete subscriber recovery
supervisors.

## When predictable handoff matters

If you control discovery, assign each publisher generation a fresh namespace under the authorized
root and publish that namespace through your application's discovery channel. Viewers can then
explicitly switch to the new generation and cancel the old subscriptions. This avoids relying on
reuse of the ended namespace's cached state, but initial announcement can still race subscription;
startup retry, freshness checks, and a bounded switch deadline remain necessary. The SDK does not
provide this discovery channel.

For a fixed namespace, keep the recovery policy above and budget for relay convergence. SDK-side
retry can mitigate a stall, but cannot rebind an existing subscription at the relay or guarantee a
handoff latency. Prompt upstream-death notification, replacement-source routing, and stale state
after retract depend on the relay.
