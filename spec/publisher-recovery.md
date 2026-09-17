# Recovering after a publisher stops or restarts

Replacing a publisher connection restores an upstream source. It does **not** guarantee that
existing subscribers switch to that source, or that a new subscription succeeds immediately.
These are relay lifecycle limits tracked in [#34](https://github.com/muxinc/relay-sdks/issues/34).
The SDKs do not automatically reconnect or resubscribe.

## What a subscriber can observe

| Signal | What it establishes |
| --- | --- |
| `onOk` / `on_ok` | The relay accepted a subscription. It does not establish that the publisher is still producing live data. |
| `onObject` / `on_object` | An object arrived. It may be cached; arrival alone does not establish freshness. |
| `onDone` / `on_done` | The relay sent `PUBLISH_DONE`. Trailing object streams may still arrive until cancellation or connection closure. |
| `onSubgroupReset` / `on_subgroup_reset` | One subgroup was reset. It does not establish that the entire publisher has stopped. |
| Connection `closed` | This subscriber's transport/session ended. A publisher's separate connection can die while the subscriber connection remains open. |

`requestTimeoutMs` / `request_timeout` bounds only the initial subscription response. Once
`SUBSCRIBE_OK` arrives, this timer is disarmed. It is not a time-to-first-object deadline or an
inactivity detector. Transport keepalive likewise does not prove the publisher is alive.

In the mux.global 0.14.17 campaign on 2026-09-16, a TypeScript subscriber received `PUBLISH_DONE`
about **37 seconds after a Python publisher was SIGKILLed**, without a subgroup reset. Restarting
that publisher caused roughly 30 stalled player samples before playback recovered. The earlier
2026-09-15 reproduction also observed fresh subscribers receiving the replacement while attached
subscribers stayed silent, and `404 dropped` or initial-response timeouts for **5–8 seconds** after
an orderly retract/re-announce. These are observations, not timeout settings, guaranteed upper
bounds, or reasons to treat the `internal error` text as a stable error classifier.

A 2026-09-17 Python check using this SDK revision reproduced a **39.9-second** delay from SIGKILL
to `PUBLISH_DONE`. A fresh subscriber connected while the relay still retained the dead source
received only that source's final cached object, not fresh replacement data. Both subscriber
connections stayed open. A successful subscribe on a new connection is therefore insufficient
as a recovery check. In a second run, a new subscription after the delayed `PUBLISH_DONE`
received 40 objects from the replacement; the original subscriptions received none from it.

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
   for `on_done`: it can be delayed while the relay detects a dead upstream connection. Keep an
   overall recovery deadline and a cancellation path; do not create a new loop on every callback.
3. Retire the old subscription before accumulating replacement attempts. For live playback,
   cancelling it deliberately discards any trailing data so the application can prefer a fresh
   random-access point. A consumer that needs trailing data must choose its own bounded drain
   policy before cancelling; `on_done` alone does not prove the data is drained.
4. Request the desired live track again. The subscriber connection can stay open; if it has closed,
   open a replacement connection first. A new connection is not a guaranteed bypass for relay
   state. During a known publisher transition, classify `404` and initial-response timeouts as
   potentially transient and retry within the recovery budget. Authentication, invalid input, and
   protocol failures need their own handling rather than an unlimited retry loop.
5. Apply backoff and jitter across failed attempts **and accepted-but-stale attempts**. Cached
   objects followed by `on_done` must not reset the budget or create an immediate resubscribe loop.
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
after retract remain relay-side work; this guidance does not close #34.
