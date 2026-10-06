/**
 * Delivery timeouts (see CONTEXT.md): the draft's SUBGROUP_DELIVERY_TIMEOUT and OBJECT_DELIVERY_TIMEOUT, as the write
 * slot applies them to one group (draft-ietf-moq-transport-22 §5.2).
 *
 * Each side has a value for each timeout. The publisher's is the property (0x02 or 0x06) on the group's first object
 * when present, and the track property otherwise; the subscriber's is the parameter on the relay subscription, as
 * updated by REQUEST_UPDATE. The group uses the smaller of the two when both are non-zero, and the non-zero one
 * otherwise. 0 means no timeout. Nothing here rejects a value: a byte value is ignored, and a value above the timer
 * bound is clamped to it.
 */
import type { KeyValuePair, MessageParameters } from '@mux/moq-core';
import { MAX_TIMEOUT_MS } from './connect-session.js';

/** One side's delivery timeouts, or a group's effective ones, in milliseconds. 0 means no timeout. */
export interface DeliveryTimeouts {
  /** OBJECT_DELIVERY_TIMEOUT: the most an object may age before it reaches the transport. */
  objectMs: number;
  /** SUBGROUP_DELIVERY_TIMEOUT: the most a group may wait, from its FIN, for the relay's acknowledgment. */
  subgroupMs: number;
}

/** Property and parameter type of OBJECT_DELIVERY_TIMEOUT (§9.20.4, §10.2). */
export const OBJECT_DELIVERY_TIMEOUT = 0x02;
/** Property and parameter type of SUBGROUP_DELIVERY_TIMEOUT (§9.20.3, §10.1). */
export const SUBGROUP_DELIVERY_TIMEOUT = 0x06;

/** No timeout of either kind. */
export const NO_DELIVERY_TIMEOUTS: DeliveryTimeouts = Object.freeze({ objectMs: 0, subgroupMs: 0 });

/** The first integer value of `type` among `properties`, if any. A byte value is not a timeout. */
function propertyValue(properties: readonly KeyValuePair[], type: number): number | undefined {
  for (const property of properties) {
    if (property.type === type && typeof property.value === 'number') return property.value;
  }
  return undefined;
}

/**
 * The publisher's values for a group: each timeout from the group's first object's properties when present there,
 * and from the track's otherwise (§5.2). Pass the track's own values as `fallback`.
 */
export function publisherTimeouts(
  properties: readonly KeyValuePair[],
  fallback: DeliveryTimeouts = NO_DELIVERY_TIMEOUTS,
): DeliveryTimeouts {
  return {
    objectMs: propertyValue(properties, OBJECT_DELIVERY_TIMEOUT) ?? fallback.objectMs,
    subgroupMs: propertyValue(properties, SUBGROUP_DELIVERY_TIMEOUT) ?? fallback.subgroupMs,
  };
}

/**
 * A subscription's values after `parameters`: each timeout the message carries replaces the previous one, and an
 * absent one keeps it (§9.5: a parameter not present in REQUEST_UPDATE keeps its value).
 */
export function subscriberTimeouts(
  parameters: MessageParameters,
  previous: DeliveryTimeouts = NO_DELIVERY_TIMEOUTS,
): DeliveryTimeouts {
  return {
    objectMs: parameters.objectDeliveryTimeout ?? previous.objectMs,
    subgroupMs: parameters.subgroupDeliveryTimeout ?? previous.subgroupMs,
  };
}

/** The smaller non-zero of two values, clamped to the timer bound; 0 when neither is set. */
function smallerNonZero(a: number, b: number): number {
  const set = [a, b].filter((value) => value > 0);
  return set.length === 0 ? 0 : Math.min(MAX_TIMEOUT_MS, ...set);
}

/** A group's effective timeouts from the publisher's and the subscriber's values (§5.2). */
export function effectiveTimeouts(publisher: DeliveryTimeouts, subscriber: DeliveryTimeouts): DeliveryTimeouts {
  return {
    objectMs: smallerNonZero(publisher.objectMs, subscriber.objectMs),
    subgroupMs: smallerNonZero(publisher.subgroupMs, subscriber.subgroupMs),
  };
}
