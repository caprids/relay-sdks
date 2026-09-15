/**
 * MOQT subgroup data-stream writing (moq-transport draft-19 §11.4.2) — the publish-direction complement to
 * `object-stream.ts`'s reader.
 *
 * One writer owns one unidirectional stream carrying one subgroup of one group: the SUBGROUP_HEADER goes out first,
 * objects follow serially with delta-encoded object IDs, and `fin()` closes the stream to mark the group complete.
 * `abort()` resets the stream instead — the drop path for a publisher falling behind transport backpressure.
 *
 * The MSF/LOC shape this engine publishes is one subgroup per group at subgroup ID 0, so the writer pins
 * SUBGROUP_ID_MODE to `0b00` (no explicit subgroup-id field) and sets END_OF_GROUP: on FIN the subgroup contains the
 * group's largest object by construction.
 *
 * Everything written here round-trips through `readSubgroupHeader` + `readSubgroupObjects` — that pairing is the
 * module's contract and its test suite.
 */
import { ByteWriter } from './bytes.js';
import { encodeKeyValuePairs, type KeyValuePair } from './control-messages.js';
import { MoqtProtocolError } from './errors.js';
import { MAX_OBJECT_PAYLOAD_LENGTH, MAX_OBJECT_PROPERTIES_LENGTH, type ObjectStatus } from './object-stream.js';

const SUBGROUP_TYPE_BASE = 0x10;
const SUBGROUP_TYPE_PROPERTIES = 0x01;
const SUBGROUP_TYPE_END_OF_GROUP = 0x08;
const SUBGROUP_TYPE_DEFAULT_PRIORITY = 0x20;
const SUBGROUP_TYPE_FIRST_OBJECT = 0x40;

const OBJECT_STATUS_WIRE: Record<ObjectStatus, number> = {
  normal: 0,
  'end-of-group': 3,
  'end-of-track': 4,
};

export interface SubgroupWriterOptions {
  trackAlias: number;
  groupId: number;
  /**
   * Explicit publisher priority byte. Omitted, the header sets the DEFAULT_PRIORITY flag and subscribers fall back to
   * the subscription's priority.
   */
  priority?: number;
  /**
   * Every object on this stream carries a Properties block (LOC frame metadata). Default true — an empty properties
   * list still encodes (as a zero length), so mixed-metadata tracks need no special casing.
   */
  hasProperties?: boolean;
  /** The subgroup contains the group's largest object on FIN. Default true. */
  endOfGroup?: boolean;
}

export interface SubgroupObjectInput {
  /** Absolute object ID; must increase monotonically within the stream. */
  objectId: number;
  properties?: readonly KeyValuePair[];
  payload: Uint8Array;
  /** Only meaningful for zero-length payloads (§11.2.1.1). Default 'normal'. */
  status?: ObjectStatus;
}

export interface SubgroupWriter {
  readonly groupId: number;
  /** Serialize one object. Resolves when the transport accepted the bytes. */
  writeObject(object: SubgroupObjectInput): Promise<void>;
  /** FIN the stream — the group is complete. */
  fin(): Promise<void>;
  /** Reset the stream — delivered objects may be incomplete (drop path). */
  abort(reason?: unknown): void;
}

function encodeSubgroupHeader(options: SubgroupWriterOptions): Uint8Array {
  const hasProperties = options.hasProperties ?? true;
  const endOfGroup = options.endOfGroup ?? true;
  const type =
    SUBGROUP_TYPE_BASE |
    SUBGROUP_TYPE_FIRST_OBJECT |
    (hasProperties ? SUBGROUP_TYPE_PROPERTIES : 0) |
    (endOfGroup ? SUBGROUP_TYPE_END_OF_GROUP : 0) |
    (options.priority === undefined ? SUBGROUP_TYPE_DEFAULT_PRIORITY : 0);

  const writer = new ByteWriter(32);

  writer.writeVarint(type);
  writer.writeVarint(options.trackAlias);
  writer.writeVarint(options.groupId);

  if (options.priority !== undefined) writer.writeUint8(options.priority);

  return writer.toBytes();
}

function encodeObject(
  object: SubgroupObjectInput,
  previousObjectId: number | undefined,
  hasProperties: boolean
): Uint8Array {
  if (previousObjectId !== undefined && object.objectId <= previousObjectId) {
    throw new MoqtProtocolError(`subgroup object ID ${object.objectId} does not increase past ${previousObjectId}`);
  }

  if (object.payload.length > MAX_OBJECT_PAYLOAD_LENGTH) {
    throw new MoqtProtocolError(
      `object payload length ${object.payload.length} exceeds ${MAX_OBJECT_PAYLOAD_LENGTH} bytes`
    );
  }

  const writer = new ByteWriter(object.payload.length + 64);

  // First object carries its absolute ID; the rest are `delta + 1` (§11.4.2).
  writer.writeVarint(previousObjectId === undefined ? object.objectId : object.objectId - previousObjectId - 1);

  if (hasProperties) {
    const properties = new ByteWriter(64);

    encodeKeyValuePairs(properties, object.properties ?? []);

    if (properties.length > MAX_OBJECT_PROPERTIES_LENGTH) {
      throw new MoqtProtocolError(
        `object properties length ${properties.length} exceeds ${MAX_OBJECT_PROPERTIES_LENGTH} bytes`
      );
    }

    writer.writeLengthPrefixed(properties.toBytes());
  } else if (object.properties?.length) {
    throw new MoqtProtocolError('object carries properties on a subgroup stream without the PROPERTIES flag');
  }

  writer.writeVarint(object.payload.length);

  if (object.payload.length === 0) {
    writer.writeVarint(OBJECT_STATUS_WIRE[object.status ?? 'normal']);
  } else {
    writer.writeBytes(object.payload);
  }

  return writer.toBytes();
}

/**
 * Open a subgroup writer over an established unidirectional stream. The SUBGROUP_HEADER is queued immediately; objects
 * follow in `writeObject` call order (writes serialize through the stream's own queue).
 */
export function createSubgroupWriter(
  stream: WritableStream<Uint8Array>,
  options: SubgroupWriterOptions
): SubgroupWriter {
  const hasProperties = options.hasProperties ?? true;
  const writer = stream.getWriter();
  let previousObjectId: number | undefined;
  let finished = false;
  let finishing: Promise<void> | undefined;

  // Header errors surface on the first awaited write/fin — an errored
  // stream rejects everything queued after it.
  const headerWrite = writer.write(encodeSubgroupHeader(options));

  headerWrite.catch(() => {});

  return {
    groupId: options.groupId,

    async writeObject(object: SubgroupObjectInput): Promise<void> {
      if (finished || finishing) throw new MoqtProtocolError('subgroup stream already finished');

      const bytes = encodeObject(object, previousObjectId, hasProperties);

      previousObjectId = object.objectId;
      await headerWrite;
      await writer.write(bytes);
    },

    fin(): Promise<void> {
      if (finishing) return finishing;
      if (finished) return Promise.resolve();

      finishing = (async () => {
        try {
          await headerWrite;
          await writer.close();
        } finally {
          finished = true;
        }
      })();
      return finishing;
    },

    abort(reason?: unknown): void {
      if (finished) return;

      finished = true;
      writer.abort(reason).catch(() => {});
    },
  };
}
