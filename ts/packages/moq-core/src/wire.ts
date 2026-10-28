/**
 * The MOQT wire codec, for code that stands in for the other end of a session: a test relay that drives a publish or
 * subscribe session byte by byte. Covered by semver like the package index, which keeps these out of the client
 * surface. The SDK's sessions use the same codec, so a byte written here is a byte a session reads.
 *
 * @module
 */
export { StreamReader, utf8Encode } from './protocol/bytes.js';
export type {
  ControlMessage,
  ControlMessageFrame,
  FetchRequest,
  SubscribeNamespaceRequest,
  SubscribeRequest,
} from './protocol/control-messages.js';
export {
  ControlMessageDeframer,
  decodeControlMessage,
  encodeGoaway,
  encodePublish,
  encodePublishDone,
  encodeRequestError,
  encodeSetup,
  encodeSubscribe,
  encodeSubscribeNamespace,
  encodeSubscribeOk,
} from './protocol/control-messages.js';
export type { SubgroupHeader, SubgroupIdMode } from './protocol/object-stream.js';
export {
  isSubgroupHeaderType,
  readFetchHeader,
  readSubgroupHeader,
  readSubgroupObjects,
  STREAM_TYPE,
} from './protocol/object-stream.js';
export type { VarintDecodeResult } from './protocol/varint.js';
export { decodeVarint, encodeVarint } from './protocol/varint.js';
