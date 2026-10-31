/**
 * Relay endpoints: the Endpoint rule every connect call checks, and the room-rooted endpoint for a room, the endpoint
 * with the room's fields appended to its path ("Room-rooted endpoints" in spec/behavior.md). Pure: it builds strings
 * and connects nothing.
 */
import { namespaceFields } from './validation.js';

/**
 * The characters a room field may hold: what a URL path carries unchanged in every parser and
 * moq-relay reads back as the same field. The relay does not percent-decode the path, so a field
 * that would need encoding cannot be rooted at, and `*` fails the connection (spec/relay-interop.md,
 * "Endpoint paths").
 */
const ROOM_FIELD = /^[A-Za-z0-9\-._~!$&'()+,;=:@]+$/;

/** The hosted Relay endpoint, pinned in spec/protocol.json as `relay.defaultEndpoint`. */
export const DEFAULT_ENDPOINT = 'https://relay.mux.global';

/**
 * Check an endpoint by the Endpoint rule in spec/behavior.md and return it parsed. Not a string, or
 * not a URL at all (including an https URL with no host), is a TypeError: the wrong shape. A URL
 * that parses but is not one Relay accepts is a RangeError: a disallowed value.
 */
export function endpointUrl(endpoint: unknown): URL {
  if (typeof endpoint !== 'string') throw new TypeError('Relay endpoints must be a URL string');
  let url: URL;
  try {
    url = new URL(endpoint);
  } catch (error) {
    throw new TypeError('Relay endpoints must be a valid absolute URL', { cause: error });
  }
  if (url.protocol !== 'https:') throw new RangeError('Relay endpoints must use HTTPS');
  if (url.hash || url.username || url.password)
    throw new RangeError('Relay endpoints cannot contain fragments or userinfo');
  // A present jwt wins over getToken, so an empty one would connect with no credential at all.
  if (url.searchParams.getAll('jwt').some((value) => value.length === 0))
    throw new RangeError(
      'Relay endpoint has an empty jwt query parameter; put a token in it, or remove it and supply getToken',
    );
  return url;
}

/**
 * Root an endpoint at a room. The result is the endpoint with each room field appended to its
 * path, after any path it has and without its trailing slashes, and with its query kept, a `jwt`
 * parameter included. A connection to it names namespaces relative to the room. Without an
 * endpoint it roots {@link DEFAULT_ENDPOINT}:
 *
 * ```ts
 * roomEndpoint(['acme', 'rooms', '42']);
 * // 'https://relay.mux.global/acme/rooms/42'
 * roomEndpoint('https://relay.example', ['acme', 'rooms', '42']);
 * // 'https://relay.example/acme/rooms/42'
 * ```
 *
 * The endpoint follows the connect calls' rules and the room the namespace rules. Nothing is
 * percent-encoded: a room field holds only `A`–`Z`, `a`–`z`, `0`–`9` and `-._~!$&'()+,;=:@`, and is
 * not `.` or `..`. A namespace with any other field is reached from the bare host instead.
 *
 * @throws {TypeError} An endpoint that is not an absolute URL string, or a room that is not a
 * sequence of non-empty string fields.
 * @throws {RangeError} An endpoint that is not `https`, has a fragment or userinfo, or an empty
 * `jwt`; a room of more than 32 fields; a room field with any other character, or `.` or `..`.
 */
export function roomEndpoint(room: readonly string[]): string;
export function roomEndpoint(endpoint: string, room: readonly string[]): string;
export function roomEndpoint(...args: [room: readonly string[]] | [endpoint: string, room: readonly string[]]): string {
  // One argument is the room alone; two are the endpoint and the room, as before the default.
  const [endpoint, room] = args.length === 1 ? [DEFAULT_ENDPOINT, args[0]] : args;
  const url = endpointUrl(endpoint);
  const fields = namespaceFields(room);
  fields.forEach((field, index) => {
    if (!ROOM_FIELD.test(field))
      throw new RangeError(
        `Room field ${index} (${JSON.stringify(field)}) has a character a room-rooted endpoint cannot carry: ` +
          "only A-Z, a-z, 0-9 and -._~!$&'()+,;=:@ reach the relay as the same field. " +
          'Reach this namespace from the bare host instead',
      );
    if (field === '.' || field === '..')
      throw new RangeError(
        `Room field ${index} is "${field}", which a URL folds into the path; reach this namespace from the bare host instead`,
      );
  });
  url.pathname = `${url.pathname.replace(/\/+$/, '')}/${fields.join('/')}`;
  return url.href;
}
