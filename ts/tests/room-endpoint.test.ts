/**
 * `roomEndpoint`: the cases come from spec/vectors/room-endpoint.json and run in every SDK, per
 * "Room-rooted endpoints" in spec/behavior.md; the local cases hold what JSON cannot carry.
 */
import { readFile } from 'node:fs/promises';
import { describe, expect, it } from 'vitest';
import { roomEndpoint } from '../src/index.js';

type ErrorKind = 'type' | 'range';
interface RoomEndpointCase {
  name: string;
  endpoint: unknown;
  room: unknown;
  expected?: string;
  error?: ErrorKind;
}
const vectors = JSON.parse(
  await readFile(new URL('../../spec/vectors/room-endpoint.json', import.meta.url), 'utf8'),
) as { cases: RoomEndpointCase[] };
const ERROR_CLASS: Record<ErrorKind, ErrorConstructor> = { type: TypeError, range: RangeError };
const build = (endpoint: unknown, room: unknown) => roomEndpoint(endpoint as string, room as readonly string[]);

describe('spec/vectors/room-endpoint.json', () => {
  it.each(vectors.cases.filter((item) => item.error === undefined))('$name', ({ endpoint, room, expected }) => {
    expect(build(endpoint, room)).toBe(expected);
  });

  it.each(vectors.cases.filter((item) => item.error !== undefined))('$name is refused', ({ endpoint, room, error }) => {
    expect(() => build(endpoint, room)).toThrow(ERROR_CLASS[error as ErrorKind]);
  });
});

describe('roomEndpoint', () => {
  it('names the refused field and points at the bare host', () => {
    expect(() => roomEndpoint('https://relay.example', ['acme', 'a b'])).toThrow(/Room field 1 \("a b"\).*bare host/);
    expect(() => roomEndpoint('https://relay.example', ['..'])).toThrow(/Room field 0 is "\.\."/);
  });

  it('refuses a sparse room as a type error, as the namespace rule does', () => {
    // biome-ignore lint/suspicious/noSparseArray: the hole is the case under test.
    expect(() => build('https://relay.example', ['acme', , 'rooms'])).toThrow(TypeError);
  });

  it('does not change the room it was given', () => {
    const room = ['acme', 'rooms'];
    roomEndpoint('https://relay.example', room);
    expect(room).toEqual(['acme', 'rooms']);
  });
});
