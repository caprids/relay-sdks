import { describe, expect, it } from 'vitest';
import { readJSON, readText } from '../src/index.js';

const object = (text: string) => ({ payload: new TextEncoder().encode(text) });

describe('payload readers', () => {
  it('decodes only the payload view and preserves the input', () => {
    const bytes = new Uint8Array([0xff, 65, 0xff]);
    expect(readText({ payload: bytes.subarray(1, 2) })).toBe('A');
    expect(bytes).toEqual(new Uint8Array([0xff, 65, 0xff]));
    expect(readText(object('\uFEFFhello'))).toBe('\uFEFFhello');
  });

  it.each([[0xff], [0xc3], [0xed, 0xa0, 0x80]])('rejects malformed UTF-8 %j', (...bytes) => {
    const input = { payload: new Uint8Array(bytes) };
    expect(() => readText(input)).toThrow(TypeError);
    expect(() => readJSON(input)).toThrow(TypeError);
    expect(readText(object('valid'))).toBe('valid');
  });

  it.each(['', '{', 'undefined', 'NaN', 'Infinity', '{"x":1,}', '\uFEFFnull'])('rejects invalid JSON %j', (text) =>
    expect(() => readJSON(object(text))).toThrow(SyntaxError),
  );

  it.each([null, false, 0, '', [1, 2], { text: '世界 🌍' }].map((value) => ({ value })))(
    'reads JSON values $value',
    ({ value }) => {
      expect(readJSON(object(JSON.stringify(value)))).toEqual(value);
    },
  );
});
