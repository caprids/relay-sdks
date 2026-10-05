import { describe, expect, it } from 'vitest';

import { platformSuffix } from '../addon.js';
import { resetCode, safeCode, toError } from '../errors.js';

describe('resetCode (TA-17)', () => {
  it("takes the reason's integer streamErrorCode from 0 to 2^32-1", () => {
    for (const code of [0, 7, 0x12, 255, 256, 0xffff_ffff]) expect(resetCode({ streamErrorCode: code })).toBe(code);
  });

  it('is 0 for a missing, invalid or out-of-range code, whatever the class', () => {
    for (const reason of [undefined, null, 'x', 7, {}, new Error('no code')]) expect(resetCode(reason)).toBe(0);
    for (const code of [-1, 0x1_0000_0000, 1.5, Number.NaN, '7', null]) {
      expect(resetCode({ streamErrorCode: code })).toBe(0);
    }
  });
});

describe('safeCode (TA-22)', () => {
  it('is the number up to 2^53-1 and null above it, where a number would round', () => {
    expect(safeCode('11')).toBe(11);
    expect(safeCode(String(Number.MAX_SAFE_INTEGER))).toBe(Number.MAX_SAFE_INTEGER);
    expect(safeCode('9007199254740992')).toBeNull();
    expect(safeCode('4611686018427387903')).toBeNull();
    expect(safeCode(undefined)).toBeNull();
  });
});

describe('toError (TA-22, TA-23, TA-24)', () => {
  it("makes a peer reset or stop a stream error with the peer's code", () => {
    const error = toError({ source: 'stream', kind: 'stopped', code: '13', reason: '' });
    expect(error).toBeInstanceOf(Error);
    expect(error).toMatchObject({ source: 'stream', streamErrorCode: 13 });
    expect(toError({ source: 'stream', kind: 'reset', code: '4611686018427387903', reason: '' })).toMatchObject({
      source: 'stream',
      streamErrorCode: null,
    });
  });

  it('makes a reset that carried no code (a GREASE slot, TA-27) a stream error with a null code', () => {
    const error = toError({ source: 'stream', kind: 'reset', reason: '' });
    expect(error).toMatchObject({ source: 'stream', streamErrorCode: null, message: 'the peer reset the stream' });
  });

  it("makes the peer's close a session error carrying its code and reason", () => {
    expect(toError({ source: 'session', kind: 'application', code: '16', reason: 'unauthorized' })).toMatchObject({
      source: 'session',
      closeCode: 16,
      reason: 'unauthorized',
    });
  });

  it('gives a lost connection neither a code nor a reason', () => {
    const error = toError({ source: 'session', kind: 'timeout', reason: 'the connection timed out' });
    expect(error).toMatchObject({ source: 'session' });
    expect(error).not.toHaveProperty('closeCode');
  });

  it('gives a credit rejection no source, so it is never read as the session ending', () => {
    const error = toError({ source: 'credit', kind: 'credit', reason: 'no stream credit' });
    expect(error).toBeInstanceOf(Error);
    expect(error).not.toHaveProperty('source');
  });
});

describe('platformSuffix', () => {
  it("names the binary the way napi-rs's --platform does", () => {
    expect(platformSuffix('linux', 'x64', true)).toBe('linux-x64-gnu');
    expect(platformSuffix('linux', 'arm64', false)).toBe('linux-arm64-musl');
    expect(platformSuffix('darwin', 'arm64', true)).toBe('darwin-arm64');
    expect(platformSuffix('win32', 'x64', true)).toBe('win32-x64-msvc');
  });
});
