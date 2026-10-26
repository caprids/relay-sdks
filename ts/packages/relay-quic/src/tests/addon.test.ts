import { createRequire } from 'node:module';
import { describe, expect, it } from 'vitest';

import { addonCandidates, loadFirst } from '../addon.js';

const require = createRequire(import.meta.url);

/** A `require` that finds only `present`, and reports any other id missing the way Node does. */
function requireOnly(present: Record<string, unknown>): (id: string) => unknown {
  return (id) => {
    if (id in present) return present[id];
    return require(id);
  };
}

describe('addonCandidates (#195)', () => {
  it('tries the local build first, then the prebuilt platform package', () => {
    const [local, pkg] = addonCandidates('linux-arm64-musl');
    expect(local).toMatch(/[/\\]native[/\\]relay-quic\.linux-arm64-musl\.node$/);
    expect(pkg).toBe('@caprid/relay-quic-linux-arm64-musl');
  });
});

describe('loadFirst (#195)', () => {
  // Neither exists, whatever was built here: a local build in native/ would otherwise load for real.
  const local = '/nowhere/native/relay-quic.linux-x64-gnu.node';
  const pkg = '@caprid/relay-quic-nowhere-linux-x64-gnu';

  it('takes a local build over the platform package', () => {
    expect(loadFirst([local, pkg], requireOnly({ [local]: 'local', [pkg]: 'package' }), 'linux-x64-gnu')).toBe('local');
  });

  it('falls back to the platform package when there is no local build', () => {
    expect(loadFirst([local, pkg], requireOnly({ [pkg]: 'package' }), 'linux-x64-gnu')).toBe('package');
  });

  it('names the platform package and the local build when neither is there', () => {
    expect(() => loadFirst([local, pkg], requireOnly({}), 'linux-x64-gnu')).toThrow(
      /no native binary for this platform \(linux-x64-gnu\)\. Install its optional dependency @caprid\/relay-quic-nowhere-linux-x64-gnu.*build:native/s,
    );
  });

  it("throws a binary's own load failure instead of looking further", () => {
    const failure = Object.assign(new Error('Error relocating relay-quic.node: __memcpy_chk: symbol not found'), {
      code: 'ERR_DLOPEN_FAILED',
    });
    const load = (id: string) => {
      if (id === local) throw failure;
      return 'package';
    };
    expect(() => loadFirst([local, pkg], load, 'linux-x64-gnu')).toThrow(failure);
  });

  it('throws when the platform package is there but its binary is missing', () => {
    const message = `Cannot find module '/app/node_modules/${pkg}/relay-quic.linux-x64-gnu.node'`;
    const broken = Object.assign(new Error(message), { code: 'MODULE_NOT_FOUND' });
    const load = (id: string) => {
      if (id === pkg) throw broken;
      return require(id);
    };
    expect(() => loadFirst([local, pkg], load, 'linux-x64-gnu')).toThrow(broken);
  });
});
