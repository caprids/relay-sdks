/**
 * Loads the native addon (`native/`, built by `napi build`) for this platform. The binary sits beside the package as
 * `native/relay-quic.<platform>-<arch>[-<libc>].node`; until prebuilt binaries ship it comes from a local build.
 */
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import type * as Addon from './native.js';

export type { BiStream, Client, Dial, Failure, ReadResult, RecvStream, SendStream, Session } from './native.js';

/** The addon's exports. */
export type NativeAddon = typeof Addon;

let addon: NativeAddon | undefined;

/** The platform suffix napi-rs gives the binary: `linux-x64-gnu`, `linux-x64-musl`, `darwin-arm64` and so on. */
export function platformSuffix(
  platform: string = process.platform,
  arch: string = process.arch,
  glibc: boolean = hasGlibc(),
): string {
  if (platform === 'linux') return `linux-${arch}-${glibc ? 'gnu' : 'musl'}`;
  if (platform === 'win32') return `win32-${arch}-msvc`;
  return `${platform}-${arch}`;
}

function hasGlibc(): boolean {
  const report = process.report?.getReport() as { header?: { glibcVersionRuntime?: string } } | undefined;
  return typeof report?.header?.glibcVersionRuntime === 'string';
}

/** The addon, loaded on first use. Throws, naming the missing file and how to build it, when there is no binary. */
export function loadAddon(): NativeAddon {
  if (addon) return addon;
  const file = `relay-quic.${platformSuffix()}.node`;
  const url = new URL(`../native/${file}`, import.meta.url);
  try {
    addon = createRequire(import.meta.url)(fileURLToPath(url)) as NativeAddon;
  } catch (error) {
    if ((error as { code?: unknown }).code !== 'MODULE_NOT_FOUND') throw error;
    throw new Error(
      `@mux/relay-quic has no native binary for this platform (${file}). Build it with ` +
        '`pnpm --filter @mux/relay-quic build:native`, which needs a Rust toolchain.',
      { cause: error },
    );
  }
  return addon;
}
