/**
 * Loads the native addon (`native/`, built by `napi build`) for this platform. A local build,
 * `native/relay-quic.<platform>-<arch>[-<libc>].node`, wins; otherwise the binary comes from the prebuilt platform
 * package `@caprid/relay-quic-<platform>-<arch>[-<libc>]`, one of this package's optional dependencies (#195).
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

/** Where the addon for a platform suffix may be, in the order {@link loadAddon} tries them. */
export function addonCandidates(suffix: string = platformSuffix()): [localFile: string, platformPackage: string] {
  return [
    fileURLToPath(new URL(`../native/relay-quic.${suffix}.node`, import.meta.url)),
    `@caprid/relay-quic-${suffix}`,
  ];
}

/**
 * The first of `candidates` that `load` finds. A candidate that is not there is skipped; one that is there but fails
 * to load (a wrong libc, say) throws its own error. Throws, naming every place it looked, when none is there.
 */
export function loadFirst(candidates: readonly string[], load: (id: string) => unknown, suffix: string): unknown {
  const missing: unknown[] = [];
  for (const candidate of candidates) {
    try {
      return load(candidate);
    } catch (error) {
      if (!isNotFound(error, candidate)) throw error;
      missing.push(error);
    }
  }
  throw new Error(
    `@caprid/relay-quic has no native binary for this platform (${suffix}). Install its optional dependency ` +
      `${candidates.at(-1)}, which a package manager skips when optional dependencies are off, or build ` +
      `${candidates[0]} with \`pnpm --filter @caprid/relay-quic build:native\`, which needs a Rust toolchain.`,
    { cause: new AggregateError(missing, 'no candidate was found') },
  );
}

/** Whether `error` says `id` itself is missing, rather than something it needs. */
function isNotFound(error: unknown, id: string): boolean {
  const { code, message } = error as { code?: unknown; message?: unknown };
  return code === 'MODULE_NOT_FOUND' && typeof message === 'string' && message.includes(`'${id}'`);
}

/** The addon, loaded on first use. Throws, naming the places it looked, when there is no binary for this platform. */
export function loadAddon(): NativeAddon {
  if (addon) return addon;
  const suffix = platformSuffix();
  addon = loadFirst(addonCandidates(suffix), createRequire(import.meta.url), suffix) as NativeAddon;
  return addon;
}
