import assert from 'node:assert/strict';
import { readdir, readFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const core = JSON.parse(await readFile(resolve(root, 'packages/moq-core/package.json'), 'utf8'));
const media = JSON.parse(await readFile(resolve(root, 'packages/moq-media/package.json'), 'utf8'));
const adapter = JSON.parse(await readFile(resolve(root, 'packages/moq-mediabunny/package.json'), 'utf8'));
const quic = JSON.parse(await readFile(resolve(root, 'packages/relay-quic/package.json'), 'utf8'));
const sdk = JSON.parse(await readFile(resolve(root, 'package.json'), 'utf8'));

assert.deepEqual(core.dependencies ?? {}, {}, 'Generic MoQ core must have no runtime dependencies');
assert.deepEqual(
  Object.keys(sdk.dependencies ?? {}),
  [core.name],
  'The SDK should depend only on the generic MoQ core',
);

assert.deepEqual(media.dependencies, { [core.name]: 'workspace:*' }, 'Media formats depend only on the generic core');

assert.deepEqual(adapter.dependencies, { [media.name]: 'workspace:*' });
assert.deepEqual(Object.keys(adapter.peerDependencies), ['mediabunny']);

// The Node transport adapter is the only package with native code and Node built-ins (#194). It builds on the
// generic core alone, and nothing runtime-agnostic depends on it.
assert.deepEqual(quic.dependencies, { [core.name]: 'workspace:*' }, 'The native QUIC adapter depends only on the core');
// `@mux/relay/node` loads the adapter when it is installed (#196): an optional peer, never a dependency.
assert.deepEqual(sdk.peerDependencies, { [quic.name]: 'workspace:*' }, 'The SDK takes only the adapter as a peer');
assert.deepEqual(sdk.peerDependenciesMeta, { [quic.name]: { optional: true } }, 'The adapter is an optional peer');

// Its prebuilt binaries ship as one package per napi target, each holding only its `.node` (#195). The packages are
// named for the platform suffix the loader computes, and its only package-name lookup is that platform package. The
// optional dependencies on them are added only when the packages are packed, so the lockfile never names them.
const quicRoot = resolve(root, 'packages/relay-quic');
const arch = { x86_64: 'x64', aarch64: 'arm64' };
const suffixOf = (triple) => {
  const [cpu, , os, abi] = triple.split('-');
  if (os === 'darwin') return `darwin-${arch[cpu]}`;
  if (os === 'windows') return `win32-${arch[cpu]}-${abi}`;
  return `linux-${arch[cpu]}-${abi}`;
};
assert.equal(quic.optionalDependencies, undefined, 'Platform packages are optional dependencies only when packed');
const platformDirs = (await readdir(resolve(quicRoot, 'npm'))).sort();
assert.deepEqual(platformDirs, quic.napi.targets.map(suffixOf).sort(), 'One platform package per napi target');
for (const suffix of platformDirs) {
  const platform = JSON.parse(await readFile(resolve(quicRoot, 'npm', suffix, 'package.json'), 'utf8'));
  const binary = `relay-quic.${suffix}.node`;
  assert.equal(platform.name, `${quic.name}-${suffix}`, `npm/${suffix} is named for its platform suffix`);
  assert.equal(platform.version, quic.version, `npm/${suffix} moves in lockstep with ${quic.name}`);
  assert.equal(platform.main, binary, `npm/${suffix} has its binary as main`);
  assert.deepEqual(platform.files, [binary], `npm/${suffix} ships only its binary`);
  for (const field of ['dependencies', 'optionalDependencies', 'peerDependencies', 'scripts']) {
    assert.equal(platform[field], undefined, `npm/${suffix} has no ${field}`);
  }
}
const loader = (await readFile(resolve(quicRoot, 'dist/addon.js'), 'utf8')).replace(/\/\*[\s\S]*?\*\//g, '');
assert.deepEqual(
  [...loader.matchAll(/[`'"](@[\w.-]+\/[\w.${}-]+)[`'"]/g)].map((match) => match[1]),
  [`${quic.name}-\${suffix}`],
  'The addon loader looks up no package but the platform one',
);

// Every package releases at one version, under one licence, publicly (#216, ADR 0003). scripts/release.mjs sets the
// version; the registry rejects a scoped package published without public access.
const published = [sdk, core, media, adapter, quic];
for (const suffix of platformDirs) {
  published.push(JSON.parse(await readFile(resolve(quicRoot, 'npm', suffix, 'package.json'), 'utf8')));
}
for (const manifest of published) {
  assert.equal(manifest.version, sdk.version, `${manifest.name} moves in lockstep with ${sdk.name}`);
  assert.equal(manifest.license, 'Apache-2.0', `${manifest.name} is Apache-2.0`);
  assert.equal(manifest.private, undefined, `${manifest.name} is not private: scripts/release.mjs gates publishing`);
  assert.equal(manifest.publishConfig?.access, 'public', `${manifest.name} publishes with public access`);
  assert.equal(manifest.repository?.url, sdk.repository.url, `${manifest.name} names the repository`);
}

async function inspect(directory, permittedPackage, { nodeBuiltins = false, extra = {} } = {}) {
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = resolve(directory, entry.name);
    if (entry.isDirectory()) {
      await inspect(path, permittedPackage, { nodeBuiltins, extra });
    } else if (entry.name.endsWith('.js')) {
      // Block comments go first: a doc comment's code example is not an import.
      const text = (await readFile(path, 'utf8')).replace(/\/\*[\s\S]*?\*\//g, '');
      for (const match of text.matchAll(/(?:from\s*|import\s*\(\s*|import\s*)['"]([^'"]+)['"]/g)) {
        const specifier = match[1];
        assert(
          (specifier.startsWith('.') && !specifier.endsWith('.node')) ||
            specifier === permittedPackage ||
            specifier === extra[path] ||
            (nodeBuiltins && specifier.startsWith('node:')),
          `Unexpected runtime import in ${path}: ${specifier}`,
        );
      }
    }
  }
}

await inspect(resolve(root, 'packages/moq-core/dist'));
await inspect(resolve(root, 'packages/moq-media/dist'), core.name);
// Only the Node entry point may load the adapter.
await inspect(resolve(root, 'dist'), core.name, { extra: { [resolve(root, 'dist/node.js')]: quic.name } });
await inspect(resolve(root, 'packages/moq-mediabunny/dist'), media.name);
await inspect(resolve(root, 'packages/relay-quic/dist'), core.name, { nodeBuiltins: true });
const mediaExports = await import('../packages/moq-media/dist/index.js');
for (const name of [
  'buildMsfCatalog',
  'chunkToLocFrame',
  'videoTrackFromDecoderConfig',
  'audioTrackFromDecoderConfig',
]) {
  assert.equal(typeof mediaExports[name], 'function', `${name} must be available without a browser runtime`);
}
const adapterExports = await import('../packages/moq-mediabunny/dist/index.js');
for (const name of ['videoTrackFromDecoderConfig', 'audioTrackFromDecoderConfig']) {
  assert.equal(adapterExports[name], mediaExports[name], `${name} remains a compatibility re-export`);
}
const sdkExports = await import('../dist/index.js');
assert.deepEqual(Object.keys(sdkExports).sort(), [
  'RelayError',
  'SubscriptionConnectionClosedError',
  'SubscriptionOverflowError',
  'SubscriptionRequestError',
  'connectRelayPublisher',
  'connectRelaySubscriber',
  'connectSupervisedPublisher',
  'connectSupervisedSubscriber',
  'readJSON',
  'readText',
]);
// The Node entry point exports what the root does, with its own connect functions (#196).
const nodeExports = await import('../dist/node.js');
assert.deepEqual(Object.keys(nodeExports).sort(), Object.keys(sdkExports).sort(), '@mux/relay/node mirrors the root');
for (const name of Object.keys(sdkExports)) {
  const own = name.startsWith('connect');
  assert.equal(nodeExports[name] === sdkExports[name], !own, `${name} ${own ? 'defaults the transport' : 'is shared'}`);
}
// The media entry point exports only the media publishing policy, which the root leaves out (#263).
const mediaPolicy = await import('../dist/media.js');
assert.deepEqual(Object.keys(mediaPolicy).sort(), [
  'createCatalogService',
  'createDocumentService',
  'createFrameWriter',
  'mediaTrackProperties',
]);
// Its declarations stand without the optional adapter installed.
const nodeDeclarations = (await readFile(resolve(root, 'dist/node.d.ts'), 'utf8')).replace(/\/\*[\s\S]*?\*\//g, '');
assert.doesNotMatch(nodeDeclarations, /['"]@mux\/relay-quic['"]/, 'dist/node.d.ts must not import @mux/relay-quic');
// Generated declarations name the shared connection options and each role's own (issue #72).
const declarations = await readFile(resolve(root, 'dist/index.d.ts'), 'utf8');
for (const name of ['ConnectRelayOptions', 'ConnectRelaySubscriberOptions', 'ConnectRelayPublisherOptions']) {
  assert.match(declarations, new RegExp(`\\b${name}\\b`), `${name} must be exported from the built declarations`);
}
console.log(
  'Built SDK and media formats import only the generic core; core has no runtime package imports; ' +
    'only the native QUIC adapter uses Node built-ins, and only @mux/relay/node loads it.',
);
