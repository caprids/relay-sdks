import assert from 'node:assert/strict';
import { readdir, readFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const core = JSON.parse(await readFile(resolve(root, 'packages/moq-core/package.json'), 'utf8'));
const media = JSON.parse(await readFile(resolve(root, 'packages/moq-media/package.json'), 'utf8'));
const sdk = JSON.parse(await readFile(resolve(root, 'package.json'), 'utf8'));

assert.deepEqual(core.dependencies ?? {}, {}, 'Generic MoQ core must have no runtime dependencies');
assert.deepEqual(
  Object.keys(sdk.dependencies ?? {}),
  [core.name],
  'The SDK should depend only on the generic MoQ core',
);

assert.deepEqual(media.dependencies, { [core.name]: 'workspace:*' }, 'Media formats depend only on the generic core');

async function inspect(directory, permittedPackage) {
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = resolve(directory, entry.name);
    if (entry.isDirectory()) {
      await inspect(path, permittedPackage);
    } else if (entry.name.endsWith('.js')) {
      const text = await readFile(path, 'utf8');
      for (const match of text.matchAll(/(?:from\s*|import\s*\(\s*|import\s*)['"]([^'"]+)['"]/g)) {
        const specifier = match[1];
        assert(
          specifier.startsWith('.') || specifier === permittedPackage,
          `Unexpected runtime import in ${path}: ${specifier}`,
        );
      }
    }
  }
}

await inspect(resolve(root, 'packages/moq-core/dist'));
await inspect(resolve(root, 'packages/moq-media/dist'), core.name);
await inspect(resolve(root, 'dist'), core.name);
const { buildMsfCatalog } = await import('../packages/moq-media/dist/index.js');
assert.equal(typeof buildMsfCatalog, 'function');
const { connectRelay } = await import('../dist/index.js');
assert.equal(typeof connectRelay, 'function');
console.log('Built SDK and media formats import only the generic core; core has no runtime package imports.');
