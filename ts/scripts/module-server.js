import { readFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { resolve, sep } from 'node:path';

/**
 * Serve a page whose only content is `importMap`, plus the `.js` files under each directory in `modules` (URL prefix
 * to directory), on 127.0.0.1. The Chromium launchers load their runtime-neutral modules into a page from it.
 * With `transpile`, a `.js` request with no such file serves the `.ts` source beside it, types stripped by
 * TypeScript, so a page can load sources that are only typechecked, such as the examples and test helpers.
 * Resolves with the server once it listens; its port is `server.address().port`.
 */
export async function startModuleServer({ modules, importMap, transpile = false }) {
  const typescript = transpile ? (await import('typescript')).default : undefined;
  const load = async (file) => {
    try {
      return await readFile(file);
    } catch (error) {
      if (!typescript || error?.code !== 'ENOENT') throw error;
      const source = await readFile(`${file.slice(0, -'.js'.length)}.ts`, 'utf8');
      return typescript.transpileModule(source, {
        compilerOptions: { module: typescript.ModuleKind.ESNext, target: typescript.ScriptTarget.ES2022 },
      }).outputText;
    }
  };
  const page = `<!doctype html><script type="importmap">${JSON.stringify({ imports: importMap })}</script>`;
  const server = createServer(async (request, response) => {
    try {
      const path = new URL(request.url, 'http://localhost').pathname;
      if (path === '/') {
        response.setHeader('Content-Type', 'text/html');
        response.end(page);
        return;
      }
      const prefix = Object.keys(modules).find((candidate) => path.startsWith(candidate));
      if (!prefix) {
        response.writeHead(404).end();
        return;
      }
      const base = modules[prefix];
      const file = resolve(base, decodeURIComponent(path.slice(prefix.length)));
      if (!file.startsWith(base + sep) || !file.endsWith('.js')) {
        response.writeHead(403).end();
        return;
      }
      response.setHeader('Content-Type', 'text/javascript');
      response.end(await load(file));
    } catch {
      response.writeHead(404).end();
    }
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  return server;
}
