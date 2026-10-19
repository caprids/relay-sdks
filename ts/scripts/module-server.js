import { readFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { resolve, sep } from 'node:path';

/**
 * Serve a page whose only content is `importMap` and `body`, plus the `.js` files under each directory in `modules`
 * (URL prefix to directory), on 127.0.0.1 at `port` (default: any free one). The Chromium launchers load their
 * runtime-neutral modules into a page from it, and an example page can be served the same way.
 * With `transpile`, a `.js` request with no such file serves the `.ts` source beside it, types stripped by
 * TypeScript, so a page can load sources that are only typechecked, such as the examples and test helpers.
 * Only requests addressed to the loopback interface by name are served: a page that holds a token must not be readable
 * from another site's hostname resolved to 127.0.0.1 (DNS rebinding).
 * Resolves with the server once it listens; its port is `server.address().port`.
 */
export async function startModuleServer({ modules, importMap, transpile = false, body = '', port = 0 }) {
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
  const page = `<!doctype html><script type="importmap">${JSON.stringify({ imports: importMap })}</script>${body}`;
  const server = createServer(async (request, response) => {
    try {
      const { port: listening } = server.address();
      if (![`127.0.0.1:${listening}`, `localhost:${listening}`].includes(request.headers.host ?? '')) {
        response.writeHead(421).end();
        return;
      }
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
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, '127.0.0.1', resolve);
  });
  return server;
}
