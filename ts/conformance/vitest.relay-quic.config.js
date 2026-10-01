// The Node target's run (`pnpm test:conformance:node`): only the @mux/relay-quic conformance file, kept out of
// `pnpm test` because it needs the native addon and the Python accepting end.
import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['conformance/relay-quic.conformance.js'],
    // One peer process serves every case in turn.
    fileParallelism: false,
  },
});
