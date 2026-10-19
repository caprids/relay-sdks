// The WebSocket target's Node run (`pnpm test:conformance:websocket`): only the @mux/relay-websocket conformance file,
// kept out of `pnpm test` because Node must trust the accepting end's certificate before it starts. The certificate is
// made here, in the main process, and the test workers inherit NODE_EXTRA_CA_CERTS when they fork.
import { defineConfig } from 'vitest/config';
import { createPeerCertificate } from '../scripts/conformance-peer.js';

const { certificate, key } = createPeerCertificate();
process.env.WEBSOCKET_PEER_CERTIFICATE = certificate;
process.env.WEBSOCKET_PEER_KEY = key;
process.env.NODE_EXTRA_CA_CERTS = certificate;

export default defineConfig({
  test: {
    include: ['conformance/websocket.conformance.js'],
    pool: 'forks',
    // One peer process serves every case in turn.
    fileParallelism: false,
  },
});
