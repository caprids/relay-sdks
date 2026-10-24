import { createHash } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { OPUS_DECODER_WASM_BASE64, OPUS_DECODER_WASM_SHA256 } from '../packages/moq-media/src/opus-decoder-wasm.js';

// The audio player's loss concealment runs libopus compiled to WebAssembly, committed as base64 by
// packages/moq-media/scripts/build-opus-wasm.sh. These checks keep the committed bytes the ones the script recorded.
describe('the WebAssembly Opus decoder', () => {
  const bytes = Buffer.from(OPUS_DECODER_WASM_BASE64, 'base64');

  it('is the module build-opus-wasm.sh recorded', () => {
    expect(createHash('sha256').update(bytes).digest('hex')).toBe(OPUS_DECODER_WASM_SHA256);
  });

  it('imports nothing and exports the decoder surface', () => {
    const module = new WebAssembly.Module(bytes);
    expect(WebAssembly.Module.imports(module)).toEqual([]);
    expect(
      WebAssembly.Module.exports(module)
        .map((entry) => entry.name)
        .sort(),
    ).toEqual(
      [
        'memory',
        'mux_opus_create',
        'mux_opus_decode',
        'mux_opus_destroy',
        'mux_opus_free',
        'mux_opus_has_lbrr',
        'mux_opus_malloc',
        'mux_opus_reset',
      ].sort(),
    );
  });
});
