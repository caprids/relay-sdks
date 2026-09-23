/** Codec derivation follows Video.js avc-codec.ts. Encoders supply the configuration record. */

/** Read profile, constraint flags and level; this inspects the header, not SPS bitstream semantics. */
export function avcCodecString(avcc: Uint8Array): string {
  if (avcc.length < 4 || avcc[0] !== 1) throw new Error('not an AVCDecoderConfigurationRecord');
  return `avc1.${Array.from(avcc.subarray(1, 4), (byte) => byte.toString(16).padStart(2, '0').toUpperCase()).join('')}`;
}
