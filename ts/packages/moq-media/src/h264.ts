/** H.264 format helpers ported from the Relay Python example; codec derivation follows Video.js avc-codec.ts. */

/** Split 3/4-byte Annex B start codes, omitting leading and trailing zero bytes. */
export function* iterAnnexbNalUnits(data: Uint8Array): Generator<Uint8Array> {
  let start: number | undefined;
  let index = 0;
  while (index + 2 < data.length) {
    if (data[index] === 0 && data[index + 1] === 0 && data[index + 2] === 1) {
      if (start !== undefined) {
        let end = index;
        while (end > start && data[end - 1] === 0) end--;
        if (end > start) yield data.slice(start, end);
      }
      index += 3;
      start = index;
    } else {
      index++;
    }
  }
  if (start !== undefined) {
    let end = data.length;
    while (end > start && data[end - 1] === 0) end--;
    if (end > start) yield data.slice(start, end);
  }
}

export function nalUnitType(nal: Uint8Array): number {
  if (nal.length === 0) throw new Error('NAL unit is empty');
  return nal[0]! & 0x1f;
}

function concat(parts: readonly Uint8Array[]): Uint8Array {
  const result = new Uint8Array(parts.reduce((length, part) => length + part.length, 0));
  let offset = 0;
  for (const part of parts) {
    result.set(part, offset);
    offset += part.length;
  }
  return result;
}

export function annexbToAvcc(data: Uint8Array, options: { dropParameterSets?: boolean } = {}): Uint8Array {
  const parts: Uint8Array[] = [];
  for (const nal of iterAnnexbNalUnits(data)) {
    if ((options.dropParameterSets ?? true) && [7, 8, 9].includes(nalUnitType(nal))) continue;
    const length = new Uint8Array(4);
    new DataView(length.buffer).setUint32(0, nal.length);
    parts.push(length, nal);
  }
  return concat(parts);
}

export function extractParameterSets(data: Uint8Array): [Uint8Array[], Uint8Array[]] {
  const sps: Uint8Array[] = [];
  const pps: Uint8Array[] = [];
  for (const nal of iterAnnexbNalUnits(data)) {
    if (nalUnitType(nal) === 7) sps.push(nal);
    if (nalUnitType(nal) === 8) pps.push(nal);
  }
  return [sps, pps];
}

export function buildAvcc(sps: readonly Uint8Array[], pps: readonly Uint8Array[]): Uint8Array {
  if (sps.length === 0 || pps.length === 0) throw new Error('an avcC needs at least one SPS and one PPS');
  if (sps.length > 31 || pps.length > 255) throw new Error('too many SPS or PPS NAL units');
  for (const [units, kind] of [
    [sps, 7],
    [pps, 8],
  ] as const) {
    if (units.some((nal) => nal.length === 0 || nal.length > 65535 || nalUnitType(nal) !== kind)) {
      throw new Error('invalid parameter-set type or length');
    }
  }
  const first = sps[0]!;
  if (first.length < 4) throw new Error('SPS is too short');
  // High-family profiles require SPS-derived chroma/bit-depth extension fields.
  if (sps.some((nal) => nal.length < 4 || ![66, 77, 88].includes(nal[1]!))) {
    throw new Error('avcC construction supports only Baseline, Main, and Extended profiles');
  }
  const parts: Uint8Array[] = [new Uint8Array([1, first[1]!, first[2]!, first[3]!, 0xff, 0xe0 | sps.length])];
  const append = (nal: Uint8Array) => parts.push(new Uint8Array([nal.length >> 8, nal.length & 0xff]), nal);
  for (const nal of sps) append(nal);
  parts.push(new Uint8Array([pps.length]));
  for (const nal of pps) append(nal);
  return concat(parts);
}

/** Read profile, constraint flags and level; this inspects the header, not SPS bitstream semantics. */
export function avcCodecString(avcc: Uint8Array): string {
  if (avcc.length < 4 || avcc[0] !== 1) throw new Error('not an AVCDecoderConfigurationRecord');
  return `avc1.${Array.from(avcc.subarray(1, 4), (byte) => byte.toString(16).padStart(2, '0').toUpperCase()).join('')}`;
}

export class H264Config {
  readonly avcc: Uint8Array;
  constructor(avcc: Uint8Array) {
    this.avcc = avcc.slice();
  }
  static fromAnnexb(data: Uint8Array): H264Config {
    return new H264Config(buildAvcc(...extractParameterSets(data)));
  }
  get codec(): string {
    return avcCodecString(this.avcc);
  }
  toAvcc(data: Uint8Array): Uint8Array {
    return annexbToAvcc(data);
  }
}
