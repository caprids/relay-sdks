/** MSF source composition, adapted from Video.js parse-source.ts. */
import { CATALOG_TRACK } from './loc.js';

const LITERAL_CHAR = /^[a-zA-Z0-9_]$/;
/** Encode one namespace field or track name into the MSF string form. */
export function encodeNameComponent(value: string): string {
  if (/[\uD800-\uDFFF]/u.test(value)) throw new Error('name components must be valid Unicode');
  let out = '';

  for (const byte of new TextEncoder().encode(value)) {
    const char = String.fromCharCode(byte);

    out += LITERAL_CHAR.test(char) ? char : `.${byte.toString(16).padStart(2, '0')}`;
  }

  return out;
}

export function encodeNamespaceName(namespace: readonly string[], trackName: string): string {
  return `${namespace.map(encodeNameComponent).join('-')}--${encodeNameComponent(trackName)}`;
}

export interface ComposeMoqSourceOptions {
  trackName?: string;
  token?: string;
}
export function composeMoqSource(
  origin: string,
  namespace: readonly string[],
  options: ComposeMoqSourceOptions = {}
): string {
  const authority = /^(https|moqt):\/\/([^/?#]+)/i.exec(origin)?.[2];
  // biome-ignore lint/suspicious/noControlCharactersInRegex: Reject invalid URL whitespace and controls before parsing.
  if (!authority || /[\u0000-\u0020\u007f\\]/.test(origin))
    throw new Error('relay URL must be an absolute https:// or moqt:// URL');
  if (authority.includes('@')) throw new Error('relay URL must not carry credentials');
  if (origin.includes('#')) throw new Error('relay URL must not carry a fragment');
  const parsed = new URL(origin);
  if (!parsed.hostname || parsed.username || parsed.password)
    throw new Error('relay URL must have a host and no credentials');
  if (namespace.length === 0 || namespace.includes('')) throw new Error('namespace fields must be non-empty');
  const trackName = options.trackName ?? CATALOG_TRACK;
  if (!trackName) throw new Error('track name is empty');
  // Preserve the supplied authority, path and query, just as Python's urlsplit does.
  const base = origin
    .replace(/^(https|moqt):/i, 'moqt:')
    .replace(/^([^?]*)\?$/, '$1')
    .replace(/^(moqt:\/\/[^/?]+)(?=\?|$)/, '$1/');
  const token = options.token
    ? `&c4m=${encodeURIComponent(options.token).replace(/[!'()*]/g, (char) => `%${char.charCodeAt(0).toString(16).toUpperCase()}`)}`
    : '';
  return `${base}#msf:${encodeNamespaceName(namespace, trackName)}${token}`;
}
