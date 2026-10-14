import { afterEach, describe, expect, it, vi } from 'vitest';
import { RelayError } from '../src/errors.js';
import { createCatalogService, createDocumentService } from '../src/media.js';
import { settle, slotTrack } from './helpers/slot-track.js';

const bytes = (...values: number[]) => Uint8Array.from(values);
const sent = (track: ReturnType<typeof slotTrack>) =>
  track.groups.filter((group) => group.finished).map((group) => Array.from(group.objects[0]?.bytes ?? []));

afterEach(() => {
  vi.useRealTimers();
});

describe('catalog service', () => {
  it('sends on the current relay subscription and again on every replacement, not before demand', async () => {
    const track = slotTrack({ demand: false });
    const catalog = createCatalogService(track.writer, bytes(1, 2));
    const stop = new AbortController();
    const serving = catalog.serve({ signal: stop.signal });
    await settle();
    expect(sent(track)).toEqual([]);
    track.demand(true);
    await settle();
    track.demand(true);
    await settle();
    expect(sent(track)).toEqual([
      [1, 2],
      [1, 2],
    ]);
    expect(catalog.sends).toBe(2);
    stop.abort(new Error('done'));
    await expect(serving).rejects.toThrow('done');
  });

  it('copies the document, so later changes to the caller’s bytes do not leak in', async () => {
    const document = bytes(7);
    const track = slotTrack();
    const catalog = createCatalogService(track.writer, document);
    document[0] = 9;
    const stop = new AbortController();
    const serving = catalog.serve({ signal: stop.signal });
    await settle();
    expect(sent(track)).toEqual([[7]]);
    stop.abort();
    await serving.catch(() => {});
  });

  it('retries a failed write after the delay while demand remains, and stops retrying when it goes', async () => {
    vi.useFakeTimers();
    const track = slotTrack();
    const catalog = createCatalogService(track.writer, bytes(1), { retryDelayMs: 500 });
    track.failNextWrite(new Error('reset'));
    const stop = new AbortController();
    const serving = catalog.serve({ signal: stop.signal });
    await settle();
    expect(catalog.failures).toBe(1);
    expect(sent(track)).toEqual([]);
    await vi.advanceTimersByTimeAsync(500);
    expect(sent(track)).toEqual([[1]]);

    track.demand(true);
    track.failNextWrite(new Error('reset'));
    await settle();
    expect(catalog.failures).toBe(2);
    track.demand(false);
    await vi.advanceTimersByTimeAsync(500);
    expect(catalog.sends).toBe(1);
    stop.abort();
    await serving.catch(() => {});
  });

  it('resolves when the track ends quietly and rejects with the failure that ended it under a write', async () => {
    const quiet = slotTrack({ demand: false });
    const quietServing = createCatalogService(quiet.writer, bytes(1)).serve();
    quiet.writer.close();
    await expect(quietServing).resolves.toBeUndefined();

    const track = slotTrack();
    track.stall();
    const serving = createCatalogService(track.writer, bytes(1)).serve();
    await settle();
    track.disconnect(new RelayError('CLOSED', 'Connection closed', 'connection'));
    await expect(serving).rejects.toMatchObject({ code: 'CLOSED' });
  });

  it('refuses a bad document or delay before touching the track', () => {
    const track = slotTrack();
    expect(() => createCatalogService(track.writer, 'catalog' as never)).toThrow(TypeError);
    expect(() => createCatalogService(track.writer, bytes(1), { retryDelayMs: -1 })).toThrow(RangeError);
    expect(() => createCatalogService(track.writer, bytes(1), { retryDelayMs: Number.NaN })).toThrow(TypeError);
  });
});

describe('document service', () => {
  it('sends nothing before the first document, then the latest on demand and on every update', async () => {
    const track = slotTrack();
    const service = createDocumentService(track.writer);
    const stop = new AbortController();
    const serving = service.serve({ signal: stop.signal });
    await settle();
    expect(sent(track)).toEqual([]);
    service.update(bytes(1));
    await settle();
    service.update(bytes(2));
    await settle();
    expect(sent(track)).toEqual([[1], [2]]);
    expect(Array.from(service.document ?? [])).toEqual([2]);
    track.demand(true);
    await settle();
    expect(sent(track)).toEqual([[1], [2], [2]]);
    stop.abort();
    await serving.catch(() => {});
  });

  it('only replaces the document without demand, and sends an update made during a write after it', async () => {
    const track = slotTrack({ demand: false });
    const service = createDocumentService(track.writer, bytes(1));
    const stop = new AbortController();
    const serving = service.serve({ signal: stop.signal });
    service.update(bytes(2));
    await settle();
    expect(sent(track)).toEqual([]);
    track.stall();
    track.demand(true);
    await settle();
    service.update(bytes(3));
    track.release();
    await settle();
    expect(sent(track)).toEqual([[2], [3]]);
    stop.abort();
    await serving.catch(() => {});
  });

  it('retries with the latest document', async () => {
    vi.useFakeTimers();
    const track = slotTrack();
    const service = createDocumentService(track.writer, bytes(1), { retryDelayMs: 100 });
    track.failNextWrite(new Error('reset'));
    const stop = new AbortController();
    const serving = service.serve({ signal: stop.signal });
    await settle();
    service.update(bytes(2));
    await vi.advanceTimersByTimeAsync(100);
    expect(sent(track)).toEqual([[2]]);
    expect(service.failures).toBe(1);
    stop.abort();
    await serving.catch(() => {});
  });
});
