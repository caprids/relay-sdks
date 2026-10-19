/**
 * Chromium's echo canceller takes only audio that WebRTC plays as its reference, so call audio played straight from an
 * `AudioContext` reaches the far end again through the speakers. The standard workaround routes it through a pair of
 * local `RTCPeerConnection`s into a media element. Firefox and Safari cancel echo from all page output and need none.
 */
import type { AudioPlayerNode } from './audio-player.js';

/** The part of an `AudioContext` the loopback uses. */
export interface EchoCancellationContext {
  /** Create the node whose stream the loopback sends. */
  createMediaStreamDestination(): AudioPlayerNode & {
    /** The `MediaStream` carrying what the node receives. */
    readonly stream: unknown;
    /** Channels the node mixes to; the loopback sets 2. */
    channelCount: number;
  };
}

/** Options for {@link createEchoCancellationLoopback}. */
export interface EchoCancellationLoopbackOptions {
  /** The output device, as for `HTMLMediaElement.setSinkId`. Default: the system's. */
  sinkId?: string;
  /** How long to wait for the loopback to connect, in milliseconds. Default 5000. */
  timeoutMs?: number;
}

/** A running loopback. Close it, and connect the node to the context's destination, to play directly again. */
export interface EchoCancellationLoopback {
  /** Play on another output device. */
  setSinkId(sinkId: string): Promise<void>;
  /** Disconnect the node from the loopback and close both connections. */
  close(): void;
}

interface Track {
  readonly kind: string;
}
interface Stream {
  getAudioTracks(): Track[];
}
interface Candidate {
  readonly candidate: unknown;
}
interface Description {
  type: string;
  sdp?: string | undefined;
}
interface PeerConnection {
  onicecandidate: ((event: Candidate) => void) | null;
  ontrack: ((event: { track: Track }) => void) | null;
  readonly connectionState: string;
  addEventListener(type: 'connectionstatechange', listener: () => void): void;
  addTrack(track: Track, stream: Stream): unknown;
  addIceCandidate(candidate: unknown): Promise<void>;
  createOffer(): Promise<Description>;
  createAnswer(): Promise<Description>;
  setLocalDescription(description: Description): Promise<void>;
  setRemoteDescription(description: Description): Promise<void>;
  close(): void;
}
interface MediaElement {
  autoplay: boolean;
  srcObject: unknown;
  play(): Promise<void>;
  setSinkId?(sinkId: string): Promise<void>;
}
interface Globals {
  RTCPeerConnection: new () => PeerConnection;
  MediaStream: new (tracks: Track[]) => unknown;
  Audio: new () => MediaElement;
}

// Full-band stereo at the highest bitrate: this hop never leaves the machine, so its Opus pass should be transparent.
const transparent = (sdp: string | undefined, offer: boolean) =>
  sdp?.replace(
    /(a=fmtp:\d+ .*useinbandfec=1)/,
    `$1;stereo=1;${offer ? 'sprop-stereo=1;' : ''}maxaveragebitrate=510000`
  );

function within<T>(promise: Promise<T>, ms: number, what: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  return Promise.race([
    promise,
    new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`The echo-cancellation loopback ${what} within ${ms} ms`)), ms);
    }),
  ]).finally(() => clearTimeout(timer));
}

/**
 * Play `node` through a local WebRTC loopback so Chromium's echo canceller hears it. The node is connected to the
 * loopback only once it is up; disconnect it from the context's destination yourself, or it plays twice. Rejects if
 * the loopback cannot connect. It adds the WebRTC receiver's own buffering, which no API reports as output latency:
 * set the audio player's `outputDelayUs` if video must stay in sync with it.
 */
export async function createEchoCancellationLoopback(
  context: EchoCancellationContext,
  node: AudioPlayerNode,
  options: EchoCancellationLoopbackOptions = {}
): Promise<EchoCancellationLoopback> {
  const { RTCPeerConnection, MediaStream, Audio } = globalThis as unknown as Globals;
  const timeoutMs = options.timeoutMs ?? 5_000;
  const destination = context.createMediaStreamDestination();
  destination.channelCount = 2;
  const stream = destination.stream as Stream;
  const sender = new RTCPeerConnection();
  const receiver = new RTCPeerConnection();
  const element = new Audio();
  element.autoplay = true;
  const close = () => {
    element.srcObject = null;
    sender.close();
    receiver.close();
  };
  try {
    sender.onicecandidate = ({ candidate }) => {
      if (candidate) receiver.addIceCandidate(candidate).catch(() => {});
    };
    receiver.onicecandidate = ({ candidate }) => {
      if (candidate) sender.addIceCandidate(candidate).catch(() => {});
    };
    const received = new Promise<Track>((resolve) => {
      receiver.ontrack = ({ track }) => resolve(track);
    });
    const connected = new Promise<void>((resolve, reject) => {
      const check = () => {
        if (receiver.connectionState === 'connected') resolve();
        else if (receiver.connectionState === 'failed' || receiver.connectionState === 'closed') {
          reject(new Error(`The echo-cancellation loopback ${receiver.connectionState}`));
        }
      };
      receiver.addEventListener('connectionstatechange', check);
      check();
    });
    for (const track of stream.getAudioTracks()) sender.addTrack(track, stream);
    const offer = await sender.createOffer();
    const offerSdp = { type: 'offer', sdp: transparent(offer.sdp, true) };
    await sender.setLocalDescription(offerSdp);
    await receiver.setRemoteDescription(offerSdp);
    const answer = await receiver.createAnswer();
    const answerSdp = { type: 'answer', sdp: transparent(answer.sdp, false) };
    await receiver.setLocalDescription(answerSdp);
    await sender.setRemoteDescription(answerSdp);
    const track = await within(received, timeoutMs, 'received no track');
    await within(connected, timeoutMs, 'did not connect');
    element.srcObject = new MediaStream([track]);
    if (options.sinkId) await element.setSinkId?.(options.sinkId);
    // Autoplay may still be blocked; the context itself needed a gesture, so this one usually succeeds.
    await element.play().catch(() => {});
  } catch (error) {
    close();
    throw error;
  }
  node.connect(destination);
  let closed = false;
  return {
    async setSinkId(sinkId) {
      if (!element.setSinkId) throw new Error('This browser cannot choose an output device for a media element');
      await element.setSinkId(sinkId);
    },
    close() {
      if (closed) return;
      closed = true;
      try {
        node.disconnect(destination);
      } catch {
        // Already disconnected.
      }
      close();
    },
  };
}
