// Forwards the first channel of each input to the page, tagged with the context frame of its first sample.
// Input 0 is the microphone under test, input 1 the optional reference microphone. An unconnected input has no
// channels and posts nothing.
const BATCH = 1024;

class CaptureProcessor extends AudioWorkletProcessor {
  constructor() {
    super();
    this.slots = [0, 1].map(() => ({ frame: -1, data: new Float32Array(BATCH), length: 0 }));
  }

  process(inputs) {
    for (let slot = 0; slot < this.slots.length; slot++) {
      const channel = inputs[slot]?.[0];
      const buffer = this.slots[slot];
      if (!channel) {
        buffer.length = 0;
        continue;
      }
      // A gap (the input was disconnected) starts a new batch so frames stay contiguous within one.
      if (buffer.length > 0 && buffer.frame + buffer.length !== currentFrame) this.flush(slot);
      if (buffer.length === 0) buffer.frame = currentFrame;
      buffer.data.set(channel, buffer.length);
      buffer.length += channel.length;
      if (buffer.length + channel.length > BATCH) this.flush(slot);
    }
    return true;
  }

  flush(slot) {
    const buffer = this.slots[slot];
    if (buffer.length === 0) return;
    const data = buffer.data.slice(0, buffer.length);
    this.port.postMessage({ slot, frame: buffer.frame, data }, [data.buffer]);
    buffer.length = 0;
  }
}

registerProcessor('capture', CaptureProcessor);
