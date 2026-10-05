/**
 * The suite's peer interface (`../README.md`, "The peer") as requests to the Python accepting end, which speaks JSON
 * lines: `call(method, params)` sends one. Shared by the targets whose peer is that process.
 */

/** A peer proxy: every method is a request to the Python accepting end. Bytes cross as arrays of numbers. */
export function createRemotePeer(call) {
  return {
    acceptUni: () => call('acceptUni'),
    acceptBi: () => call('acceptBi'),
    openUni: () => call('openUni'),
    openBi: () => call('openBi'),
    write: (stream, bytes) => call('write', { stream, data: Array.from(bytes) }),
    finish: (stream) => call('finish', { stream }),
    reset: (stream, code, { wire } = {}) => call('reset', { stream, code, wire }),
    stop: (stream, code, { wire } = {}) => call('stop', { stream, code, wire }),
    read: (stream, { bytes, discard } = {}) => call('read', { stream, count: bytes, discard }),
    stopped: (stream) => call('stopped', { stream }),
    wireCode: (stream) => call('wireCode', { stream }),
    close: (code, reason) => call('close', { code, reason }),
    closed: () => call('closed'),
    quicClose: () => call('quicClose'),
    loseConnection: () => call('loseConnection'),
    pause: () => call('pause'),
    resume: () => call('resume'),
    releaseHandshake: () => call('releaseHandshake'),
    observed: () => call('observedDial'),
  };
}
