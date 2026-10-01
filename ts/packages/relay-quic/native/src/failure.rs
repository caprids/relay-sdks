//! Failures as data. Nothing fallible throws into JS: each call resolves with a [`Failure`], or with
//! nothing on success, so the JS layer builds the contract's error shapes (TA-22) from fields
//! rather than by parsing messages.

use napi_derive::napi;
use quinn::{ConnectionError, ReadError, StoppedError, VarInt, WriteError};

/// Why an operation failed.
#[napi(object)]
pub struct Failure {
    /// TA-22's `source`: `stream` when the peer reset or stopped the stream, `session` once the
    /// connection is over or the dial failed. `credit` (TA-21) and `closed` (this end already ended
    /// the stream) are the adapter's own and never reach the core as TA-22 errors.
    pub source: String,
    /// What happened. For `stream`: `reset` or `stopped`. For `session`: `application` (an
    /// application CONNECTION_CLOSE, 0x1d), `transport` (a QUIC-layer one, 0x1c, or a local
    /// protocol or TLS failure), `local`, `timeout`, `stateless-reset`, `cancelled` or `dial`.
    pub kind: String,
    /// The peer's code in decimal: a QUIC varint has 62 bits, more than a JS number holds exactly.
    pub code: Option<String>,
    /// The peer's reason phrase, or a diagnostic.
    pub reason: String,
}

impl Failure {
    pub fn stream(kind: &str, code: VarInt) -> Self {
        Self {
            source: "stream".into(),
            kind: kind.into(),
            code: Some(code.into_inner().to_string()),
            reason: String::new(),
        }
    }

    pub fn session(kind: &str, code: Option<u64>, reason: impl Into<String>) -> Self {
        Self {
            source: "session".into(),
            kind: kind.into(),
            code: code.map(|code| code.to_string()),
            reason: reason.into(),
        }
    }

    /// An operation on a stream this end already reset, stopped or finished.
    pub fn closed() -> Self {
        Self {
            source: "closed".into(),
            kind: "closed".into(),
            code: None,
            reason: "the stream is closed".into(),
        }
    }

    /// The peer's stream limit is used up (TA-21).
    pub fn credit() -> Self {
        Self {
            source: "credit".into(),
            kind: "credit".into(),
            code: None,
            reason: "no stream credit: the peer's MAX_STREAMS limit is used up".into(),
        }
    }
}

fn lossy(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// The session failure a connection error stands for.
pub fn connection(error: &ConnectionError) -> Failure {
    match error {
        ConnectionError::ApplicationClosed(close) => {
            Failure::session("application", Some(close.error_code.into_inner()), lossy(&close.reason))
        }
        ConnectionError::ConnectionClosed(close) => {
            Failure::session("transport", Some(u64::from(close.error_code)), lossy(&close.reason))
        }
        ConnectionError::LocallyClosed => Failure::session("local", None, "the session was closed"),
        ConnectionError::TimedOut => Failure::session("timeout", None, "the connection timed out"),
        ConnectionError::Reset => Failure::session("stateless-reset", None, "the peer reset the connection"),
        other => Failure::session("transport", None, other.to_string()),
    }
}

pub fn write(error: WriteError) -> Failure {
    match error {
        WriteError::Stopped(code) => Failure::stream("stopped", code),
        WriteError::ConnectionLost(error) => connection(&error),
        WriteError::ClosedStream => Failure::closed(),
        WriteError::ZeroRttRejected => Failure::session("transport", None, "0-RTT was rejected"),
    }
}

pub fn read(error: ReadError) -> Failure {
    match error {
        ReadError::Reset(code) => Failure::stream("reset", code),
        ReadError::ConnectionLost(error) => connection(&error),
        ReadError::ClosedStream => Failure::closed(),
        ReadError::ZeroRttRejected => Failure::session("transport", None, "0-RTT was rejected"),
        ReadError::IllegalOrderedRead => Failure::closed(),
    }
}

pub fn stopped(error: StoppedError) -> Failure {
    match error {
        StoppedError::ConnectionLost(error) => connection(&error),
        StoppedError::ZeroRttRejected => Failure::session("transport", None, "0-RTT was rejected"),
    }
}
