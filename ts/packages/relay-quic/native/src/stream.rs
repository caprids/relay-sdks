//! Stream halves. Each holds its quinn stream behind an async mutex, which a write or a read keeps
//! for as long as it waits. A reset or a stop first cancels a sticky token that every waiting
//! operation selects on, so the waiter lets go of the lock at once and the frame goes out
//! immediately (TA-16, TA-18). The token is sticky: an operation that starts after the reset fails
//! at once instead of missing a one-shot wake-up.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use napi::bindgen_prelude::*;
use napi_derive::napi;
use quinn::VarInt;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::failure::{self, Failure};

type Stopped = Pin<Box<dyn Future<Output = std::result::Result<Option<VarInt>, quinn::StoppedError>> + Send>>;

/// The sending half of a stream.
#[napi]
pub struct SendStream {
    inner: Arc<Mutex<quinn::SendStream>>,
    aborted: CancellationToken,
    /// Taken by the first `stopped()`: quinn's stop watch does not borrow the stream, so it never
    /// waits on the lock a blocked write holds.
    stopped: std::sync::Mutex<Option<Stopped>>,
}

impl SendStream {
    pub fn new(stream: quinn::SendStream) -> Self {
        let stopped: Stopped = Box::pin(stream.stopped());
        Self {
            inner: Arc::new(Mutex::new(stream)),
            aborted: CancellationToken::new(),
            stopped: std::sync::Mutex::new(Some(stopped)),
        }
    }

    async fn lock(&self) -> Option<tokio::sync::MutexGuard<'_, quinn::SendStream>> {
        tokio::select! {
            biased;
            _ = self.aborted.cancelled() => None,
            guard = self.inner.lock() => Some(guard),
        }
    }
}

#[napi]
impl SendStream {
    /// Write all of `data`. Resolves once quinn has taken every byte (TA-14): it waits while flow
    /// control or the send buffer holds the stream, and gives up at once if the stream is reset.
    #[napi]
    pub async fn write(&self, data: Uint8Array) -> Option<Failure> {
        let Some(mut stream) = self.lock().await else {
            return Some(Failure::closed());
        };
        tokio::select! {
            biased;
            _ = self.aborted.cancelled() => Some(Failure::closed()),
            result = stream.write_all(&data) => result.err().map(failure::write),
        }
    }

    /// Send the FIN, then resolve once the peer has acknowledged every byte and the FIN (TA-15).
    /// Fails if the peer stops the stream first, the session ends, or the stream is reset.
    #[napi]
    pub async fn finish(&self) -> Option<Failure> {
        let acknowledged = {
            let Some(mut stream) = self.lock().await else {
                return Some(Failure::closed());
            };
            if stream.finish().is_err() {
                return Some(Failure::closed());
            }
            stream.stopped()
        };
        tokio::select! {
            biased;
            _ = self.aborted.cancelled() => Some(Failure::closed()),
            result = acknowledged => match result {
                Ok(None) => None,
                Ok(Some(code)) => Some(Failure::stream("stopped", code)),
                Err(error) => Some(failure::stopped(error)),
            },
        }
    }

    /// RESET_STREAM with `code`, now (TA-16): a write blocked on flow control and a FIN waiting for
    /// acknowledgement both fail at once. Harmless once the stream is complete.
    #[napi]
    pub async fn reset(&self, code: u32) {
        self.aborted.cancel();
        let _ = self.inner.lock().await.reset(VarInt::from_u32(code));
    }

    /// Resolves with the peer's STOP_SENDING (TA-20), answered with a RESET_STREAM carrying its code
    /// (RFC 9000 §3.5), or with the session's end (TA-12), even while no write or finish is in
    /// flight; with nothing once the peer has acknowledged the whole stream, or once this end resets
    /// it. Only the first call watches; later ones resolve with nothing at once.
    #[napi]
    pub async fn stopped(&self) -> Option<Failure> {
        let watch = self.stopped.lock().ok()?.take()?;
        let result = tokio::select! {
            biased;
            // quinn wakes no stop watcher when a reset is acknowledged, so without this a reset
            // stream's watcher would stay pending until the session ends.
            _ = self.aborted.cancelled() => return None,
            result = watch => result,
        };
        match result {
            Ok(Some(code)) => {
                let _ = self.inner.lock().await.reset(code);
                Some(Failure::stream("stopped", code))
            }
            Ok(None) => None,
            Err(error) => Some(failure::stopped(error)),
        }
    }
}

/// The result of one read: a chunk, the FIN, or a failure.
#[napi(object)]
pub struct ReadResult {
    /// Bytes, when the read delivered some.
    pub chunk: Option<Buffer>,
    /// The peer's FIN: nothing more will arrive.
    pub fin: bool,
    /// Why the read failed.
    pub failure: Option<Failure>,
}

/// The receiving half of a stream.
#[napi]
pub struct RecvStream {
    inner: Arc<Mutex<quinn::RecvStream>>,
    stopped: CancellationToken,
}

impl RecvStream {
    pub fn new(stream: quinn::RecvStream) -> Self {
        Self {
            inner: Arc::new(Mutex::new(stream)),
            stopped: CancellationToken::new(),
        }
    }
}

#[napi]
impl RecvStream {
    /// Read up to `max` bytes, in order. A peer reset fails the read with its code (TA-19); bytes
    /// already read stay read.
    #[napi]
    pub async fn read(&self, max: u32) -> ReadResult {
        let closed = || ReadResult {
            chunk: None,
            fin: false,
            failure: Some(Failure::closed()),
        };
        let mut stream = tokio::select! {
            biased;
            _ = self.stopped.cancelled() => return closed(),
            guard = self.inner.lock() => guard,
        };
        tokio::select! {
            biased;
            _ = self.stopped.cancelled() => closed(),
            result = stream.read_chunk(max as usize, true) => match result {
                Ok(Some(chunk)) => ReadResult { chunk: Some(chunk.bytes.to_vec().into()), fin: false, failure: None },
                Ok(None) => ReadResult { chunk: None, fin: true, failure: None },
                Err(error) => ReadResult { chunk: None, fin: false, failure: Some(failure::read(error)) },
            },
        }
    }

    /// STOP_SENDING with `code`, now (TA-18): a pending read ends, and nothing more is delivered.
    #[napi]
    pub async fn stop(&self, code: u32) {
        self.stopped.cancel();
        let _ = self.inner.lock().await.stop(VarInt::from_u32(code));
    }
}

/// A bidirectional stream, taken apart into its two halves once.
#[napi]
pub struct BiStream {
    send: Option<SendStream>,
    recv: Option<RecvStream>,
}

impl BiStream {
    pub fn new(send: quinn::SendStream, recv: quinn::RecvStream) -> Self {
        Self {
            send: Some(SendStream::new(send)),
            recv: Some(RecvStream::new(recv)),
        }
    }
}

#[napi]
impl BiStream {
    /// The sending half; once.
    #[napi]
    pub fn take_send(&mut self) -> Result<SendStream> {
        self.send
            .take()
            .ok_or_else(|| Error::new(Status::GenericFailure, "the sending half was already taken"))
    }

    /// The receiving half; once.
    #[napi]
    pub fn take_recv(&mut self) -> Result<RecvStream> {
        self.recv
            .take()
            .ok_or_else(|| Error::new(Status::GenericFailure, "the receiving half was already taken"))
    }
}
