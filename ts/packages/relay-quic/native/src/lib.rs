//! The native QUIC dialing end behind `@mux/relay-quic` (`spec/transport-adapter.md`): MOQT goes
//! straight onto a QUIC connection, and ALPN carries the protocol. The JS layer (`../src/`) turns
//! this into a `MoqtTransport`; nothing here knows MOQT.
//!
//! Nothing fallible throws once a dial has started: every call resolves with a [`Failure`], or
//! with nothing on success (`failure.rs`). Only bad options throw, when the [`Client`] is built.

mod failure;
mod stream;
mod tls;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures_util::FutureExt;
use napi::bindgen_prelude::*;
use napi_derive::napi;
use tokio_util::sync::CancellationToken;

use failure::Failure;
use stream::{BiStream, RecvStream, SendStream};

/// How often an idle connection sends a PING, well inside the idle timeout on both ends.
const KEEP_ALIVE: Duration = Duration::from_secs(4);
/// How long the connection may hear nothing before it is lost (TA-9).
const IDLE_TIMEOUT_MS: u32 = 30_000;
/// How long `closed()` waits for the endpoint to put the CONNECTION_CLOSE on the wire.
const CLOSE_FLUSH: Duration = Duration::from_secs(1);

/// What a [`Client`] trusts.
#[napi(object)]
pub struct ClientOptions {
    /// Extra trust anchors, PEM, added to the platform's roots.
    pub ca: Vec<String>,
    /// SHA-256 hashes of accepted end-entity certificates; when any are given they replace chain
    /// validation.
    pub server_certificate_hashes: Vec<Uint8Array>,
}

/// One `nativeQuicTransport`'s TLS configuration. Every dial gets its own endpoint and socket.
#[napi]
pub struct Client {
    tls: Arc<rustls::ClientConfig>,
}

#[napi]
impl Client {
    /// Throws when a `ca` entry is not a PEM certificate or a hash is not 32 bytes.
    #[napi(constructor)]
    pub fn new(options: ClientOptions) -> Result<Self> {
        let hashes: Vec<Vec<u8>> = options
            .server_certificate_hashes
            .iter()
            .map(|hash| hash.to_vec())
            .collect();
        let tls = tls::client_config(&options.ca, &hashes).map_err(|error| Error::new(Status::InvalidArg, error))?;
        Ok(Self { tls: Arc::new(tls) })
    }

    /// Start dialing `host:port`, offering `protocols` as ALPN in order (TA-5). `server_name` is
    /// what TLS checks and, for a DNS name, what SNI carries (TA-3).
    #[napi]
    pub fn dial(&self, host: String, port: u32, server_name: String, protocols: Vec<String>) -> Result<Dial> {
        let port =
            u16::try_from(port).map_err(|_| Error::new(Status::InvalidArg, format!("port {port} is out of range")))?;
        let mut tls = (*self.tls).clone();
        tls.alpn_protocols = protocols.into_iter().map(String::into_bytes).collect();
        Ok(Dial {
            tls,
            host,
            port,
            server_name,
            cancelled: CancellationToken::new(),
        })
    }
}

/// A dial in progress: [`Dial::ready`] runs it, and [`Dial::cancel`] stops it (TA-11).
#[napi]
pub struct Dial {
    tls: rustls::ClientConfig,
    host: String,
    port: u16,
    server_name: String,
    cancelled: CancellationToken,
}

#[napi]
impl Dial {
    /// Resolve the host, then run the QUIC handshake. Resolves with the session once the peer has
    /// selected one of the offered protocols, which rustls requires of a QUIC handshake (TA-7), or
    /// with the failure. Call it once.
    #[napi]
    pub async fn ready(&self) -> Either<Session, Failure> {
        let cancelled = || Either::B(Failure::session("cancelled", None, "the dial was cancelled"));
        tokio::select! {
            biased;
            _ = self.cancelled.cancelled() => cancelled(),
            result = self.connect() => match result {
                Ok(session) => Either::A(session),
                Err(failure) => Either::B(failure),
            },
        }
    }

    /// Stop the dial: `ready` fails with `cancelled`, and dropping the handshake releases the socket.
    #[napi]
    pub fn cancel(&self) {
        self.cancelled.cancel();
    }
}

impl Dial {
    async fn connect(&self) -> std::result::Result<Session, Failure> {
        let dial_failure = |reason: String| Failure::session("dial", None, reason);
        let address: SocketAddr = tokio::net::lookup_host((self.host.as_str(), self.port))
            .await
            .map_err(|error| dial_failure(format!("could not resolve {}: {error}", self.host)))?
            .next()
            .ok_or_else(|| dial_failure(format!("{} has no addresses", self.host)))?;

        let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(self.tls.clone())
            .map_err(|error| dial_failure(error.to_string()))?;
        let mut client = quinn::ClientConfig::new(Arc::new(crypto));
        let mut transport = quinn::TransportConfig::default();
        transport.keep_alive_interval(Some(KEEP_ALIVE));
        transport.max_idle_timeout(Some(quinn::IdleTimeout::from(quinn::VarInt::from_u32(IDLE_TIMEOUT_MS))));
        client.transport_config(Arc::new(transport));

        let bind: SocketAddr = if address.is_ipv6() { "[::]:0" } else { "0.0.0.0:0" }
            .parse()
            .expect("a socket address");
        let mut endpoint = quinn::Endpoint::client(bind)
            .map_err(|error| dial_failure(format!("could not bind a UDP socket: {error}")))?;
        endpoint.set_default_client_config(client);

        let connecting = endpoint
            .connect(address, &self.server_name)
            .map_err(|error| dial_failure(error.to_string()))?;
        let connection = connecting.await.map_err(|error| failure::connection(&error))?;
        let protocol = connection
            .handshake_data()
            .and_then(|data| data.downcast::<quinn::crypto::rustls::HandshakeData>().ok())
            .and_then(|data| data.protocol)
            .map(|protocol| String::from_utf8_lossy(&protocol).into_owned());
        Ok(Session {
            connection,
            endpoint: std::sync::Mutex::new(Some(endpoint)),
            protocol,
        })
    }
}

/// An established connection.
#[napi]
pub struct Session {
    connection: quinn::Connection,
    /// Dropped once `closed()` has settled, so the socket goes with the session (TA-26).
    endpoint: std::sync::Mutex<Option<quinn::Endpoint>>,
    protocol: Option<String>,
}

#[napi]
impl Session {
    /// The ALPN identifier the peer selected (TA-8).
    #[napi(getter)]
    pub fn protocol(&self) -> Option<String> {
        self.protocol.clone()
    }

    /// Open a unidirectional stream now, or fail at once with `credit` when the peer's stream limit
    /// is used up (TA-21).
    #[napi]
    pub fn open_uni(&self) -> Either<SendStream, Failure> {
        match self.connection.open_uni().now_or_never() {
            Some(Ok(stream)) => Either::A(SendStream::new(stream)),
            Some(Err(error)) => Either::B(failure::connection(&error)),
            None => Either::B(Failure::credit()),
        }
    }

    /// Open a bidirectional stream now, or fail at once with `credit` (TA-21).
    #[napi]
    pub fn open_bi(&self) -> Either<BiStream, Failure> {
        match self.connection.open_bi().now_or_never() {
            Some(Ok((send, recv))) => Either::A(BiStream::new(send, recv)),
            Some(Err(error)) => Either::B(failure::connection(&error)),
            None => Either::B(Failure::credit()),
        }
    }

    /// The next unidirectional stream the peer opens, or nothing once the session is over (TA-12).
    #[napi]
    pub async fn accept_uni(&self) -> Option<RecvStream> {
        self.connection.accept_uni().await.ok().map(RecvStream::new)
    }

    /// The next bidirectional stream the peer opens, or nothing once the session is over (TA-12).
    #[napi]
    pub async fn accept_bi(&self) -> Option<BiStream> {
        self.connection
            .accept_bi()
            .await
            .ok()
            .map(|(send, recv)| BiStream::new(send, recv))
    }

    /// Close the connection with an application CONNECTION_CLOSE carrying `code` and `reason` (TA-10).
    #[napi]
    pub fn close(&self, code: u32, reason: String) {
        self.connection.close(quinn::VarInt::from_u32(code), reason.as_bytes());
    }

    /// Resolves once the connection is over, with why. The endpoint then gets up to a second to send
    /// the CONNECTION_CLOSE, and is dropped, so nothing of the session holds the process (TA-26).
    #[napi]
    pub async fn closed(&self) -> Failure {
        let error = self.connection.closed().await;
        let endpoint = self.endpoint.lock().ok().and_then(|mut endpoint| endpoint.take());
        if let Some(endpoint) = endpoint {
            let _ = tokio::time::timeout(CLOSE_FLUSH, endpoint.wait_idle()).await;
        }
        failure::connection(&error)
    }
}
