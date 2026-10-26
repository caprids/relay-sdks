//! The native QUIC dialing end behind `@caprid/relay-quic` (`spec/transport-adapter.md`): MOQT goes
//! straight onto a QUIC connection, and ALPN carries the protocol. The JS layer (`../src/`) turns
//! this into a `MoqtTransport`; nothing here knows MOQT.
//!
//! Nothing fallible throws once a dial has started: every call resolves with a [`Failure`], or
//! with nothing on success (`failure.rs`). Only bad options throw, when the [`Client`] is built.

mod codes;
mod failure;
mod stream;
mod tls;

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures_util::stream::{FuturesUnordered, StreamExt};
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
/// How long a dial waits on one address before it also tries the next (RFC 8305 §5).
const ATTEMPT_STAGGER: Duration = Duration::from_millis(250);
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
    /// Resolve the host and race a handshake per address, RFC 8305 style: the families alternate,
    /// each attempt starts [`ATTEMPT_STAGGER`] after the one before or as soon as it fails, and the
    /// first handshake to complete wins. The others are dropped, and their sockets with them.
    async fn connect(&self) -> std::result::Result<Session, Failure> {
        let addresses: Vec<SocketAddr> = tokio::net::lookup_host((self.host.as_str(), self.port))
            .await
            .map_err(|error| dial_failure(format!("could not resolve {}: {error}", self.host)))?
            .collect();
        let mut waiting: VecDeque<SocketAddr> = interleave_families(addresses).into();
        let client = self.client_config()?;

        let mut attempts = FuturesUnordered::new();
        let mut failure = dial_failure(format!("{} has no addresses", self.host));
        let stagger = tokio::time::sleep(Duration::ZERO);
        tokio::pin!(stagger);
        loop {
            tokio::select! {
                biased;
                Some(result) = attempts.next() => match result {
                    Ok(session) => return Ok(session),
                    Err(error) => {
                        failure = error;
                        // A failed attempt makes way for the next at once.
                        stagger.as_mut().reset(tokio::time::Instant::now());
                    }
                },
                _ = &mut stagger, if !waiting.is_empty() => {
                    let address = waiting.pop_front().expect("an address is waiting");
                    attempts.push(self.attempt(address, client.clone()));
                    stagger.as_mut().reset(tokio::time::Instant::now() + ATTEMPT_STAGGER);
                },
                else => return Err(failure),
            }
        }
    }

    fn client_config(&self) -> std::result::Result<quinn::ClientConfig, Failure> {
        let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(self.tls.clone())
            .map_err(|error| dial_failure(error.to_string()))?;
        let mut client = quinn::ClientConfig::new(Arc::new(crypto));
        let mut transport = quinn::TransportConfig::default();
        transport.keep_alive_interval(Some(KEEP_ALIVE));
        transport.max_idle_timeout(Some(quinn::IdleTimeout::from(quinn::VarInt::from_u32(IDLE_TIMEOUT_MS))));
        client.transport_config(Arc::new(transport));
        Ok(client)
    }

    /// One handshake to `address`, on an endpoint of its own.
    async fn attempt(&self, address: SocketAddr, client: quinn::ClientConfig) -> std::result::Result<Session, Failure> {
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
        let selected = connection
            .handshake_data()
            .and_then(|data| data.downcast::<quinn::crypto::rustls::HandshakeData>().ok())
            .and_then(|data| data.protocol);
        // rustls insists on a selection only when the offer is not empty, so check here too (TA-7).
        let Some(selected) = selected.filter(|selected| self.tls.alpn_protocols.contains(selected)) else {
            connection.close(quinn::VarInt::from_u32(0), b"no offered protocol was selected");
            return Err(dial_failure("the peer selected none of the offered protocols".into()));
        };
        let protocol = Some(String::from_utf8_lossy(&selected).into_owned());
        Ok(Session {
            connection,
            endpoint: std::sync::Mutex::new(Some(endpoint)),
            protocol,
        })
    }
}

fn dial_failure(reason: String) -> Failure {
    Failure::session("dial", None, reason)
}

/// `addresses` in resolver order, but alternating between the families, starting with the first
/// one's (RFC 8305 §4).
fn interleave_families(addresses: Vec<SocketAddr>) -> Vec<SocketAddr> {
    let first_is_v6 = addresses.first().is_some_and(SocketAddr::is_ipv6);
    let (mut first, mut second): (VecDeque<_>, VecDeque<_>) = addresses
        .into_iter()
        .partition(|address| address.is_ipv6() == first_is_v6);
    let mut ordered = Vec::with_capacity(first.len() + second.len());
    while !first.is_empty() || !second.is_empty() {
        ordered.extend(first.pop_front());
        ordered.extend(second.pop_front());
    }
    ordered
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn families_alternate_from_the_first() {
        let address = |text: &str| text.parse::<SocketAddr>().unwrap();
        let ordered = interleave_families(vec![
            address("[::1]:1"),
            address("[::2]:1"),
            address("1.1.1.1:1"),
            address("[::3]:1"),
        ]);
        assert_eq!(
            ordered,
            vec![
                address("[::1]:1"),
                address("1.1.1.1:1"),
                address("[::2]:1"),
                address("[::3]:1")
            ]
        );
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
    /// is used up (TA-21). `send_order` is quinn's stream priority, which like WebTransport's send
    /// order sends a higher value first and defaults to 0.
    #[napi]
    pub fn open_uni(&self, send_order: Option<i32>) -> Either<SendStream, Failure> {
        match self.connection.open_uni().now_or_never() {
            Some(Ok(stream)) => {
                if let Some(order) = send_order {
                    // Fails only on a stream already finished or reset, which a new one is not.
                    let _ = stream.set_priority(order);
                }
                Either::A(SendStream::new(stream))
            }
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
