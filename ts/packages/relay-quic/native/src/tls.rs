//! Trust: the platform's roots plus any `ca` anchors, or W3C `serverCertificateHashes` semantics,
//! where a SHA-256 of the end-entity certificate replaces chain validation. The handshake signature
//! is verified either way. The 14-day validity limit browsers apply to hashed certificates is not.

use std::sync::{Arc, OnceLock};

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{verify_tls12_signature, verify_tls13_signature, CryptoProvider};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};

fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// The platform's trust anchors, loaded once per process.
fn platform_roots() -> &'static [CertificateDer<'static>] {
    static ROOTS: OnceLock<Vec<CertificateDer<'static>>> = OnceLock::new();
    ROOTS.get_or_init(|| rustls_native_certs::load_native_certs().certs)
}

/// The TLS configuration every dial of one `nativeQuicTransport` starts from; each dial sets its
/// own ALPN list on a copy.
pub fn client_config(ca: &[String], hashes: &[Vec<u8>]) -> Result<rustls::ClientConfig, String> {
    let provider = provider();
    // QUIC is TLS 1.3 only (RFC 9001 §4.2).
    let builder = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|error| error.to_string())?;

    if !hashes.is_empty() {
        if let Some(hash) = hashes.iter().find(|hash| hash.len() != 32) {
            return Err(format!(
                "a serverCertificateHashes value must be a 32-byte SHA-256, got {} bytes",
                hash.len()
            ));
        }
        let verifier = HashVerifier {
            hashes: hashes.to_vec(),
            provider,
        };
        return Ok(builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(verifier))
            .with_no_client_auth());
    }

    let mut roots = rustls::RootCertStore::empty();
    // A platform root rustls cannot parse is skipped, as browsers skip one they cannot use.
    roots.add_parsable_certificates(platform_roots().iter().cloned());
    for pem in ca {
        let mut added = 0;
        for certificate in CertificateDer::pem_slice_iter(pem.as_bytes()) {
            let certificate = certificate.map_err(|error| format!("ca: {error}"))?;
            roots.add(certificate).map_err(|error| format!("ca: {error}"))?;
            added += 1;
        }
        if added == 0 {
            return Err("ca: no PEM certificate found".into());
        }
    }
    Ok(builder.with_root_certificates(roots).with_no_client_auth())
}

/// Accepts exactly the end-entity certificates whose SHA-256 is one of `hashes`.
#[derive(Debug)]
struct HashVerifier {
    hashes: Vec<Vec<u8>>,
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for HashVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let digest = ring::digest::digest(&ring::digest::SHA256, end_entity.as_ref());
        if self.hashes.iter().any(|hash| hash.as_slice() == digest.as_ref()) {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::ApplicationVerificationFailure,
            ))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(
            message,
            certificate,
            signature,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            certificate,
            signature,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider.signature_verification_algorithms.supported_schemes()
    }
}
