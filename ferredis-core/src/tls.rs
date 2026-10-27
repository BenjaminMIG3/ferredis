//! Listener TLS via rustls (cert/key PEM, dual plain+TLS).

use anyhow::{anyhow, Context};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::ServerConfig as RustlsServerConfig;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;
use tokio_rustls::TlsAcceptor;

/// Charge un [`TlsAcceptor`] depuis des fichiers PEM certificat + clé privée.
pub fn load_acceptor(cert_path: &Path, key_path: &Path) -> anyhow::Result<TlsAcceptor> {
    load_acceptor_inner(cert_path, key_path, None)
}

/// Variante mTLS : CA optionnelle pour `tls-auth-clients yes`.
pub fn load_acceptor_mtls(
    cert_path: &Path,
    key_path: &Path,
    ca_path: Option<&Path>,
) -> anyhow::Result<TlsAcceptor> {
    load_acceptor_inner(cert_path, key_path, ca_path)
}

fn load_acceptor_inner(
    cert_path: &Path,
    key_path: &Path,
    ca_path: Option<&Path>,
) -> anyhow::Result<TlsAcceptor> {
    let certs = load_certs(cert_path)?;
    let key = load_private_key(key_path)?;
    let builder = RustlsServerConfig::builder();
    let cfg = if let Some(ca) = ca_path {
        let roots = load_certs(ca)?;
        let mut root_store = rustls::RootCertStore::empty();
        for c in roots {
            root_store
                .add(c)
                .map_err(|e| anyhow!("tls ca: {e}"))?;
        }
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
            .build()
            .map_err(|e| anyhow!("tls client verifier: {e}"))?;
        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key)
            .map_err(|e| anyhow!("tls mtls config: {e}"))?
    } else {
        builder
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| anyhow!("tls config: {e}"))?
    };
    Ok(TlsAcceptor::from(Arc::new(cfg)))
}

fn load_certs(path: &Path) -> anyhow::Result<Vec<CertificateDer<'static>>> {
    let file = File::open(path).with_context(|| format!("open cert {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let certs: Vec<_> = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| format!("parse cert {}", path.display()))?;
    if certs.is_empty() {
        return Err(anyhow!("no certificates in {}", path.display()));
    }
    Ok(certs)
}

fn load_private_key(path: &Path) -> anyhow::Result<PrivateKeyDer<'static>> {
    let file = File::open(path).with_context(|| format!("open key {}", path.display()))?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .with_context(|| format!("parse key {}", path.display()))?
        .ok_or_else(|| anyhow!("no private key in {}", path.display()))
}
