// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Cluster security: mutual TLS between Ballista processes and a token for the
//! web UIs and REST API. The counterpart of Spark's `spark.authenticate`,
//! `spark.network.crypto.enabled` / `spark.ssl.rpc.enabled` and `spark.ssl.ui`.
//!
//! Each process reads its settings once at startup ([`init_from_env`]):
//!
//! | Variable | Meaning |
//! |---|---|
//! | `BALLISTA_TLS_CERT`, `BALLISTA_TLS_KEY` | PEM certificate (chain) and private key of this process |
//! | `BALLISTA_TLS_CA` | PEM CA bundle that signs every cluster member's certificate |
//! | `BALLISTA_TLS_SERVER_NAME` | name expected in peer certificates (default `ballista-internal`) |
//! | `BALLISTA_UI_TOKEN` or `BALLISTA_UI_TOKEN_FILE` | token required by the web UI and REST API |
//! | `BALLISTA_UI_TLS` | `false` serves the web UI over plain HTTP although cluster TLS is on |
//!
//! With the three TLS variables set, every gRPC and Arrow Flight connection
//! between schedulers, executors and clients (task launch, heartbeats, shuffle
//! reads, result fetches, job submission) is mutual TLS: both ends present a
//! certificate signed by the cluster CA. A process without such a certificate
//! cannot register an executor, submit or cancel a job, launch tasks or read
//! shuffle data, and all of that traffic is encrypted. Executors are reached by
//! IP address, so peers are verified against one fixed name
//! (`BALLISTA_TLS_SERVER_NAME`) that every cluster certificate carries.
//!
//! The web UI and REST API, used by people and tools rather than cluster
//! members, check the UI token instead: `Authorization: Bearer <token>`, HTTP
//! Basic with the token as the password (browsers prompt for it), or
//! `X-Ballista-Token: <token>` (survives the Kubernetes API server's service
//! proxy, which consumes `Authorization`).
//!
//! Nothing is enabled unless configured: a process with none of the variables
//! behaves exactly as before.

use std::fmt;
use std::path::Path;
use std::sync::{Arc, OnceLock};

use base64::Engine;
use log::info;
use subtle::ConstantTimeEq;
use tonic::codegen::http::HeaderMap;
use tonic::codegen::http::header::AUTHORIZATION;
use tonic::transport::{
    Certificate, ClientTlsConfig, Endpoint, Identity, Server, ServerTlsConfig,
};

use crate::error::{BallistaError, Result};

/// Name every cluster certificate carries and peers are verified against.
pub const DEFAULT_SERVER_NAME: &str = "ballista-internal";
/// PEM certificate (chain) of this process.
pub const ENV_TLS_CERT: &str = "BALLISTA_TLS_CERT";
/// PEM private key of this process.
pub const ENV_TLS_KEY: &str = "BALLISTA_TLS_KEY";
/// PEM CA bundle trusted for cluster peers.
pub const ENV_TLS_CA: &str = "BALLISTA_TLS_CA";
/// Name expected in peer certificates.
pub const ENV_TLS_SERVER_NAME: &str = "BALLISTA_TLS_SERVER_NAME";
/// Token for the web UI and REST API.
pub const ENV_UI_TOKEN: &str = "BALLISTA_UI_TOKEN";
/// File holding the token for the web UI and REST API.
pub const ENV_UI_TOKEN_FILE: &str = "BALLISTA_UI_TOKEN_FILE";
/// `false` keeps the web UI on plain HTTP when cluster TLS is on.
pub const ENV_UI_TLS: &str = "BALLISTA_UI_TLS";
/// HTTP header carrying the UI token, an alternative to `Authorization`.
pub const TOKEN_HEADER: &str = "x-ballista-token";

/// The PEM files that make a process a cluster member.
#[derive(Clone)]
pub struct TlsMaterial {
    cert_pem: Vec<u8>,
    key_pem: Vec<u8>,
    ca_pem: Vec<u8>,
    server_name: String,
}

impl TlsMaterial {
    /// Certificate, key and CA from PEM bytes.
    pub fn from_pem(
        cert_pem: impl Into<Vec<u8>>,
        key_pem: impl Into<Vec<u8>>,
        ca_pem: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            cert_pem: cert_pem.into(),
            key_pem: key_pem.into(),
            ca_pem: ca_pem.into(),
            server_name: DEFAULT_SERVER_NAME.to_string(),
        }
    }

    /// Reads certificate, key and CA from PEM files.
    pub fn from_files(cert: &Path, key: &Path, ca: &Path) -> Result<Self> {
        Ok(Self::from_pem(read(cert)?, read(key)?, read(ca)?))
    }

    /// Verifies peers against `name` instead of [`DEFAULT_SERVER_NAME`].
    pub fn with_server_name(mut self, name: impl Into<String>) -> Self {
        self.server_name = name.into();
        self
    }
}

fn read(path: &Path) -> Result<Vec<u8>> {
    std::fs::read(path).map_err(|e| {
        BallistaError::Configuration(format!("cannot read {}: {e}", path.display()))
    })
}

/// Security settings of one process. The default enables nothing.
#[derive(Clone, Default)]
pub struct SecurityConfig {
    tls: Option<TlsMaterial>,
    client_tls: Option<ClientTlsConfig>,
    server_tls: Option<ServerTlsConfig>,
    ui_token: Option<Arc<str>>,
    ui_plain_http: bool,
}

impl fmt::Debug for SecurityConfig {
    // Never prints keys or tokens.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SecurityConfig")
            .field("tls", &self.tls.as_ref().map(|t| t.server_name.as_str()))
            .field("ui_token", &self.ui_token.is_some())
            .field("ui_tls", &self.ui_tls())
            .finish()
    }
}

impl SecurityConfig {
    /// Settings from the process environment (see the module documentation).
    pub fn from_env() -> Result<Self> {
        Self::from_vars(|name| std::env::var(name).ok())
    }

    /// Settings from `var`, which looks variables up by name.
    pub fn from_vars(var: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let get = |name: &str| var(name).filter(|v| !v.trim().is_empty());
        let tls = match (get(ENV_TLS_CERT), get(ENV_TLS_KEY), get(ENV_TLS_CA)) {
            (None, None, None) => None,
            (Some(cert), Some(key), Some(ca)) => {
                let mut material = TlsMaterial::from_files(
                    Path::new(&cert),
                    Path::new(&key),
                    Path::new(&ca),
                )?;
                if let Some(name) = get(ENV_TLS_SERVER_NAME) {
                    material = material.with_server_name(name);
                }
                Some(material)
            }
            _ => {
                return Err(BallistaError::Configuration(format!(
                    "{ENV_TLS_CERT}, {ENV_TLS_KEY} and {ENV_TLS_CA} must be set together"
                )));
            }
        };
        let ui_token = match (get(ENV_UI_TOKEN), get(ENV_UI_TOKEN_FILE)) {
            (Some(token), _) => Some(token),
            (None, Some(file)) => Some(
                String::from_utf8(read(Path::new(&file))?).map_err(|_| {
                    BallistaError::Configuration(format!("{file} is not UTF-8"))
                })?,
            ),
            (None, None) => None,
        };
        let ui_plain_http = get(ENV_UI_TLS)
            .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "false" | "0" | "no"))
            .unwrap_or(false);
        let mut config = Self::default();
        if let Some(tls) = tls {
            config = config.with_tls(tls)?;
        }
        if let Some(token) = ui_token {
            config = config.with_ui_token(token)?;
        }
        config.ui_plain_http = ui_plain_http;
        Ok(config)
    }

    /// Enables mutual TLS with `material`, checking that it is usable.
    pub fn with_tls(mut self, material: TlsMaterial) -> Result<Self> {
        install_crypto_provider();
        let identity = Identity::from_pem(&material.cert_pem, &material.key_pem);
        let ca = Certificate::from_pem(&material.ca_pem);
        let client = ClientTlsConfig::new()
            .ca_certificate(ca.clone())
            .identity(identity.clone())
            .domain_name(material.server_name.clone());
        let server = ServerTlsConfig::new().identity(identity).client_ca_root(ca);
        // tonic parses the PEM lazily; fail now, not on the first connection.
        Server::builder()
            .tls_config(server.clone())
            .map_err(|e| tls_error("server", e))?;
        Endpoint::from_static("https://localhost")
            .tls_config(client.clone())
            .map_err(|e| tls_error("client", e))?;
        rustls_config(&material, true)?;
        self.client_tls = Some(client);
        self.server_tls = Some(server);
        self.tls = Some(material);
        Ok(self)
    }

    /// Requires `token` on the web UI and REST API.
    pub fn with_ui_token(mut self, token: impl AsRef<str>) -> Result<Self> {
        let token = token.as_ref().trim();
        if token.len() < 16 {
            return Err(BallistaError::Configuration(
                "the UI token must be at least 16 characters".to_string(),
            ));
        }
        self.ui_token = Some(Arc::from(token));
        Ok(self)
    }

    /// Whether gRPC and Flight use mutual TLS.
    pub fn tls_enabled(&self) -> bool {
        self.tls.is_some()
    }

    /// The token the web UI and REST API require, if any.
    pub fn ui_token(&self) -> Option<Arc<str>> {
        self.ui_token.clone()
    }

    /// Whether the web UI and REST API ports serve HTTPS.
    pub fn ui_tls(&self) -> bool {
        self.tls.is_some() && !self.ui_plain_http
    }

    /// tonic client settings for cluster connections, if TLS is on.
    pub fn client_tls_config(&self) -> Option<ClientTlsConfig> {
        self.client_tls.clone()
    }

    /// tonic server settings requiring client certificates, if TLS is on.
    pub fn server_tls_config(&self) -> Option<ServerTlsConfig> {
        self.server_tls.clone()
    }

    /// Server-only TLS (no client certificate) with the cluster certificate,
    /// for endpoints used by people and tools rather than cluster members.
    pub fn public_server_tls_config(&self) -> Option<ServerTlsConfig> {
        self.tls.as_ref().map(|t| {
            ServerTlsConfig::new().identity(Identity::from_pem(&t.cert_pem, &t.key_pem))
        })
    }

    /// Switches `endpoint` to TLS with the cluster identity, when TLS is on:
    /// an `http://` address becomes `https://` and the cluster CA, certificate
    /// and server name are applied. Other endpoint settings are not kept, so
    /// call this before setting timeouts.
    pub fn secure_endpoint(
        &self,
        endpoint: Endpoint,
    ) -> std::result::Result<Endpoint, tonic::transport::Error> {
        let Some(tls) = self.client_tls.clone() else {
            return Ok(endpoint);
        };
        let uri = endpoint.uri().to_string();
        let endpoint = match uri.strip_prefix("http://") {
            Some(rest) => Endpoint::from_shared(format!("https://{rest}"))?,
            None => endpoint,
        };
        endpoint.tls_config(tls)
    }

    /// rustls settings for an HTTP server on the cluster certificate, if TLS is
    /// on. `require_client_cert` makes it mutual TLS (cluster members only);
    /// without it any client can connect and the UI token protects the routes.
    pub fn rustls_server_config(
        &self,
        require_client_cert: bool,
    ) -> Result<Option<Arc<rustls::ServerConfig>>> {
        self.tls
            .as_ref()
            .map(|t| rustls_config(t, require_client_cert))
            .transpose()
    }
}

fn tls_error(side: &str, e: impl fmt::Display) -> BallistaError {
    BallistaError::Configuration(format!("invalid cluster TLS {side} settings: {e}"))
}

fn rustls_config(
    material: &TlsMaterial,
    require_client_cert: bool,
) -> Result<Arc<rustls::ServerConfig>> {
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};

    let certs = CertificateDer::pem_slice_iter(&material.cert_pem)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| tls_error("certificate", e))?;
    let key = PrivateKeyDer::from_pem_slice(&material.key_pem)
        .map_err(|e| tls_error("key", e))?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| tls_error("protocol", e))?;
    let builder = if require_client_cert {
        let mut roots = rustls::RootCertStore::empty();
        for ca in CertificateDer::pem_slice_iter(&material.ca_pem) {
            roots
                .add(ca.map_err(|e| tls_error("CA", e))?)
                .map_err(|e| tls_error("CA", e))?;
        }
        let verifier =
            rustls::server::WebPkiClientVerifier::builder_with_provider(
                Arc::new(roots),
                provider,
            )
            .build()
            .map_err(|e| tls_error("client verifier", e))?;
        builder.with_client_cert_verifier(verifier)
    } else {
        builder.with_no_client_auth()
    };
    let mut config = builder
        .with_single_cert(certs, key)
        .map_err(|e| tls_error("certificate", e))?;
    // gRPC needs HTTP/2; browsers and curl may use HTTP/1.1.
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(Arc::new(config))
}

/// Makes ring rustls's process-wide crypto provider, unless one is set. tonic's
/// TLS server needs one, and cannot pick it when several are linked in.
pub fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

static CURRENT: OnceLock<SecurityConfig> = OnceLock::new();

/// Makes `config` this process's settings. Fails if they were already set or
/// read (call it before creating any client or server).
pub fn install(config: SecurityConfig) -> Result<&'static SecurityConfig> {
    install_crypto_provider();
    CURRENT.set(config).map_err(|_| {
        BallistaError::Internal(
            "security settings were already initialized in this process".to_string(),
        )
    })?;
    Ok(current())
}

/// Reads the settings from the environment, installs them and logs what is on.
pub fn init_from_env() -> Result<&'static SecurityConfig> {
    let config = install(SecurityConfig::from_env()?)?;
    match (config.tls_enabled(), config.ui_token.is_some()) {
        (false, false) => info!(
            "Cluster security is off: gRPC/Flight are plaintext and unauthenticated \
             (set {ENV_TLS_CERT}, {ENV_TLS_KEY}, {ENV_TLS_CA} and {ENV_UI_TOKEN})"
        ),
        (tls, token) => info!(
            "Cluster security: mutual TLS {}, UI/REST token {}, UI over {}",
            if tls { "on" } else { "off" },
            if token { "required" } else { "not required" },
            if config.ui_tls() { "HTTPS" } else { "HTTP" }
        ),
    }
    Ok(config)
}

/// This process's settings; nothing enabled if [`install`] was not called.
pub fn current() -> &'static SecurityConfig {
    CURRENT.get_or_init(SecurityConfig::default)
}

/// Compares a presented token with the expected one in constant time.
pub fn token_matches(expected: &str, presented: &str) -> bool {
    bool::from(expected.as_bytes().ct_eq(presented.as_bytes()))
}

/// Whether an HTTP request carries `expected`: as `X-Ballista-Token`, a bearer
/// token, or the password of HTTP Basic credentials (any user name).
pub fn http_request_authorized(expected: &str, headers: &HeaderMap) -> bool {
    if let Some(token) = headers.get(TOKEN_HEADER).and_then(|v| v.to_str().ok())
        && token_matches(expected, token.trim())
    {
        return true;
    }
    let Some(auth) = headers.get(AUTHORIZATION).and_then(|v| v.to_str().ok()) else {
        return false;
    };
    let auth = auth.trim();
    if let Some(token) = strip_scheme(auth, "bearer") {
        return token_matches(expected, token);
    }
    if let Some(encoded) = strip_scheme(auth, "basic") {
        let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(encoded) else {
            return false;
        };
        let Ok(decoded) = String::from_utf8(decoded) else {
            return false;
        };
        return decoded
            .split_once(':')
            .is_some_and(|(_, password)| token_matches(expected, password));
    }
    false
}

fn strip_scheme<'a>(value: &'a str, scheme: &str) -> Option<&'a str> {
    let (name, rest) = value.split_once(' ')?;
    name.eq_ignore_ascii_case(scheme).then(|| rest.trim())
}

#[cfg(test)]
pub(crate) mod test_certs {
    use super::TlsMaterial;
    use rcgen::{
        BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa,
        Issuer, KeyPair, KeyUsagePurpose,
    };

    /// A CA and two certificates it signs (both carry the default server
    /// name), plus a certificate from an unrelated CA.
    pub(crate) struct TestPki {
        pub(crate) member: TlsMaterial,
        pub(crate) other_member: TlsMaterial,
        pub(crate) stranger: TlsMaterial,
    }

    fn ca() -> (String, Issuer<'static, KeyPair>) {
        let key = KeyPair::generate().expect("key");
        let mut params = CertificateParams::new(Vec::<String>::new()).expect("params");
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.distinguished_name.push(DnType::CommonName, "test ca");
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let cert = params.self_signed(&key).expect("ca");
        (cert.pem(), Issuer::new(params, key))
    }

    fn leaf(issuer: &Issuer<'static, KeyPair>, ca_pem: &str) -> TlsMaterial {
        let key = KeyPair::generate().expect("key");
        let mut params = CertificateParams::new(vec![
            super::DEFAULT_SERVER_NAME.to_string(),
            "localhost".to_string(),
        ])
        .expect("params");
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let cert = params.signed_by(&key, issuer).expect("leaf");
        TlsMaterial::from_pem(cert.pem(), key.serialize_pem(), ca_pem.to_string())
    }

    pub(crate) fn pki() -> TestPki {
        let (ca_pem, issuer) = ca();
        let (other_ca_pem, other_issuer) = ca();
        TestPki {
            member: leaf(&issuer, &ca_pem),
            other_member: leaf(&issuer, &ca_pem),
            stranger: leaf(&other_issuer, &other_ca_pem),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::convert::Infallible;
    use std::task::{Context, Poll};
    use tonic::body::Body;
    use tonic::codegen::http::{self, HeaderValue};
    use tonic::server::NamedService;
    use tonic::transport::Channel;

    const TOKEN: &str = "0123456789abcdef-token";

    fn headers(name: &str, value: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(
            http::HeaderName::from_bytes(name.as_bytes()).expect("name"),
            HeaderValue::from_str(value).expect("value"),
        );
        h
    }

    #[test]
    fn http_token_forms() {
        let basic = base64::engine::general_purpose::STANDARD.encode(format!("me:{TOKEN}"));
        assert!(http_request_authorized(TOKEN, &headers("authorization", &format!("Bearer {TOKEN}"))));
        assert!(http_request_authorized(TOKEN, &headers("authorization", &format!("bearer  {TOKEN}"))));
        assert!(http_request_authorized(TOKEN, &headers("authorization", &format!("Basic {basic}"))));
        assert!(http_request_authorized(TOKEN, &headers(TOKEN_HEADER, TOKEN)));
        assert!(!http_request_authorized(TOKEN, &HeaderMap::new()));
        assert!(!http_request_authorized(TOKEN, &headers("authorization", "Bearer wrong")));
        assert!(!http_request_authorized(TOKEN, &headers("authorization", &format!("Token {TOKEN}"))));
        let wrong = base64::engine::general_purpose::STANDARD.encode(format!("{TOKEN}:x"));
        assert!(!http_request_authorized(TOKEN, &headers("authorization", &format!("Basic {wrong}"))));
        assert!(!http_request_authorized(TOKEN, &headers("authorization", "Basic !!!")));
        assert!(!http_request_authorized(TOKEN, &headers(TOKEN_HEADER, &TOKEN[1..])));
    }

    #[test]
    fn env_settings() {
        let none = SecurityConfig::from_vars(|_| None).expect("empty");
        assert!(!none.tls_enabled() && none.ui_token().is_none());

        let vars: HashMap<&str, &str> = [(ENV_TLS_CERT, "/x/tls.crt")].into();
        let err = SecurityConfig::from_vars(|k| vars.get(k).map(|v| v.to_string()))
            .expect_err("partial TLS settings");
        assert!(err.to_string().contains("must be set together"), "{err}");

        let short: HashMap<&str, &str> = [(ENV_UI_TOKEN, "short")].into();
        assert!(SecurityConfig::from_vars(|k| short.get(k).map(|v| v.to_string())).is_err());

        let dir = tempfile::tempdir().expect("dir");
        let pki = test_certs::pki();
        let (cert, key, ca) = (dir.path().join("c"), dir.path().join("k"), dir.path().join("a"));
        std::fs::write(&cert, &pki.member.cert_pem).expect("w");
        std::fs::write(&key, &pki.member.key_pem).expect("w");
        std::fs::write(&ca, &pki.member.ca_pem).expect("w");
        let token_file = dir.path().join("t");
        std::fs::write(&token_file, format!("{TOKEN}\n")).expect("w");
        let vars: HashMap<&str, String> = [
            (ENV_TLS_CERT, cert.display().to_string()),
            (ENV_TLS_KEY, key.display().to_string()),
            (ENV_TLS_CA, ca.display().to_string()),
            (ENV_UI_TOKEN_FILE, token_file.display().to_string()),
        ]
        .into();
        let config =
            SecurityConfig::from_vars(|k| vars.get(k).cloned()).expect("full settings");
        assert!(config.tls_enabled() && config.ui_tls());
        assert_eq!(config.ui_token().as_deref(), Some(TOKEN));
        assert!(!format!("{config:?}").contains(TOKEN));

        let garbage = TlsMaterial::from_pem("not a cert", "not a key", "nope");
        assert!(SecurityConfig::default().with_tls(garbage).is_err());
    }

    #[tokio::test]
    async fn endpoint_switches_to_https() {
        let pki = test_certs::pki();
        let plain = SecurityConfig::default();
        let ep = plain
            .secure_endpoint(Endpoint::from_static("http://10.0.0.1:50051"))
            .expect("plain");
        assert_eq!(ep.uri().scheme_str(), Some("http"));
        let secure = SecurityConfig::default().with_tls(pki.member).expect("tls");
        let ep = secure
            .secure_endpoint(Endpoint::from_static("http://10.0.0.1:50051"))
            .expect("tls");
        assert_eq!(ep.uri().to_string(), "https://10.0.0.1:50051/");
    }

    /// A one-method gRPC-shaped service that answers 200 to anything.
    #[derive(Clone)]
    struct Ping;
    impl NamedService for Ping {
        const NAME: &'static str = "test.Ping";
    }
    impl tonic::codegen::Service<http::Request<Body>> for Ping {
        type Response = http::Response<Body>;
        type Error = Infallible;
        type Future = std::future::Ready<std::result::Result<Self::Response, Infallible>>;
        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<std::result::Result<(), Infallible>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, _: http::Request<Body>) -> Self::Future {
            std::future::ready(Ok(http::Response::new(Body::empty())))
        }
    }

    async fn call(channel: &mut Channel) -> std::result::Result<http::StatusCode, String> {
        use tonic::codegen::Service;
        std::future::poll_fn(|cx| channel.poll_ready(cx))
            .await
            .map_err(|e| e.to_string())?;
        let req = http::Request::builder()
            .method("POST")
            .uri("https://ballista-internal/test.Ping/Call")
            .header("content-type", "application/grpc")
            .body(Body::empty())
            .map_err(|e| e.to_string())?;
        channel.call(req).await.map(|r| r.status()).map_err(|e| format!("{e:?}"))
    }

    async fn connect(client: &SecurityConfig, addr: std::net::SocketAddr) -> std::result::Result<Channel, String> {
        let ep = client
            .secure_endpoint(Endpoint::from_shared(format!("http://{addr}")).expect("uri"))
            .map_err(|e| e.to_string())?;
        ep.connect().await.map_err(|e| format!("{e:?}"))
    }

    /// Executors' gRPC/Flight servers: only cluster members get through.
    #[tokio::test]
    async fn grpc_requires_cluster_certificate() {
        let pki = test_certs::pki();
        let server = SecurityConfig::default().with_tls(pki.member.clone()).expect("tls");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let incoming = tonic::transport::server::TcpIncoming::from(listener);
        let tls = server.server_tls_config().expect("server tls");
        tokio::spawn(async move {
            Server::builder()
                .tls_config(tls)
                .expect("tls")
                .add_service(Ping)
                .serve_with_incoming(incoming)
                .await
        });

        let member = SecurityConfig::default().with_tls(pki.other_member).expect("tls");
        let mut channel = connect(&member, addr).await.expect("member connects");
        assert_eq!(call(&mut channel).await, Ok(http::StatusCode::OK));

        // A certificate from another CA is rejected by the server.
        let stranger = SecurityConfig::default().with_tls(pki.stranger.clone()).expect("tls");
        let rejected = match connect(&stranger, addr).await {
            Err(_) => true,
            Ok(mut channel) => call(&mut channel).await.is_err(),
        };
        assert!(rejected, "a certificate from another CA must be rejected");

        // No client certificate at all: the server refuses the handshake.
        let no_identity = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(&pki.member.ca_pem))
            .domain_name(DEFAULT_SERVER_NAME);
        let ep = Endpoint::from_shared(format!("https://{addr}"))
            .expect("uri")
            .tls_config(no_identity)
            .expect("tls");
        let rejected = match ep.connect().await {
            Err(_) => true,
            Ok(mut channel) => call(&mut channel).await.is_err(),
        };
        assert!(rejected, "a client without a certificate must be rejected");

        // Plaintext gRPC to a TLS port fails.
        let mut plain = Endpoint::from_shared(format!("http://{addr}"))
            .expect("uri")
            .connect()
            .await
            .expect("tcp connect");
        assert!(call(&mut plain).await.is_err());
    }
}
