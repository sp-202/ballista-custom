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

//! TLS and the UI token for the scheduler's and history server's HTTP ports,
//! configured by [`ballista_core::security`].

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use ballista_core::security;
use http::StatusCode;
use http::header::WWW_AUTHENTICATE;
use log::{debug, warn};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;

/// How long a client may take to complete the TLS handshake.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// An [`axum::serve::Listener`] that yields TLS connections. Handshakes run
/// in their own tasks, so a slow or hostile client cannot hold up others.
///
/// For [`axum::extract::ConnectInfo`] with the client's [`SocketAddr`], wrap
/// it with [`axum::serve::ListenerExt::tap_io`].
pub struct TlsListener {
    connections: mpsc::Receiver<(TlsStream<TcpStream>, SocketAddr)>,
    local_addr: SocketAddr,
    accept_loop: JoinHandle<()>,
}

impl TlsListener {
    /// Serves TLS with `config` on `listener`.
    pub fn new(listener: TcpListener, config: Arc<rustls::ServerConfig>) -> io::Result<Self> {
        let local_addr = listener.local_addr()?;
        let acceptor = TlsAcceptor::from(config);
        let (tx, connections) = mpsc::channel(64);
        let accept_loop = tokio::spawn(async move {
            loop {
                let (tcp, addr) = match listener.accept().await {
                    Ok(accepted) => accepted,
                    Err(e) => {
                        // Out of file descriptors and the like: back off.
                        warn!("Accepting a connection on {local_addr} failed: {e}");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                };
                let _ = tcp.set_nodelay(true);
                let acceptor = acceptor.clone();
                let tx = tx.clone();
                tokio::spawn(async move {
                    match tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(tcp))
                        .await
                    {
                        Ok(Ok(tls)) => {
                            let _ = tx.send((tls, addr)).await;
                        }
                        // A TCP health check or port scan: closed before any TLS.
                        Ok(Err(e)) if e.kind() == io::ErrorKind::UnexpectedEof => {
                            debug!("Connection from {addr} closed before the TLS handshake")
                        }
                        Ok(Err(e)) => {
                            warn!("Rejected connection from {addr} on {local_addr}: {e}")
                        }
                        Err(_) => debug!("TLS handshake from {addr} timed out"),
                    }
                });
            }
        });
        Ok(Self {
            connections,
            local_addr,
            accept_loop,
        })
    }
}

impl Drop for TlsListener {
    // Closes the port when the server stops, as dropping a TcpListener does.
    fn drop(&mut self) {
        self.accept_loop.abort();
    }
}

impl axum::serve::Listener for TlsListener {
    type Io = TlsStream<TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        match self.connections.recv().await {
            Some(connection) => connection,
            // The accept loop only ends when this listener is dropped.
            None => std::future::pending().await,
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        Ok(self.local_addr)
    }
}

/// Requires the UI token (see [`security::http_request_authorized`]) on every
/// route of `router` except the health probes and CORS preflights. Without a
/// token configured the router is returned unchanged.
pub fn require_ui_token(router: Router, token: Option<Arc<str>>) -> Router {
    match token {
        Some(token) => {
            router.layer(axum::middleware::from_fn_with_state(token, check_ui_token))
        }
        None => router,
    }
}

async fn check_ui_token(
    State(token): State<Arc<str>>,
    request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path();
    let exempt = matches!(path, "/healthz" | "/readyz")
        || request.method() == http::Method::OPTIONS;
    if exempt || security::http_request_authorized(&token, request.headers()) {
        return next.run(request).await;
    }
    (
        StatusCode::UNAUTHORIZED,
        // Browsers ask for a user name and password; any user name with the
        // token as password is accepted.
        [(WWW_AUTHENTICATE, "Basic realm=\"Ballista\", charset=\"UTF-8\"")],
        "authentication required: pass the Ballista UI token as a bearer token, \
         X-Ballista-Token header or Basic password\n",
    )
        .into_response()
}

/// The URL scheme of an HTTP port with or without TLS.
pub(crate) fn scheme(tls: bool) -> &'static str {
    if tls { "https" } else { "http" }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::get;
    use ballista_core::security::{SecurityConfig, TlsMaterial};
    use rcgen::{
        BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer,
        KeyPair,
    };
    use tower::ServiceExt;

    const TOKEN: &str = "test-token-0123456789";

    fn app() -> Router {
        let router = Router::new()
            .route("/api/jobs", get(|| async { "jobs" }))
            .route("/healthz", get(|| async { "ok" }));
        require_ui_token(router, Some(Arc::from(TOKEN)))
    }

    async fn status(request: http::Request<axum::body::Body>) -> StatusCode {
        app().oneshot(request).await.expect("response").status()
    }

    fn get_req(path: &str) -> http::request::Builder {
        http::Request::builder().uri(path)
    }

    #[tokio::test]
    async fn token_guards_everything_but_probes() {
        let empty = axum::body::Body::empty;
        assert_eq!(
            status(get_req("/api/jobs").body(empty()).expect("req")).await,
            StatusCode::UNAUTHORIZED
        );
        let denied = app()
            .oneshot(get_req("/api/jobs").body(empty()).expect("req"))
            .await
            .expect("response");
        assert!(denied.headers().contains_key(WWW_AUTHENTICATE));
        assert_eq!(
            status(
                get_req("/api/jobs")
                    .header("authorization", format!("Bearer {TOKEN}"))
                    .body(empty())
                    .expect("req")
            )
            .await,
            StatusCode::OK
        );
        assert_eq!(
            status(
                get_req("/api/jobs")
                    .header(security::TOKEN_HEADER, TOKEN)
                    .body(empty())
                    .expect("req")
            )
            .await,
            StatusCode::OK
        );
        assert_eq!(
            status(
                get_req("/api/jobs")
                    .header("authorization", "Bearer nope")
                    .body(empty())
                    .expect("req")
            )
            .await,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            status(get_req("/healthz").body(empty()).expect("req")).await,
            StatusCode::OK
        );
    }

    fn material() -> (TlsMaterial, String) {
        let ca_key = KeyPair::generate().expect("key");
        let mut ca = CertificateParams::new(Vec::<String>::new()).expect("params");
        ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca_pem = ca.self_signed(&ca_key).expect("ca").pem();
        let issuer = Issuer::new(ca, ca_key);
        let key = KeyPair::generate().expect("key");
        let mut leaf = CertificateParams::new(vec![
            security::DEFAULT_SERVER_NAME.to_string(),
            "localhost".to_string(),
        ])
        .expect("params");
        leaf.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let cert = leaf.signed_by(&key, &issuer).expect("leaf");
        (
            TlsMaterial::from_pem(cert.pem(), key.serialize_pem(), ca_pem.clone()),
            ca_pem,
        )
    }

    /// The UI port: HTTPS without client certificates, token on the routes,
    /// plaintext refused, and the client address still reaches handlers.
    #[tokio::test]
    async fn https_listener_serves_with_connect_info() {
        use axum::extract::ConnectInfo;
        use axum::serve::ListenerExt;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let _ = rustls::crypto::ring::default_provider().install_default();
        let (material, ca_pem) = material();
        let config = SecurityConfig::default().with_tls(material).expect("tls");
        let server_config = config
            .rustls_server_config(false)
            .expect("config")
            .expect("tls on");
        let tcp = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = tcp.local_addr().expect("addr");
        let router = require_ui_token(
            Router::new().route(
                "/api/whoami",
                get(|ConnectInfo(peer): ConnectInfo<SocketAddr>| async move {
                    peer.ip().to_string()
                }),
            ),
            Some(Arc::from(TOKEN)),
        );
        let listener = TlsListener::new(tcp, server_config).expect("tls").tap_io(|_| {});
        tokio::spawn(async move {
            axum::serve(
                listener,
                router.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
        });

        // A TLS client that trusts the cluster CA.
        let client_config = {
            let mut roots = rustls::RootCertStore::empty();
            use rustls::pki_types::pem::PemObject;
            let ca = rustls::pki_types::CertificateDer::from_pem_slice(ca_pem.as_bytes())
                .expect("ca pem");
            roots.add(ca).expect("ca");
            rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .expect("versions")
            .with_root_certificates(roots)
            .with_no_client_auth()
        };
        let connector = tokio_rustls::TlsConnector::from(Arc::new(client_config));
        let request = |auth: &str| {
            format!(
                "GET /api/whoami HTTP/1.1\r\nHost: localhost\r\n{auth}Connection: close\r\n\r\n"
            )
        };
        let mut responses = vec![];
        for auth in [String::new(), format!("Authorization: Bearer {TOKEN}\r\n")] {
            let tcp = TcpStream::connect(addr).await.expect("connect");
            let server_name = rustls::pki_types::ServerName::try_from("localhost")
                .expect("name")
                .to_owned();
            let mut tls = connector.connect(server_name, tcp).await.expect("handshake");
            tls.write_all(request(&auth).as_bytes()).await.expect("write");
            let mut body = String::new();
            tls.read_to_string(&mut body).await.expect("read");
            responses.push(body);
        }
        assert!(responses[0].starts_with("HTTP/1.1 401"), "{}", responses[0]);
        assert!(responses[1].starts_with("HTTP/1.1 200"), "{}", responses[1]);
        assert!(responses[1].ends_with("127.0.0.1"), "{}", responses[1]);

        // Plain HTTP to the TLS port gets no HTTP response.
        let mut tcp = TcpStream::connect(addr).await.expect("connect");
        tcp.write_all(request("").as_bytes()).await.expect("write");
        let mut buf = Vec::new();
        let _ = tcp.read_to_end(&mut buf).await;
        assert!(!String::from_utf8_lossy(&buf).contains("HTTP/1.1"));
    }
}
