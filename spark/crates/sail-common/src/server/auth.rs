//! Pre-shared bearer token authentication for gRPC servers, the counterpart of
//! Spark Connect's `spark.connect.authenticate.token`.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tonic::body::Body;
use tonic::codegen::http::header::AUTHORIZATION;
use tonic::codegen::http::{Request, Response};
use tower::{Layer, Service};

/// Paths served without a token, so health probes keep working.
const UNAUTHENTICATED_PREFIXES: &[&str] = &["/grpc.health.v1.Health/"];

/// Rejects calls whose `authorization` metadata is not `Bearer <token>` with
/// `UNAUTHENTICATED`, as Spark Connect does. Without a token it lets
/// everything through.
#[derive(Clone, Default)]
pub struct BearerTokenLayer {
    token: Option<Arc<str>>,
}

impl BearerTokenLayer {
    pub fn new(token: Option<Arc<str>>) -> Self {
        Self { token }
    }
}

impl<S> Layer<S> for BearerTokenLayer {
    type Service = BearerTokenService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        BearerTokenService {
            inner,
            token: self.token.clone(),
        }
    }
}

#[derive(Clone)]
pub struct BearerTokenService<S> {
    inner: S,
    token: Option<Arc<str>>,
}

impl<S> BearerTokenService<S> {
    fn authorized<B>(&self, request: &Request<B>) -> bool {
        let Some(expected) = &self.token else {
            return true;
        };
        let path = request.uri().path();
        if UNAUTHENTICATED_PREFIXES.iter().any(|p| path.starts_with(p)) {
            return true;
        }
        request
            .headers()
            .get(AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| {
                let (scheme, token) = v.trim().split_once(' ')?;
                scheme.eq_ignore_ascii_case("bearer").then(|| token.trim())
            })
            .is_some_and(|token| constant_time_eq(expected.as_bytes(), token.as_bytes()))
    }
}

/// Compares in time that depends only on the lengths, not on where the
/// first difference is.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let diff = a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y));
    std::hint::black_box(diff) == 0
}

type BoxFuture<T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send>>;

impl<S, B> Service<Request<B>> for BearerTokenService<S>
where
    S: Service<Request<B>, Response = Response<Body>>,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
{
    type Response = Response<Body>;
    type Error = S::Error;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<B>) -> Self::Future {
        if self.authorized(&request) {
            Box::pin(self.inner.call(request))
        } else {
            log::warn!(
                "Rejected unauthenticated call to {}",
                request.uri().path()
            );
            let response = tonic::Status::unauthenticated("Invalid authentication token")
                .into_http::<Body>();
            Box::pin(std::future::ready(Ok(response)))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use tower::ServiceExt;

    use super::*;

    const TOKEN: &str = "s3cret-token-0123456789";

    async fn call(token: Option<&str>, path: &str, auth: Option<&str>) -> Option<String> {
        let inner = tower::service_fn(|_: Request<Body>| async {
            Ok::<_, Infallible>(Response::new(Body::empty()))
        });
        let service = BearerTokenLayer::new(token.map(Arc::from)).layer(inner);
        let mut request = Request::builder().uri(path);
        if let Some(auth) = auth {
            request = request.header("authorization", auth);
        }
        let Ok(request) = request.body(Body::empty()) else {
            return Some("bad request".to_string());
        };
        let Ok(response) = service.oneshot(request).await;
        // tonic puts the status of an immediate error in the headers.
        response
            .headers()
            .get("grpc-status")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    }

    #[tokio::test]
    async fn requires_the_token() {
        let rpc = "/spark.connect.SparkConnectService/ExecutePlan";
        let unauthenticated = Some(format!("{}", tonic::Code::Unauthenticated as i32));
        assert_eq!(call(Some(TOKEN), rpc, None).await, unauthenticated);
        assert_eq!(
            call(Some(TOKEN), rpc, Some("Bearer wrong")).await,
            unauthenticated
        );
        assert_eq!(call(Some(TOKEN), rpc, Some(TOKEN)).await, unauthenticated);
        assert_eq!(
            call(Some(TOKEN), rpc, Some(&format!("Bearer {TOKEN}"))).await,
            None
        );
        assert_eq!(
            call(Some(TOKEN), rpc, Some(&format!("bearer {TOKEN}"))).await,
            None
        );
        assert_eq!(
            call(Some(TOKEN), "/grpc.health.v1.Health/Check", None).await,
            None
        );
        assert_eq!(call(None, rpc, None).await, None);
    }
}
