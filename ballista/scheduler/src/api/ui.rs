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

//! The built-in web UI.
//!
//! A single-page app embedded in the scheduler binary and served next to the
//! REST API, which it reads from the same origin: jobs, a per-job event
//! timeline and stage DAG, stage and task details, executors and the
//! environment. The handlers take no server state, so any server exposing the
//! same `/api` routes (e.g. the history server) can serve the UI too.

use axum::Router;
use axum::http::header;
use axum::response::{IntoResponse, Redirect};
use axum::routing::get;

const INDEX_HTML: &str = include_str!("../../ui/index.html");
const UI_CSS: &str = include_str!("../../ui/ui.css");
const UI_JS: &str = include_str!("../../ui/ui.js");

/// The assets are compiled in, so a browser should revalidate rather than
/// cache them across scheduler upgrades.
const CACHE: (header::HeaderName, &str) = (header::CACHE_CONTROL, "no-cache");

async fn index() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8"), CACHE],
        INDEX_HTML,
    )
}

async fn stylesheet() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "text/css; charset=utf-8"), CACHE], UI_CSS)
}

async fn script() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/javascript; charset=utf-8"), CACHE],
        UI_JS,
    )
}

/// Normalizes a configured route to `/segment` form (no trailing slash);
/// `/` and the empty string both mean the root.
fn normalize(route: &str) -> String {
    let trimmed = route.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        String::new()
    } else if trimmed.starts_with('/') {
        trimmed.to_string()
    } else {
        format!("/{trimmed}")
    }
}

/// Routes serving the UI under `route` (`/` on the web UI port): the page at `{route}/`
/// with its assets next to it, and `{route}` redirecting there so the page's
/// relative asset URLs resolve.
pub fn routes<S: Clone + Send + Sync + 'static>(route: &str) -> Router<S> {
    let base = normalize(route);
    let page = format!("{base}/");
    let router = Router::new()
        .route(&page, get(index))
        .route(&format!("{base}/ui.css"), get(stylesheet))
        .route(&format!("{base}/ui.js"), get(script));
    if base.is_empty() {
        router
    } else {
        router.route(&base, get(move || async move { Redirect::permanent(&page) }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    async fn get_path(router: Router, path: &str) -> (StatusCode, String, String) {
        let response = router
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let header = |name| {
            response
                .headers()
                .get(name)
                .map(|v: &axum::http::HeaderValue| v.to_str().unwrap().to_string())
                .unwrap_or_default()
        };
        let content_type = header(header::CONTENT_TYPE);
        let location = header(header::LOCATION);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        (status, content_type, if location.is_empty() { body } else { location })
    }

    #[tokio::test]
    async fn serves_page_and_assets_under_route() {
        let (status, ct, body) = get_path(routes("/ui"), "/ui/").await;
        assert_eq!(status, StatusCode::OK);
        assert!(ct.starts_with("text/html"), "{ct}");
        assert!(body.contains(r#"<script src="ui.js">"#), "{body}");

        let (status, ct, body) = get_path(routes("/ui"), "/ui/ui.js").await;
        assert_eq!(status, StatusCode::OK);
        assert!(ct.starts_with("text/javascript"), "{ct}");
        assert!(body.contains("const API = \"/api\""));

        let (status, ct, _) = get_path(routes("/ui"), "/ui/ui.css").await;
        assert_eq!(status, StatusCode::OK);
        assert!(ct.starts_with("text/css"), "{ct}");
    }

    #[tokio::test]
    async fn redirects_route_without_trailing_slash() {
        let (status, _, location) = get_path(routes("/ui"), "/ui").await;
        assert_eq!(status, StatusCode::PERMANENT_REDIRECT);
        assert_eq!(location, "/ui/");
    }

    #[tokio::test]
    async fn serves_at_root() {
        let (status, ct, _) = get_path(routes("/"), "/").await;
        assert_eq!(status, StatusCode::OK);
        assert!(ct.starts_with("text/html"), "{ct}");
        let (status, _, _) = get_path(routes("/"), "/ui.js").await;
        assert_eq!(status, StatusCode::OK);
    }

    #[test]
    fn normalizes_routes() {
        assert_eq!(normalize("/ui/"), "/ui");
        assert_eq!(normalize("ui"), "/ui");
        assert_eq!(normalize(" / "), "");
    }
}
