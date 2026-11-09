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

//! Application ids for client sessions.
//!
//! Spark's history server is organised around applications, and every application has a short,
//! time-ordered id chosen when it starts (`local-1793774821504`, or `app-20261109202025-0001`
//! on a standalone cluster). A Ballista client session (one Spark Connect connection, or one
//! Flight SQL session) plays that part, but its id is a UUID. The scheduler therefore gives each
//! session an application id the first time it sees it, in the standalone format, and records it
//! on every job of the session.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

/// Sessions remembered. A scheduler that outlives this many sessions forgets the oldest ids
/// wholesale; a session seen again afterwards simply gets a new application id.
const MAX_SESSIONS: usize = 100_000;

static APPS: LazyLock<Mutex<HashMap<String, String>>> = LazyLock::new(Default::default);
static NEXT: AtomicU32 = AtomicU32::new(1);

/// The application id of a client session: `app-<yyyyMMddHHmmss>-<NNNN>` (UTC), the same
/// for every call with the same session id.
pub fn app_id_for_session(session_id: &str) -> String {
    let mut apps = APPS.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(id) = apps.get(session_id) {
        return id.clone();
    }
    if apps.len() >= MAX_SESSIONS {
        apps.clear();
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let id = format!("app-{}-{:04}", utc_stamp(now), NEXT.fetch_add(1, Ordering::Relaxed) % 10_000);
    apps.insert(session_id.to_string(), id.clone());
    id
}

/// `yyyyMMddHHmmss` of a Unix time in UTC.
fn utc_stamp(secs: u64) -> String {
    let (days, rem) = ((secs / 86_400) as i64, secs % 86_400);
    // Civil-from-days (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}{month:02}{day:02}{:02}{:02}{:02}",
        rem / 3_600,
        rem % 3_600 / 60,
        rem % 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_a_unix_time_as_utc() {
        assert_eq!(utc_stamp(0), "19700101000000");
        // 2026-11-09 14:39:05 UTC
        assert_eq!(utc_stamp(1_794_235_145), "20261109143905");
        // A leap day.
        assert_eq!(utc_stamp(1_709_208_296), "20240229120456");
    }

    #[test]
    fn a_session_keeps_its_application_id() {
        let a = app_id_for_session("session-a");
        let b = app_id_for_session("session-b");
        assert_ne!(a, b);
        assert_eq!(a, app_id_for_session("session-a"));
        assert!(a.starts_with("app-") && a.len() == "app-20261109143905-0001".len(), "{a}");
    }
}
