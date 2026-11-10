//! Reconciles a `BallistaApplication` through the `SparkApplication` states:
//! (cluster ready) → SUBMITTED (driver pod created) → RUNNING → COMPLETED | FAILED,
//! with PENDING_RERUN for `restartPolicy: OnFailure` retries. A cluster created
//! for the application is deleted when it finishes; `timeToLiveSeconds` deletes
//! the application itself.

use std::sync::Arc;
use std::time::Duration as StdDuration;

use chrono::{Duration, Utc};
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::{Api, Patch, PatchParams, PostParams};
use kube::runtime::controller::Action;
use kube::{Resource, ResourceExt};
use serde_json::json;

use crate::cluster::{apply, delete_if_exists, parse_time};
use crate::crd::{
    AppType, ApplicationState, BallistaApplication, BallistaApplicationStatus, BallistaCluster,
    state,
};
use crate::resources as r;
use crate::{Ctx, Error};

pub fn ephemeral_cluster_name(app: &str) -> String {
    format!("{app}-cluster")
}

fn driver_pod_name(app: &str, attempt: u32) -> String {
    let suffix = format!("-driver-{attempt}");
    let keep = 63usize.saturating_sub(suffix.len());
    format!("{}{suffix}", &app[..app.len().min(keep)])
}

pub async fn reconcile(app: Arc<BallistaApplication>, ctx: Arc<Ctx>) -> Result<Action, Error> {
    let ns = app
        .namespace()
        .ok_or_else(|| Error::Invalid("BallistaApplication must be namespaced".into()))?;
    let name = app.name_any();
    let owner = app
        .controller_owner_ref(&())
        .ok_or_else(|| Error::Invalid("BallistaApplication has no uid yet".into()))?;
    let spec = &app.spec;
    let client = &ctx.client;
    let apps: Api<BallistaApplication> = Api::namespaced(client.clone(), &ns);
    let clusters: Api<BallistaCluster> = Api::namespaced(client.clone(), &ns);
    let pods: Api<Pod> = Api::namespaced(client.clone(), &ns);
    let now = Utc::now();
    let mut st = app.status.clone().unwrap_or_default();

    // Finished: clean up, then honour timeToLiveSeconds.
    if state::is_terminal(&st.application_state.state) {
        if spec.cluster.is_some() {
            delete_if_exists(&clusters, &ephemeral_cluster_name(&name)).await?;
        }
        let ended = st.termination_time.as_deref().and_then(parse_time);
        if let (Some(ttl), Some(ended)) = (spec.time_to_live_seconds, ended) {
            let expires = ended + Duration::seconds(ttl);
            if now >= expires {
                log::info!("{ns}/{name}: time to live passed, deleting");
                delete_if_exists(&apps, &name).await?;
                return Ok(Action::await_change());
            }
            let wait = (expires - now).to_std().unwrap_or_default();
            return Ok(Action::requeue(wait + StdDuration::from_secs(1)));
        }
        return Ok(Action::await_change());
    }

    // Validate.
    let invalid = match (spec.app_type, &spec.main_application_file, &spec.sql) {
        (AppType::Python, None, _) => Some("type Python needs mainApplicationFile"),
        (AppType::Sql, _, None) => Some("type Sql needs sql"),
        _ => None,
    }
    .or(match (&spec.cluster_ref, &spec.cluster) {
        (Some(_), Some(_)) | (None, None) => Some("set exactly one of clusterRef or cluster"),
        _ => None,
    });
    if let Some(msg) = invalid {
        finish(&mut st, state::SUBMISSION_FAILED, Some(msg.to_string()));
        return save(&apps, &name, &app, st).await;
    }

    // The cluster: shared, or created for this application.
    let cluster_name = match (&spec.cluster_ref, &spec.cluster) {
        (Some(c), _) => c.name.clone(),
        (None, Some(cs)) => {
            let cname = ephemeral_cluster_name(&name);
            let mut labels = r::labels(&cname, "cluster");
            labels.insert(r::APP_LABEL.to_string(), name.clone());
            let obj = BallistaCluster {
                metadata: ObjectMeta {
                    name: Some(cname.clone()),
                    namespace: Some(ns.clone()),
                    labels: Some(labels),
                    owner_references: Some(vec![owner.clone()]),
                    ..Default::default()
                },
                spec: cs.clone(),
                status: None,
            };
            apply(&clusters, &obj).await?;
            cname
        }
        (None, None) => return Err(Error::Invalid("no cluster".into())),
    };
    st.cluster_name = Some(cluster_name.clone());
    let cluster = match clusters.get_opt(&cluster_name).await? {
        Some(c) if c.status.as_ref().and_then(|s| s.phase.as_deref()) == Some("Running") => c,
        found => {
            st.application_state = ApplicationState {
                state: state::SUBMITTED.to_string(),
                error_message: Some(if found.is_some() {
                    format!("waiting for BallistaCluster {cluster_name} to be Running")
                } else {
                    format!("BallistaCluster {cluster_name} not found")
                }),
            };
            save(&apps, &name, &app, st).await?;
            return Ok(Action::requeue(StdDuration::from_secs(3)));
        }
    };

    // The driver pod of the current attempt.
    let current = st.driver_info.pod_name.clone();
    let pod = match &current {
        Some(p) => pods.get_opt(p).await?,
        None => None,
    };
    let rerun_due = st.application_state.state == state::PENDING_RERUN
        && pod.as_ref().and_then(finished_at).is_none_or(|t| {
            now >= t + Duration::seconds(
                spec.restart_policy.on_failure_retry_interval_seconds as i64,
            )
        });
    if current.is_none() || rerun_due {
        let attempt = st.submission_attempts + 1;
        let pod_name = driver_pod_name(&name, attempt);
        let driver = r::driver_pod(
            &name,
            &pod_name,
            &ns,
            spec,
            &cluster_name,
            &cluster.spec,
            &ctx.driver_image,
            &owner,
        );
        match pods.create(&PostParams::default(), &driver).await {
            Ok(_) => log::info!("{ns}/{name}: started driver {pod_name}"),
            Err(kube::Error::Api(s)) if s.code == 409 => {}
            Err(e) => return Err(e.into()),
        }
        st.submission_attempts = attempt;
        st.last_submission_attempt_time = Some(now.to_rfc3339());
        st.driver_info.pod_name = Some(pod_name);
        st.application_state = ApplicationState {
            state: state::SUBMITTED.to_string(),
            error_message: None,
        };
        save(&apps, &name, &app, st).await?;
        return Ok(Action::requeue(StdDuration::from_secs(5)));
    }
    if st.application_state.state == state::PENDING_RERUN {
        return Ok(Action::requeue(StdDuration::from_secs(5)));
    }

    let Some(pod) = pod else {
        finish(
            &mut st,
            state::FAILED,
            Some("driver pod was deleted".into()),
        );
        cleanup(&clusters, spec.cluster.is_some(), &name).await?;
        return save(&apps, &name, &app, st).await;
    };
    let phase = pod
        .status
        .as_ref()
        .and_then(|s| s.phase.clone())
        .unwrap_or_default();
    match phase.as_str() {
        "Running" => {
            st.application_state = ApplicationState {
                state: state::RUNNING.to_string(),
                error_message: None,
            };
        }
        "Succeeded" => {
            finish(&mut st, state::COMPLETED, None);
            cleanup(&clusters, spec.cluster.is_some(), &name).await?;
        }
        "Failed" => {
            let reason = failure_reason(&pod);
            let retries_left = spec.restart_policy.policy == "OnFailure"
                && st.submission_attempts <= spec.restart_policy.on_failure_retries;
            if retries_left {
                log::info!("{ns}/{name}: driver failed ({reason}), retrying");
                st.application_state = ApplicationState {
                    state: state::PENDING_RERUN.to_string(),
                    error_message: Some(reason),
                };
            } else {
                finish(&mut st, state::FAILED, Some(reason));
                cleanup(&clusters, spec.cluster.is_some(), &name).await?;
            }
        }
        _ => {} // Pending: still SUBMITTED
    }
    save(&apps, &name, &app, st).await
}

pub fn error_policy(app: Arc<BallistaApplication>, err: &Error, _: Arc<Ctx>) -> Action {
    log::warn!("reconciling BallistaApplication {}: {err}", app.name_any());
    Action::requeue(StdDuration::from_secs(10))
}

fn finish(st: &mut BallistaApplicationStatus, s: &str, error: Option<String>) {
    st.application_state = ApplicationState {
        state: s.to_string(),
        error_message: error,
    };
    st.termination_time = Some(Utc::now().to_rfc3339());
}

async fn cleanup(clusters: &Api<BallistaCluster>, ephemeral: bool, app: &str) -> Result<(), Error> {
    if ephemeral {
        delete_if_exists(clusters, &ephemeral_cluster_name(app)).await?;
    }
    Ok(())
}

async fn save(
    apps: &Api<BallistaApplication>,
    name: &str,
    app: &BallistaApplication,
    st: BallistaApplicationStatus,
) -> Result<Action, Error> {
    if app.status.as_ref() != Some(&st) {
        let patch = json!({
            "apiVersion": "ballista.apache.org/v1alpha1",
            "kind": "BallistaApplication",
            "status": st,
        });
        apps.patch_status(
            name,
            &PatchParams::apply(r::MANAGER).force(),
            &Patch::Apply(&patch),
        )
        .await?;
    }
    let done = state::is_terminal(&st.application_state.state);
    Ok(if done {
        // Come back for the time to live, if any.
        Action::requeue(StdDuration::from_secs(1))
    } else {
        Action::requeue(StdDuration::from_secs(5))
    })
}

fn driver_status(pod: &Pod) -> Option<&k8s_openapi::api::core::v1::ContainerStatus> {
    pod.status
        .as_ref()?
        .container_statuses
        .as_ref()?
        .iter()
        .find(|c| c.name == "driver")
}

fn finished_at(pod: &Pod) -> Option<chrono::DateTime<Utc>> {
    let t = driver_status(pod)?.state.as_ref()?.terminated.as_ref()?;
    let ts = t.finished_at.as_ref()?.0;
    chrono::DateTime::<Utc>::from_timestamp(ts.as_second(), 0)
}

/// Exit code, reason and the end of the driver's log (termination message).
fn failure_reason(pod: &Pod) -> String {
    let Some(t) = driver_status(pod)
        .and_then(|c| c.state.as_ref())
        .and_then(|s| s.terminated.as_ref())
    else {
        return pod
            .status
            .as_ref()
            .and_then(|s| s.message.clone())
            .unwrap_or_else(|| "driver pod failed".to_string());
    };
    let mut msg = format!(
        "driver exited with code {} ({})",
        t.exit_code,
        t.reason.clone().unwrap_or_default()
    );
    if let Some(m) = &t.message {
        let tail: String = m
            .lines()
            .rev()
            .take(20)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect::<Vec<_>>()
            .join("\n");
        msg.push_str(":\n");
        msg.push_str(&tail);
    }
    msg
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn driver_pod_name_fits_63_chars() {
        let long = "a".repeat(80);
        let n = driver_pod_name(&long, 12);
        assert_eq!(n.len(), 63);
        assert!(n.ends_with("-driver-12"));
        assert_eq!(driver_pod_name("etl", 1), "etl-driver-1");
    }
}
