//! Reconciles a `BallistaCluster`: scheduler, connect and history Deployments and
//! Services (server-side apply), executor pods (static count or dynamic
//! allocation), and status.

use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration as StdDuration;

use chrono::{DateTime, Utc};
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{Pod, Service};
use kube::api::{Api, DeleteParams, ListParams, Patch, PatchParams, PostParams};
use kube::runtime::controller::Action;
use kube::{Client, Resource, ResourceExt};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::json;

use crate::autoscale::{self, ExecutorPod, Scaling};
use crate::crd::{BallistaCluster, BallistaClusterStatus, ExecutorCounts};
use crate::resources as r;
use crate::{Ctx, Error};

pub async fn reconcile(bc: Arc<BallistaCluster>, ctx: Arc<Ctx>) -> Result<Action, Error> {
    let ns = bc
        .namespace()
        .ok_or_else(|| Error::Invalid("BallistaCluster must be namespaced".into()))?;
    let name = bc.name_any();
    let owner = bc
        .controller_owner_ref(&())
        .ok_or_else(|| Error::Invalid("BallistaCluster has no uid yet".into()))?;
    let spec = &bc.spec;
    let client = &ctx.client;
    let deploys: Api<Deployment> = Api::namespaced(client.clone(), &ns);
    let services: Api<Service> = Api::namespaced(client.clone(), &ns);
    let pods: Api<Pod> = Api::namespaced(client.clone(), &ns);

    // Control plane.
    let (d, s) = r::scheduler(&name, &ns, spec, &owner);
    apply(&deploys, &d).await?;
    apply(&services, &s).await?;
    if spec.connect.enabled {
        let (d, s) = r::connect(&name, &ns, spec, &owner);
        apply(&deploys, &d).await?;
        apply(&services, &s).await?;
    } else {
        delete_if_exists(&deploys, &r::connect_name(&name)).await?;
        delete_if_exists(&services, &r::connect_name(&name)).await?;
    }
    if let Some((d, s)) = r::history(&name, &ns, spec, &owner) {
        apply(&deploys, &d).await?;
        apply(&services, &s).await?;
    } else {
        delete_if_exists(&deploys, &r::history_name(&name)).await?;
        delete_if_exists(&services, &r::history_name(&name)).await?;
    }
    let scheduler_ready = deployment_ready(&deploys, &r::scheduler_name(&name)).await?;
    let connect_ready =
        !spec.connect.enabled || deployment_ready(&deploys, &r::connect_name(&name)).await?;

    // Executor pods: replace finished ones, keep the rest.
    let listed = pods
        .list(&ListParams::default().labels(&r::selector(&name, "executor")))
        .await?;
    let mut live = vec![];
    for p in listed.items {
        if p.metadata.deletion_timestamp.is_some() {
            continue;
        }
        let phase = p
            .status
            .as_ref()
            .and_then(|s| s.phase.clone())
            .unwrap_or_default();
        if phase == "Succeeded" || phase == "Failed" {
            log::info!(
                "{ns}/{name}: removing finished executor {} ({phase})",
                p.name_any()
            );
            delete_if_exists(&pods, &p.name_any()).await?;
            continue;
        }
        live.push(p);
    }

    let scaling = if scheduler_ready {
        match fetch_scaling(client, &ns, &name).await {
            Ok(s) => Some(s),
            Err(e) => {
                log::warn!("{ns}/{name}: scheduler /api/scaling: {e}");
                None
            }
        }
    } else {
        None
    };

    let prev = bc.status.clone().unwrap_or_default();
    let now = Utc::now();
    let da = &spec.executor.dynamic_allocation;
    let (desired, create, remove, backlog_since) = if da.enabled {
        let views: Vec<ExecutorPod> = live
            .iter()
            .map(|p| ExecutorPod {
                name: p.name_any(),
                idle_since: p
                    .annotations()
                    .get(r::IDLE_SINCE_ANNOTATION)
                    .and_then(|v| parse_time(v)),
            })
            .collect();
        let d = autoscale::decide(
            now,
            da,
            spec.executor.vcores,
            &views,
            scaling.as_ref(),
            prev.backlog_since.as_deref().and_then(parse_time),
        );
        // Remember idle times on the pods themselves.
        for p in &views {
            let mark = d.idle_marks.get(&p.name).copied().flatten();
            if mark != p.idle_since && !d.remove.contains(&p.name) {
                let value = mark.map(|t| t.to_rfc3339());
                let patch = json!({"metadata": {"annotations": {r::IDLE_SINCE_ANNOTATION: value}}});
                pods.patch(&p.name, &PatchParams::default(), &Patch::Merge(&patch))
                    .await?;
            }
        }
        (d.desired, d.create, d.remove, d.backlog_since)
    } else {
        let want = spec.executor.instances;
        let current = live.len() as u32;
        // Shrinking a static cluster: remove executors with no running task first.
        let mut extra: Vec<String> = live.iter().map(|p| p.name_any()).collect();
        if let Some(s) = &scaling {
            let busy = |n: &String| {
                s.executors
                    .iter()
                    .any(|e| &e.id == n && e.running_tasks > 0)
            };
            extra.sort_by_key(busy);
        }
        extra.truncate(current.saturating_sub(want) as usize);
        (want, want.saturating_sub(current), extra, None)
    };

    for n in &remove {
        log::info!("{ns}/{name}: removing executor {n}");
        delete_if_exists(&pods, n).await?;
    }
    // Executors wait for the scheduler; create them once it is up.
    let create = if scheduler_ready { create } else { 0 };
    for _ in 0..create {
        let p = pods
            .create(
                &PostParams::default(),
                &r::executor_pod(&name, &ns, spec, &owner),
            )
            .await?;
        log::info!("{ns}/{name}: started executor {}", p.name_any());
    }

    let ready = live
        .iter()
        .filter(|p| !remove.contains(&p.name_any()) && pod_ready(p))
        .count() as u32;
    let current = live.len() as u32 - remove.len() as u32 + create;
    let (phase, message) = match (scheduler_ready, connect_ready) {
        (true, true) => ("Running", None),
        (false, _) => ("Pending", Some("waiting for the scheduler".to_string())),
        (true, false) => (
            "Pending",
            Some("waiting for the connect server".to_string()),
        ),
    };
    let scaled = create > 0 || !remove.is_empty();
    let status = BallistaClusterStatus {
        phase: Some(phase.to_string()),
        message,
        scheduler_url: Some(r::scheduler_url(&name, &ns)),
        connect_url: spec.connect.enabled.then(|| {
            format!(
                "sc://{}.{ns}.svc:{}",
                r::connect_name(&name),
                r::CONNECT_PORT
            )
        }),
        executors: ExecutorCounts {
            desired,
            current,
            ready,
            registered: scaling
                .as_ref()
                .map(|s| s.executors.iter().filter(|e| !e.terminating).count() as u32)
                .unwrap_or_default(),
            pending_vcores: scaling
                .as_ref()
                .map(|s| s.pending_vcores as u64)
                .unwrap_or_default(),
            busy_vcores: scaling.as_ref().map(|s| s.busy_vcores).unwrap_or_default(),
            total_vcores: scaling.as_ref().map(|s| s.total_vcores).unwrap_or_default(),
        },
        backlog_since: backlog_since.map(|t| t.to_rfc3339()),
        last_scale_time: if scaled {
            Some(now.to_rfc3339())
        } else {
            prev.last_scale_time.clone()
        },
        observed_generation: bc.metadata.generation,
    };
    if bc.status.as_ref() != Some(&status) {
        let api: Api<BallistaCluster> = Api::namespaced(client.clone(), &ns);
        let patch = json!({
            "apiVersion": "ballista.apache.org/v1alpha1",
            "kind": "BallistaCluster",
            "status": status,
        });
        api.patch_status(
            &name,
            &PatchParams::apply(r::MANAGER).force(),
            &Patch::Apply(&patch),
        )
        .await?;
    }

    Ok(Action::requeue(StdDuration::from_secs(if da.enabled {
        5
    } else {
        30
    })))
}

pub fn error_policy(bc: Arc<BallistaCluster>, err: &Error, _: Arc<Ctx>) -> Action {
    log::warn!("reconciling BallistaCluster {}: {err}", bc.name_any());
    Action::requeue(StdDuration::from_secs(10))
}

/// `GET /api/scaling` through the API server's service proxy, so the operator
/// reaches the scheduler both in the cluster and from a laptop.
async fn fetch_scaling(client: &Client, ns: &str, cluster: &str) -> Result<Scaling, Error> {
    let path = format!(
        "/api/v1/namespaces/{ns}/services/{}:{}/proxy/api/scaling",
        r::scheduler_name(cluster),
        r::SCHEDULER_PORT
    );
    let req = http::Request::get(path)
        .body(vec![])
        .map_err(|e| Error::Invalid(e.to_string()))?;
    Ok(client.request::<Scaling>(req).await?)
}

pub(crate) async fn apply<K>(api: &Api<K>, obj: &K) -> Result<K, Error>
where
    K: Resource + Clone + DeserializeOwned + Serialize + Debug,
{
    let name = obj
        .meta()
        .name
        .clone()
        .ok_or_else(|| Error::Invalid("object without a name".into()))?;
    Ok(api
        .patch(
            &name,
            &PatchParams::apply(r::MANAGER).force(),
            &Patch::Apply(obj),
        )
        .await?)
}

pub(crate) async fn delete_if_exists<K>(api: &Api<K>, name: &str) -> Result<(), Error>
where
    K: Resource + Clone + DeserializeOwned + Debug,
{
    match api.delete(name, &DeleteParams::default()).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(s)) if s.code == 404 => Ok(()),
        Err(e) => Err(e.into()),
    }
}

async fn deployment_ready(api: &Api<Deployment>, name: &str) -> Result<bool, Error> {
    Ok(api
        .get_opt(name)
        .await?
        .and_then(|d| d.status)
        .and_then(|s| s.ready_replicas)
        .unwrap_or(0)
        >= 1)
}

fn pod_ready(p: &Pod) -> bool {
    p.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .is_some_and(|c| c.iter().any(|c| c.type_ == "Ready" && c.status == "True"))
}

pub(crate) fn parse_time(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|t| t.with_timezone(&Utc))
}
