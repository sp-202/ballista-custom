// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::api::dto_build;
use crate::scheduler_server::event::QueryStageSchedulerEvent;
use crate::state::execution_graph_dot::ExecutionGraphDot;
use crate::{api::SchedulerErrorResponse, scheduler_server::SchedulerServer};
use axum::extract::Query;
use axum::response::Redirect;
use axum::{
    Json,
    extract::{Path, State},
    response::{IntoResponse, Response},
};
use ballista_api_types::dto::{JobConfig, JobResponse, PlanFormat, QueryStagesResponse};
use ballista_core::BALLISTA_VERSION;
use ballista_core::serde::protobuf::job_status::Status;
use ballista_core::serde::protobuf::{ExecutorMetric, executor_metric::Metric};
use ballista_core::serde::scheduler::{
    ExecutorOperatingSystemSpecification, ExecutorSpecification,
};
use ballista_core::utils::get_current_time;
use datafusion::DATAFUSION_VERSION;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;
#[cfg(feature = "graphviz-support")]
use graphviz_rust::{
    cmd::{CommandArg, Format},
    exec,
    printer::PrinterContext,
};
use http::{HeaderMap, StatusCode, header::CONTENT_TYPE};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct SchedulerStateResponse {
    pub started: u128,
    /// Scheduler identity exposed in scheduler state.
    pub scheduler_id: String,
    pub version: &'static str,
    pub datafusion_version: &'static str,
    pub substrait_support: bool,
    pub keda_support: bool,
    pub prometheus_support: bool,
    pub graphviz_support: bool,
    pub scheduling_policy: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub advertise_flight_endpoint: Option<String>,
    pub enable_embedded_flight_proxy: bool,
}

#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct SchedulerVersionResponse {
    pub version: &'static str,
    pub datafusion_version: &'static str,
}

#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct ExecutorResponse {
    pub id: String,
    pub host: String,
    pub port: u16,
    pub last_seen: Option<u128>,
    pub specification: ExecutorSpecification,
    pub metrics: Vec<ExecutorMetricResponse>,
    pub os_info: ExecutorOperatingSystemSpecification,
}

/// What an executor autoscaler needs to know, in vcore units (like Spark's
/// `ExecutorAllocationManager`: pending + running task demand vs. slots).
#[derive(Debug, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct ScalingResponse {
    /// Jobs accepted but not yet planned into stages.
    pub pending_jobs: usize,
    /// Jobs with an execution graph (planned, not finished).
    pub running_jobs: usize,
    /// Input partitions of running stages not yet handed to a task; each needs
    /// one vcore to run.
    pub pending_vcores: usize,
    /// Tasks currently running.
    pub running_tasks: usize,
    /// vcores held by running tasks.
    pub busy_vcores: u64,
    /// vcores of executors that accept new tasks (not terminating).
    pub total_vcores: u64,
    pub executors: Vec<ExecutorScalingResponse>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct ExecutorScalingResponse {
    pub id: String,
    pub host: String,
    pub vcores: u32,
    pub running_tasks: usize,
    pub busy_vcores: u64,
    /// Shutting down: the scheduler sends it no new tasks.
    pub terminating: bool,
    /// Holds task output of a job that is still running. Removing the executor
    /// makes the scheduler recompute that output (like Spark's shuffle tracking).
    pub holds_shuffle_data: bool,
    /// Seconds since the epoch of the last heartbeat.
    pub last_seen: Option<u64>,
}

#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
#[allow(clippy::enum_variant_names)]
pub enum ExecutorMetricResponse {
    AvailableMemory(u64),
    TotalMemory(u64),
    UsedMemory(u64),
    ProcPhysicalMemory(u64),
    ProcVirtualMemory(u64),
    PeakPhysicalMemory(u64),
    PeakVirtualMemory(u64),
    MemoryPoolReserved(u64),
    MemoryPoolSize(u64),
    AllocatedMemory(u64),
}

impl ExecutorMetricResponse {
    pub fn from_proto(proto_spec: ExecutorMetric) -> Option<Self> {
        proto_spec.metric.map(|inner| match inner {
            Metric::AvailableMemory(v) => Self::AvailableMemory(v),
            Metric::TotalMemory(v) => Self::TotalMemory(v),
            Metric::UsedMemory(v) => Self::UsedMemory(v),
            Metric::ProcPhysicalMemory(v) => Self::ProcPhysicalMemory(v),
            Metric::ProcVirtualMemory(v) => Self::ProcVirtualMemory(v),
            Metric::PeakPhysicalMemory(v) => Self::PeakPhysicalMemory(v),
            Metric::PeakVirtualMemory(v) => Self::PeakVirtualMemory(v),
            Metric::MemoryPoolReserved(v) => Self::MemoryPoolReserved(v),
            Metric::MemoryPoolSize(v) => Self::MemoryPoolSize(v),
            Metric::AllocatedMemory(v) => Self::AllocatedMemory(v),
        })
    }
}

#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct CancelJobResponse {
    pub cancelled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, serde::Deserialize, Default, utoipa::IntoParams, utoipa::ToSchema)]
#[into_params(parameter_in = Query)]
pub struct JobQueryParams {
    /// Controls plan format
    pub plan_format: Option<PlanFormat>,
}

/// A handler for GET requests to the root (`/`).
/// It redirects to `https://nightlies.apache.org/datafusion/ballista/tui/<BALLISTA_VERSION>/`
/// forwarding any query parameters
pub async fn get_webtui<
    T: AsLogicalPlan + Clone + Send + Sync + 'static,
    U: AsExecutionPlan + Send + Sync + 'static,
>(
    header_map: HeaderMap,
    Query(mut params): Query<HashMap<String, String>>,
    State(data_server): State<Arc<SchedulerServer<T, U>>>,
) -> Result<Redirect, (StatusCode, String)> {
    const NIGHTLIES_URL: &str = "https://nightlies.apache.org/datafusion/ballista/tui";
    let external_host = &data_server.state.config.external_host;
    let bind_port = data_server.state.config.bind_port;

    let ballista_scheduler_url =
        params.remove("ballista_scheduler_url").unwrap_or_else(|| {
            let proto = header_map
                .get("x-forwarded-proto")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("http");
            let host = header_map
                .get("x-forwarded-host")
                .and_then(|hv| hv.to_str().ok())
                .unwrap_or(external_host);
            let port = header_map
                .get("x-forwarded-port")
                .and_then(|hv| hv.to_str().ok())
                .and_then(|v| v.parse::<u16>().ok())
                .unwrap_or(bind_port);
            format!("{proto}://{host}:{port}")
        });

    let mut query_string = String::new();
    query_string.push_str(&format!(
        "ballista_scheduler_url={}",
        url_escape::encode_query(&ballista_scheduler_url)
    ));

    for (k, v) in params.iter() {
        query_string.push_str(&format!(
            "&{}={}",
            url_escape::encode_query(k),
            url_escape::encode_query(v)
        ));
    }

    let target = format!("{NIGHTLIES_URL}/{BALLISTA_VERSION}/?{query_string}");

    Ok(Redirect::to(&target))
}

#[utoipa::path(
    get,
    path = "/api/scheduler/config",
    tag = "state",
    responses(
        (status = 200, description = "The scheduler's own configuration, as name/value pairs", body = JobConfig)
    )
)]
pub async fn get_scheduler_config<
    T: AsLogicalPlan + Clone + Send + Sync + 'static,
    U: AsExecutionPlan + Send + Sync + 'static,
>(
    State(data_server): State<Arc<SchedulerServer<T, U>>>,
) -> impl IntoResponse {
    let c = &data_server.state.config;
    let mut entries = JobConfig::new();
    let mut put = |name: &str, value: String| {
        entries.insert(name.to_string(), value);
    };
    put("scheduler_id", c.scheduler_id.clone());
    put("namespace", c.namespace.clone());
    put("external_host", c.external_host.clone());
    put("bind_host", c.bind_host.clone());
    put("bind_port", c.bind_port.to_string());
    put("scheduling_policy", c.scheduling_policy.to_string());
    put("task_distribution", format!("{:?}", c.task_distribution));
    put("event_loop_buffer_size", c.event_loop_buffer_size.to_string());
    put(
        "finished_job_data_clean_up_interval_seconds",
        c.finished_job_data_clean_up_interval_seconds.to_string(),
    );
    put(
        "finished_job_state_clean_up_interval_seconds",
        c.finished_job_state_clean_up_interval_seconds.to_string(),
    );
    put("executor_timeout_seconds", c.executor_timeout_seconds.to_string());
    put(
        "expire_dead_executor_interval_seconds",
        c.expire_dead_executor_interval_seconds.to_string(),
    );
    put(
        "executor_termination_grace_period",
        c.executor_termination_grace_period.to_string(),
    );
    put(
        "graceful_shutdown_timeout_seconds",
        c.graceful_shutdown_timeout_seconds.to_string(),
    );
    put(
        "no_executors_grace_period_seconds",
        c.no_executors_grace_period_seconds.to_string(),
    );
    put("min_ready_executors", c.min_ready_executors.to_string());
    put("task_max_failures", c.task_max_failures.to_string());
    put("stage_max_failures", c.stage_max_failures.to_string());
    put(
        "grpc_server_max_decoding_message_size",
        c.grpc_server_max_decoding_message_size.to_string(),
    );
    put(
        "grpc_server_max_encoding_message_size",
        c.grpc_server_max_encoding_message_size.to_string(),
    );
    put(
        "grpc_client_max_message_size",
        c.grpc_client_max_message_size.to_string(),
    );
    put("use_tls", c.use_tls.to_string());
    #[cfg(feature = "flight-sql")]
    put("flight_sql", c.flight_sql.to_string());
    #[allow(deprecated)]
    put(
        "enable_embedded_flight_proxy",
        c.enable_embedded_flight_proxy.to_string(),
    );
    if let Some(v) = &c.advertise_flight_endpoint {
        put("advertise_flight_endpoint", v.clone());
    }
    if let Some(v) = c.job_resubmit_interval_ms {
        put("job_resubmit_interval_ms", v.to_string());
    }
    Json(entries)
}

#[utoipa::path(
    get,
    path = "/api/state",
    tag = "state",
    responses(
        (status = 200, description = "Scheduler state and feature configuration", body = SchedulerStateResponse)
    )
)]
pub async fn get_scheduler_state<
    T: AsLogicalPlan + Clone + Send + Sync + 'static,
    U: AsExecutionPlan + Send + Sync + 'static,
>(
    State(data_server): State<Arc<SchedulerServer<T, U>>>,
) -> impl IntoResponse {
    #[allow(deprecated)]
    let response = SchedulerStateResponse {
        started: data_server.start_time,
        scheduler_id: data_server.scheduler_id.clone(),
        version: BALLISTA_VERSION,
        datafusion_version: DATAFUSION_VERSION,
        substrait_support: cfg!(feature = "substrait"),
        keda_support: cfg!(feature = "keda-scaler"),
        prometheus_support: cfg!(feature = "prometheus-metrics"),
        graphviz_support: cfg!(feature = "graphviz-support"),
        scheduling_policy: data_server.state.config.scheduling_policy.to_string(),
        advertise_flight_endpoint: data_server
            .state
            .config
            .advertise_flight_endpoint
            .clone(),
        enable_embedded_flight_proxy: data_server
            .state
            .config
            .enable_embedded_flight_proxy,
    };
    Json(response)
}

#[utoipa::path(
    get,
    path = "/api/version",
    tag = "version",
    responses(
        (status = 200, description = "Scheduler and DataFusion version information", body = SchedulerVersionResponse)
    )
)]
pub async fn get_scheduler_version() -> impl IntoResponse {
    let response = SchedulerVersionResponse {
        version: BALLISTA_VERSION,
        datafusion_version: DATAFUSION_VERSION,
    };
    Json(response)
}

#[utoipa::path(
    get,
    path = "/api/executors",
    tag = "executors",
    responses(
        (status = 200, description = "List of registered executors", body = Vec<ExecutorResponse>)
    )
)]
pub async fn get_executors<
    T: AsLogicalPlan + Clone + Send + Sync + 'static,
    U: AsExecutionPlan + Send + Sync + 'static,
>(
    State(data_server): State<Arc<SchedulerServer<T, U>>>,
) -> impl IntoResponse {
    let state = &data_server.state;
    let executors: Vec<ExecutorResponse> = state
        .executor_manager
        .get_executors_state()
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|(metadata, duration, metrics)| ExecutorResponse {
            id: metadata.id,
            host: metadata.host,
            port: metadata.port,
            last_seen: duration.map(|d| d.as_millis()),
            specification: metadata.specification,
            metrics: metrics
                .into_iter()
                .filter_map(ExecutorMetricResponse::from_proto)
                .collect(),
            os_info: metadata.os_info,
        })
        .collect();

    Json(executors)
}

#[utoipa::path(
    get,
    path = "/api/scaling",
    tag = "executors",
    responses(
        (status = 200, description = "Task demand and executor usage for autoscalers", body = ScalingResponse)
    )
)]
pub async fn get_scaling<
    T: AsLogicalPlan + Clone + Send + Sync + 'static,
    U: AsExecutionPlan + Send + Sync + 'static,
>(
    State(data_server): State<Arc<SchedulerServer<T, U>>>,
) -> impl IntoResponse {
    Json(scaling_snapshot(&data_server).await)
}

pub(crate) async fn scaling_snapshot<
    T: AsLogicalPlan + Clone + Send + Sync + 'static,
    U: AsExecutionPlan + Send + Sync + 'static,
>(
    data_server: &SchedulerServer<T, U>,
) -> ScalingResponse {
    use crate::state::execution_stage::ExecutionStage;
    use ballista_core::serde::protobuf::{executor_status, task_status};
    use std::collections::HashSet;

    let state = &data_server.state;
    let mut busy: HashMap<String, (usize, u64)> = HashMap::new();
    let mut holds: HashSet<String> = HashSet::new();
    let mut pending_vcores = 0;
    let jobs = state.task_manager.get_running_job_cache();
    for job in jobs.values() {
        let graph = job.execution_graph.read().await;
        pending_vcores += graph.available_tasks();
        for stage in graph.stages().values() {
            let infos = match stage {
                ExecutionStage::Running(s) => &s.task_infos,
                ExecutionStage::Successful(s) => &s.task_infos,
                _ => continue,
            };
            for info in infos {
                match &info.task_status {
                    task_status::Status::Running(r) => {
                        let e = busy.entry(r.executor_id.clone()).or_default();
                        e.0 += 1;
                        e.1 += u64::from(info.vcores_consumed);
                    }
                    task_status::Status::Successful(s) => {
                        holds.insert(s.executor_id.clone());
                    }
                    task_status::Status::Failed(_) => {}
                }
            }
        }
    }

    let mut executors = vec![];
    for (metadata, _, _) in state
        .executor_manager
        .get_executors_state()
        .await
        .unwrap_or_default()
    {
        let heartbeat = state.executor_manager.get_executor_hearbeat(&metadata.id);
        let terminating = matches!(
            heartbeat
                .as_ref()
                .and_then(|hb| hb.status.as_ref())
                .and_then(|s| s.status.as_ref()),
            Some(executor_status::Status::Terminating(_))
                | Some(executor_status::Status::Dead(_))
        );
        let (running_tasks, busy_vcores) =
            busy.get(&metadata.id).copied().unwrap_or_default();
        executors.push(ExecutorScalingResponse {
            holds_shuffle_data: holds.contains(&metadata.id),
            id: metadata.id,
            host: metadata.host,
            vcores: metadata.specification.vcores,
            running_tasks,
            busy_vcores,
            terminating,
            last_seen: heartbeat.map(|hb| hb.timestamp),
        });
    }
    executors.sort_by(|a, b| a.id.cmp(&b.id));

    ScalingResponse {
        pending_jobs: state.task_manager.pending_job_number(),
        running_jobs: jobs.len(),
        pending_vcores,
        running_tasks: executors.iter().map(|e| e.running_tasks).sum(),
        busy_vcores: executors.iter().map(|e| e.busy_vcores).sum(),
        total_vcores: executors
            .iter()
            .filter(|e| !e.terminating)
            .map(|e| u64::from(e.vcores))
            .sum(),
        executors,
    }
}

#[utoipa::path(
    get,
    path = "/api/executor/{executor_id}",
    tag = "executors",
    params(
        ("executor_id" = String, Path, description = "Unique executor identifier")
    ),
    responses(
        (status = 200, description = "Executor details", body = ExecutorResponse),
        (status = 404, description = "Executor not found", body = SchedulerErrorResponse)
    )
)]
pub async fn get_executor_info<
    T: AsLogicalPlan + Clone + Send + Sync + 'static,
    U: AsExecutionPlan + Send + Sync + 'static,
>(
    State(data_server): State<Arc<SchedulerServer<T, U>>>,
    Path(executor_id): Path<String>,
) -> Result<impl IntoResponse, SchedulerErrorResponse> {
    let state = &data_server.state;
    let executor_info = state
        .executor_manager
        .get_executors_state()
        .await
        .unwrap_or_default()
        .into_iter()
        .find(|(metadata, _, _)| metadata.id == executor_id)
        .map(|(metadata, duration, metrics)| ExecutorResponse {
            id: metadata.id,
            host: metadata.host,
            port: metadata.port,
            last_seen: duration.map(|d| d.as_millis()),
            specification: metadata.specification,
            metrics: metrics
                .into_iter()
                .filter_map(ExecutorMetricResponse::from_proto)
                .collect(),
            os_info: metadata.os_info,
        });

    executor_info
        .map(Json)
        .ok_or(SchedulerErrorResponse::new(StatusCode::NOT_FOUND))
}

#[utoipa::path(
    get,
    path = "/api/jobs",
    tag = "jobs",
    responses(
        (status = 200, description = "List of all jobs", body = Vec<JobResponse>),
        (status = 500, description = "Internal server error", body = SchedulerErrorResponse)
    )
)]
pub async fn get_jobs<
    T: AsLogicalPlan + Clone + Send + Sync + 'static,
    U: AsExecutionPlan + Send + Sync + 'static,
>(
    State(data_server): State<Arc<SchedulerServer<T, U>>>,
) -> Result<impl IntoResponse, SchedulerErrorResponse> {
    let state = &data_server.state;

    let jobs = state.task_manager.get_all_jobs().await.map_err(|e| {
        tracing::error!("Error occurred while getting jobs, reason: {e:?}");
        SchedulerErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR)
    })?;

    let jobs: Vec<JobResponse> = jobs
        .iter()
        .map(dto_build::job_overview_to_response)
        .collect();

    Ok(Json(jobs))
}

#[utoipa::path(
    get,
    path = "/api/job/{job_id}",
    tag = "jobs",
    params(
        ("job_id" = String, Path, description = "Unique job identifier"),
        JobQueryParams
    ),
    responses(
        (status = 200, description = "Job details", body = JobResponse),
        (status = 404, description = "Job not found", body = SchedulerErrorResponse),
        (status = 500, description = "Internal server error", body = SchedulerErrorResponse)
    )
)]
pub async fn get_job<
    T: AsLogicalPlan + Clone + Send + Sync + 'static,
    U: AsExecutionPlan + Send + Sync + 'static,
>(
    State(data_server): State<Arc<SchedulerServer<T, U>>>,
    Path(job_id): Path<String>,
    query: Query<JobQueryParams>,
) -> Result<impl IntoResponse, SchedulerErrorResponse> {
    let graph = data_server
        .state
        .task_manager
        .get_job_execution_graph(&job_id.clone().into())
        .await
        .map_err(|err| {
            tracing::error!("Error occurred while getting the execution graph for job '{job_id}' reason: {err:?}");
            SchedulerErrorResponse::with_error(StatusCode::INTERNAL_SERVER_ERROR, format!("Error occurred while getting the execution graph for job '{job_id}'"))
        })?
        .ok_or_else(|| SchedulerErrorResponse::new(StatusCode::NOT_FOUND))?;

    Ok(Json(dto_build::graph_to_job_response(
        &graph,
        query.plan_format.unwrap_or_default(),
    )))
}

#[utoipa::path(
    patch,
    path = "/api/job/{job_id}",
    tag = "jobs",
    params(
        ("job_id" = String, Path, description = "Unique job identifier")
    ),
    responses(
        (status = 200, description = "Job cancellation accepted", body = CancelJobResponse),
        (status = 404, description = "Job not found", body = SchedulerErrorResponse),
        (status = 409, description = "Job is in terminal state", body = CancelJobResponse),
        (status = 500, description = "Internal server error", body = SchedulerErrorResponse)
    )
)]
pub async fn cancel_job<
    T: AsLogicalPlan + Clone + Send + Sync + 'static,
    U: AsExecutionPlan + Send + Sync + 'static,
>(
    State(data_server): State<Arc<SchedulerServer<T, U>>>,
    Path(job_id): Path<String>,
) -> Result<impl IntoResponse, SchedulerErrorResponse> {
    // 404 if the job doesn't exist
    let job_status = data_server
        .state
        .task_manager
        .get_job_status(&job_id.clone().into())
        .await
        .map_err(|err| {
            tracing::error!("Error getting job status: {err:?}");
            SchedulerErrorResponse::with_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Error getting job status: {err}"),
            )
        })?
        .ok_or_else(|| SchedulerErrorResponse::new(StatusCode::NOT_FOUND))?;

    match &job_status.status {
        None | Some(Status::Queued(_)) | Some(Status::Running(_)) => {
            data_server
                .query_stage_event_loop
                .get_sender()
                .map_err(|err| {
                    tracing::error!(
                        "Error getting query stage event loop sender: {err:?}"
                    );
                    SchedulerErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR)
                })?
                .post_event(QueryStageSchedulerEvent::JobCancel(job_id.into()))
                .await
                .map_err(|_| {
                    SchedulerErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR)
                })?;

            Ok((
                StatusCode::OK,
                Json(CancelJobResponse {
                    cancelled: true,
                    reason: None,
                }),
            )
                .into_response())
        }
        Some(Status::Failed(_)) => Ok((
            StatusCode::CONFLICT,
            Json(CancelJobResponse {
                cancelled: false,
                reason: Some("The job has failed".into()),
            }),
        )
            .into_response()),
        Some(Status::Successful(_)) => Ok((
            StatusCode::CONFLICT,
            Json(CancelJobResponse {
                cancelled: false,
                reason: Some("The job is already completed".into()),
            }),
        )
            .into_response()),
    }
}

#[utoipa::path(
    get,
    path = "/api/job/{job_id}/stages",
    tag = "jobs",
    params(
        ("job_id" = String, Path, description = "Unique job identifier"),
        JobQueryParams
    ),
    responses(
        (status = 200, description = "Query stages summary", body = QueryStagesResponse),
        (status = 404, description = "Job not found", body = SchedulerErrorResponse),
        (status = 500, description = "Internal server error", body = SchedulerErrorResponse)
    )
)]
pub async fn get_query_stages<
    T: AsLogicalPlan + Clone + Send + Sync + 'static,
    U: AsExecutionPlan + Send + Sync + 'static,
>(
    State(data_server): State<Arc<SchedulerServer<T, U>>>,
    Path(job_id): Path<String>,
    query: Query<JobQueryParams>,
) -> Result<impl IntoResponse, SchedulerErrorResponse> {
    if let Some(graph) = data_server
        .state
        .task_manager
        .get_job_execution_graph(&job_id.clone().into())
        .await
        .map_err(|e| {
            tracing::error!("Error occurred while getting the query stages for job '{job_id}' reason: {e:?}");
            SchedulerErrorResponse::with_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Error occurred while getting the query stages for job '{job_id}'"),
            )
        })?
    {
        Ok(Json(dto_build::graph_to_query_stages(
            &graph,
            query.plan_format.unwrap_or_default(),
            get_current_time(),
        )))
    } else {
        Err(SchedulerErrorResponse::new(StatusCode::NOT_FOUND))
    }
}

#[utoipa::path(
    get,
    path = "/api/job/{job_id}/dot",
    tag = "graphs",
    params(
        ("job_id" = String, Path, description = "Unique job identifier")
    ),
    responses(
        (status = 200, description = "Job execution graph in Graphviz DOT format", content_type = "text/plain", body = String),
        (status = 404, description = "Job not found", body = SchedulerErrorResponse),
        (status = 500, description = "Internal server error", body = SchedulerErrorResponse)
    )
)]
pub async fn get_job_dot_graph<
    T: AsLogicalPlan + Clone + Send + Sync + 'static,
    U: AsExecutionPlan + Send + Sync + 'static,
>(
    State(data_server): State<Arc<SchedulerServer<T, U>>>,
    Path(job_id): Path<String>,
) -> Result<String, SchedulerErrorResponse> {
    if let Some(graph) = data_server
        .state
        .task_manager
        .get_job_execution_graph(&job_id.clone().into())
        .await
        .map_err(|e| {
            tracing::error!("Error occurred while getting the dot graph for job '{job_id}' reason: {e:?}");
            SchedulerErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR)
        })?
    {
        ExecutionGraphDot::generate(graph.as_ref())
            .map_err(|e| {
                tracing::error!("Error occurred while getting the dot graph for job '{job_id}' reason: {e:?}");
                SchedulerErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR)
            })
    } else {
        Err(SchedulerErrorResponse::new(StatusCode::NOT_FOUND))
    }
}

#[utoipa::path(
    get,
    path = "/api/job/{job_id}/stage/{stage_id}/dot",
    tag = "graphs",
    params(
        ("job_id" = String, Path, description = "Unique job identifier"),
        ("stage_id" = usize, Path, description = "Stage identifier")
    ),
    responses(
        (status = 200, description = "Query stage graph in Graphviz DOT format", content_type = "text/plain", body = String),
        (status = 404, description = "Job or stage not found", body = SchedulerErrorResponse),
        (status = 500, description = "Internal server error", body = SchedulerErrorResponse)
    )
)]
pub async fn get_query_stage_dot_graph<
    T: AsLogicalPlan + Clone + Send + Sync + 'static,
    U: AsExecutionPlan + Send + Sync + 'static,
>(
    State(data_server): State<Arc<SchedulerServer<T, U>>>,
    Path((job_id, stage_id)): Path<(String, usize)>,
) -> Result<impl IntoResponse, SchedulerErrorResponse> {
    if let Some(graph) = data_server
        .state
        .task_manager
        .get_job_execution_graph(&job_id.clone().into())
        .await
        .map_err(|_| SchedulerErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR))?
    {
        ExecutionGraphDot::generate_for_query_stage(graph.as_ref(), stage_id)
            .map_err(|_| SchedulerErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR))
    } else {
        Err(SchedulerErrorResponse::new(StatusCode::NOT_FOUND))
    }
}
#[cfg(feature = "graphviz-support")]
#[utoipa::path(
    get,
    path = "/api/job/{job_id}/dot_svg",
    tag = "graphs",
    params(
        ("job_id" = String, Path, description = "Unique job identifier")
    ),
    responses(
        (status = 200, description = "Job execution graph rendered as SVG", content_type = "image/svg+xml", body = String),
        (status = 400, description = "DOT parsing or graphviz execution error", body = SchedulerErrorResponse),
        (status = 404, description = "Job not found", body = SchedulerErrorResponse),
        (status = 500, description = "Internal server error", body = SchedulerErrorResponse)
    )
)]
pub async fn get_job_svg_graph<
    T: AsLogicalPlan + Clone + Send + Sync + 'static,
    U: AsExecutionPlan + Send + Sync + 'static,
>(
    State(data_server): State<Arc<SchedulerServer<T, U>>>,
    Path(job_id): Path<String>,
) -> Result<impl IntoResponse, SchedulerErrorResponse> {
    let dot = get_job_dot_graph(State(data_server.clone()), Path(job_id.clone())).await?;
    match graphviz_rust::parse(&dot) {
        Ok(graph) => {
            let result = exec(
                graph,
                &mut PrinterContext::default(),
                vec![CommandArg::Format(Format::Svg)],
            )
                .map_err(|e| {
                    tracing::error!("Error occurred while getting job svg graph for job '{job_id}' reason: {e:?}");
                    SchedulerErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR)
                })?;

            let svg = String::from_utf8_lossy(&result).to_string();
            Ok(Response::builder()
                .header(CONTENT_TYPE, "image/svg+xml")
                .body(svg)
                .unwrap())
        }
        Err(e) => Err(SchedulerErrorResponse::with_error(
            StatusCode::BAD_REQUEST,
            e.to_string(),
        )),
    }
}

#[utoipa::path(
    get,
    path = "/api/metrics",
    tag = "metrics",
    responses(
        (status = 200, description = "Prometheus text metrics", content_type = "text/plain; version=0.0.4; charset=utf-8", body = String),
        (status = 204, description = "No metrics collected"),
        (status = 500, description = "Internal server error")
    )
)]
pub async fn get_scheduler_metrics<
    T: AsLogicalPlan + Clone + Send + Sync + 'static,
    U: AsExecutionPlan + Send + Sync + 'static,
>(
    State(data_server): State<Arc<SchedulerServer<T, U>>>,
) -> impl IntoResponse {
    match data_server.metrics_collector().gather_metrics() {
        Ok(Some((data, content_type))) => Response::builder()
            .header(CONTENT_TYPE, content_type)
            .body(axum::body::Body::from(data))
            .unwrap(),
        Ok(None) => Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(axum::body::Body::empty())
            .unwrap(),
        Err(_) => Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(axum::body::Body::empty())
            .unwrap(),
    }
}

#[utoipa::path(
    get,
    path = "/api/job/{job_id}/config",
    tag = "jobs",
    params(
        ("job_id" = String, Path, description = "Unique job identifier")
    ),
    responses(
        (status = 200, description = "Job session configuration properties", body = std::collections::BTreeMap<String, String>),
        (status = 404, description = "Job not found", body = SchedulerErrorResponse)
    )
)]
pub async fn get_job_config<
    T: AsLogicalPlan + Clone + Send + Sync + 'static,
    U: AsExecutionPlan + Send + Sync + 'static,
>(
    State(data_server): State<Arc<SchedulerServer<T, U>>>,
    Path(job_id): Path<String>,
) -> Result<impl IntoResponse, SchedulerErrorResponse> {
    data_server
        .state
        .task_manager
        .get_job_config(&job_id.clone().into())
        .await
        .map(|e| Json(e.to_props()))
        .map_err(|_| SchedulerErrorResponse::new(StatusCode::NOT_FOUND))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The executor memory metrics' JSON names are part of the REST contract.
    #[test]
    fn executor_memory_metrics_serialize_with_stable_names() {
        use ballista_core::serde::protobuf::ExecutorMetric;

        let json = |metric| {
            let response = ExecutorMetricResponse::from_proto(ExecutorMetric {
                metric: Some(metric),
            })
            .unwrap();
            serde_json::to_value(response).unwrap()
        };
        assert_eq!(
            json(Metric::MemoryPoolReserved(1)),
            serde_json::json!({"type": "memory_pool_reserved", "value": 1})
        );
        assert_eq!(
            json(Metric::MemoryPoolSize(2)),
            serde_json::json!({"type": "memory_pool_size", "value": 2})
        );
        assert_eq!(
            json(Metric::AllocatedMemory(3)),
            serde_json::json!({"type": "allocated_memory", "value": 3})
        );
    }

    #[tokio::test]
    // Still covers the deprecated embedded-proxy flag until it is removed.
    #[allow(deprecated)]
    async fn scheduler_state_reflects_server_configuration() {
        use crate::config::SchedulerConfig;
        use crate::metrics::default_metrics_collector;
        use crate::test_utils::test_cluster_context;
        use axum::response::IntoResponse;
        use ballista_core::serde::BallistaCodec;
        use datafusion::DATAFUSION_VERSION;
        use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};

        let config = SchedulerConfig::default()
            .with_scheduler_id("scheduler-a")
            .with_advertise_flight_endpoint(Some("flight.example.com:50055".into()))
            .with_enable_embedded_flight_proxy(true);
        let expected_scheduling_policy = config.scheduling_policy.to_string();
        let server: Arc<SchedulerServer<LogicalPlanNode, PhysicalPlanNode>> =
            Arc::new(SchedulerServer::new(
                "localhost:50050".to_owned(),
                test_cluster_context(),
                BallistaCodec::default(),
                Arc::new(config),
                default_metrics_collector().unwrap(),
            ));

        let response = get_scheduler_state(State(server)).await.into_response();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert!(body["started"].is_number());
        assert_eq!(body["scheduler_id"], "scheduler-a");
        assert_eq!(body["version"], BALLISTA_VERSION);
        assert_eq!(body["datafusion_version"], DATAFUSION_VERSION);
        assert_eq!(body["scheduling_policy"], expected_scheduling_policy);
        assert_eq!(
            body["advertise_flight_endpoint"],
            "flight.example.com:50055"
        );
        assert_eq!(body["enable_embedded_flight_proxy"], true);
    }

    mod get_webtui {
        use super::*;
        use crate::config::SchedulerConfig;
        use crate::metrics::default_metrics_collector;
        use crate::test_utils::test_cluster_context;
        use axum::response::IntoResponse;
        use ballista_core::serde::BallistaCodec;
        use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};

        fn test_scheduler(
            config: SchedulerConfig,
        ) -> Arc<SchedulerServer<LogicalPlanNode, PhysicalPlanNode>> {
            let server = SchedulerServer::new(
                "localhost:50050".to_owned(),
                test_cluster_context(),
                BallistaCodec::default(),
                Arc::new(config),
                default_metrics_collector().unwrap(),
            );
            Arc::new(server)
        }

        fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
            let mut map = HeaderMap::new();
            for (k, v) in pairs {
                map.insert(
                    http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                    v.parse().unwrap(),
                );
            }
            map
        }

        fn location_of(result: Result<Redirect, (StatusCode, String)>) -> String {
            let response = result.expect("handler should not error").into_response();
            assert_eq!(response.status(), StatusCode::SEE_OTHER);
            response
                .headers()
                .get("location")
                .expect("redirect response must have a location header")
                .to_str()
                .unwrap()
                .to_owned()
        }

        #[tokio::test]
        async fn defaults_to_config_external_host_and_bind_port() {
            let scheduler = test_scheduler(SchedulerConfig::default());

            let result =
                get_webtui(HeaderMap::new(), Query(HashMap::new()), State(scheduler))
                    .await;

            let location = location_of(result);
            assert!(
                location.contains("ballista_scheduler_url=http://localhost:50050"),
                "unexpected location: {location}"
            );
            assert!(
                location
                    .starts_with("https://nightlies.apache.org/datafusion/ballista/tui/")
            );
        }

        #[tokio::test]
        async fn uses_custom_external_host_and_bind_port_from_config() {
            let scheduler = test_scheduler(SchedulerConfig {
                external_host: "scheduler.example.com".into(),
                bind_port: 8080,
                ..Default::default()
            });

            let result =
                get_webtui(HeaderMap::new(), Query(HashMap::new()), State(scheduler))
                    .await;

            let location = location_of(result);
            assert!(
                location
                    .contains("ballista_scheduler_url=http://scheduler.example.com:8080"),
                "unexpected location: {location}"
            );
        }

        #[tokio::test]
        async fn x_forwarded_headers_take_precedence_over_config() {
            let scheduler = test_scheduler(SchedulerConfig {
                external_host: "internal-host".into(),
                bind_port: 50050,
                ..Default::default()
            });

            let result = get_webtui(
                headers(&[
                    ("x-forwarded-proto", "https"),
                    ("x-forwarded-host", "public-host.example.com"),
                    ("x-forwarded-port", "443"),
                ]),
                Query(HashMap::new()),
                State(scheduler),
            )
            .await;

            let location = location_of(result);
            assert!(
                location.contains(
                    "ballista_scheduler_url=https://public-host.example.com:443"
                ),
                "unexpected location: {location}"
            );
        }

        #[tokio::test]
        async fn partial_forwarded_headers_fall_back_to_config_for_the_rest() {
            let scheduler = test_scheduler(SchedulerConfig {
                external_host: "internal-host".into(),
                bind_port: 50050,
                ..Default::default()
            });

            let result = get_webtui(
                headers(&[("x-forwarded-proto", "https")]),
                Query(HashMap::new()),
                State(scheduler),
            )
            .await;

            let location = location_of(result);
            assert!(
                location.contains("ballista_scheduler_url=https://internal-host:50050"),
                "unexpected location: {location}"
            );
        }

        #[tokio::test]
        async fn malformed_forwarded_port_falls_back_to_config_bind_port() {
            let scheduler = test_scheduler(SchedulerConfig {
                external_host: "internal-host".into(),
                bind_port: 50050,
                ..Default::default()
            });

            let result = get_webtui(
                headers(&[
                    ("x-forwarded-proto", "https"),
                    ("x-forwarded-port", "not-a-port"),
                ]),
                Query(HashMap::new()),
                State(scheduler),
            )
            .await;

            let location = location_of(result);
            assert!(
                location.contains("ballista_scheduler_url=https://internal-host:50050"),
                "unexpected location: {location}"
            );
        }

        #[tokio::test]
        async fn explicit_ballista_scheduler_url_param_takes_precedence() {
            let scheduler = test_scheduler(SchedulerConfig::default());

            let mut params = HashMap::new();
            params.insert(
                "ballista_scheduler_url".to_string(),
                "https://override.example.com:1234".to_string(),
            );

            let result = get_webtui(
                headers(&[("x-forwarded-host", "should-be-ignored.example.com")]),
                Query(params),
                State(scheduler),
            )
            .await;

            let location = location_of(result);
            assert!(
                location
                    .contains("ballista_scheduler_url=https://override.example.com:1234"),
                "unexpected location: {location}"
            );
            assert!(
                !location.contains("should-be-ignored"),
                "explicit ballista_scheduler_url must not be overridden: {location}"
            );
        }

        #[tokio::test]
        async fn additional_query_params_are_forwarded() {
            let scheduler = test_scheduler(SchedulerConfig::default());

            let mut params = HashMap::new();
            params.insert("foo".to_string(), "bar".to_string());

            let result =
                get_webtui(HeaderMap::new(), Query(params), State(scheduler)).await;

            let location = location_of(result);
            assert!(
                location.contains("&foo=bar"),
                "unexpected location: {location}"
            );
        }

        #[tokio::test]
        async fn query_param_values_are_url_escaped() {
            let scheduler = test_scheduler(SchedulerConfig::default());

            let mut params = HashMap::new();
            params.insert("weird".to_string(), "a#b c".to_string());

            let result =
                get_webtui(HeaderMap::new(), Query(params), State(scheduler)).await;

            let location = location_of(result);
            assert!(
                location.contains("&weird=a%23b%20c"),
                "unexpected location: {location}"
            );
        }
    }
}
