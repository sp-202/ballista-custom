//! Custom resources: `BallistaCluster` and `BallistaApplication` (`ballista.apache.org/v1alpha1`).
//!
//! `BallistaApplication` follows the Spark operator's `SparkApplication`
//! (`sparkoperator.k8s.io/v1beta2`) where the concepts match: `type`,
//! `mainApplicationFile`, `arguments`, `sparkConf`, `restartPolicy`,
//! `timeToLiveSeconds`, and `status.applicationState.state` with the same state
//! names, so tools that watch Spark applications need only the group/kind changed.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{
    EnvFromSource, EnvVar, LocalObjectReference, PodSecurityContext, ResourceRequirements,
    SecurityContext, Toleration, Volume, VolumeMount,
};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Fields every component pod accepts.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PodOptions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceRequirements>,
    /// Extra command-line arguments for the component's binary.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env: Vec<EnvVar>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub node_selector: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tolerations: Vec<Toleration>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub annotations: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
}

// ---------------------------------------------------------------------------
// BallistaCluster
// ---------------------------------------------------------------------------

/// A Ballista cluster: scheduler (with web UI), executors, and optionally a Spark
/// Connect server and a history server.
#[derive(CustomResource, Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "ballista.apache.org",
    version = "v1alpha1",
    kind = "BallistaCluster",
    namespaced,
    status = "BallistaClusterStatus",
    shortname = "bc",
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Ready","type":"integer","jsonPath":".status.executors.ready"}"#,
    printcolumn = r#"{"name":"Desired","type":"integer","jsonPath":".status.executors.desired"}"#,
    printcolumn = r#"{"name":"Busy vcores","type":"integer","jsonPath":".status.executors.busyVcores"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct BallistaClusterSpec {
    /// Image with ballista-spark-scheduler, ballista-spark-executor, ballista-connect
    /// and ballista-history-server.
    pub image: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image_pull_policy: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub image_pull_secrets: Vec<LocalObjectReference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_account_name: Option<String>,
    /// Environment for every component (object-store credentials, RUST_LOG, ...).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env: Vec<EnvVar>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env_from: Vec<EnvFromSource>,
    /// Volumes mounted in the scheduler, executors and connect servers (including
    /// drivers of applications on this cluster): data paths are opened by all of them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub volumes: Vec<Volume>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub volume_mounts: Vec<VolumeMount>,
    /// Pod security context of every pod. Default (Pod Security "restricted"):
    /// non-root uid/gid 185 like Spark's images, fsGroup 185, seccomp RuntimeDefault.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pod_security_context: Option<PodSecurityContext>,
    /// Container security context of every container. Default: no privilege
    /// escalation, all capabilities dropped, read-only root filesystem (a
    /// writable emptyDir is mounted at /tmp).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub security_context: Option<SecurityContext>,
    #[serde(default)]
    pub scheduler: SchedulerSpec,
    #[serde(default)]
    pub executor: ExecutorSpec,
    #[serde(default)]
    pub connect: ConnectSpec,
    #[serde(default)]
    pub history: HistorySpec,
    #[serde(default)]
    pub security: SecuritySpec,
}

/// Authentication and encryption, on by default: Spark's `spark.authenticate`
/// with `spark.network.crypto.enabled` (mutual TLS between all components),
/// `spark.ssl.ui` with a UI filter (HTTPS and a token on the web UI and REST
/// API), and `spark.connect.authenticate.token` (a token for Spark Connect).
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct SecuritySpec {
    pub enabled: bool,
    /// Secret to use instead of the generated `<cluster>-security`, with keys
    /// ca.crt, tls.crt, tls.key (a certificate for both server and client auth
    /// whose names include "ballista-internal"), ui-token and connect-token.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret_name: Option<String>,
    /// Serve Spark Connect over TLS with the cluster certificate. PySpark sends
    /// a token only to localhost or over TLS, so clients in other pods need
    /// this (`sc://<cluster>-connect.<ns>.svc:15002/;use_ssl=true;token=...`,
    /// with GRPC_DEFAULT_SSL_ROOTS_FILE_PATH pointing at ca.crt).
    pub connect_tls: bool,
}

impl Default for SecuritySpec {
    fn default() -> Self {
        Self {
            enabled: true,
            secret_name: None,
            connect_tls: false,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SchedulerSpec {
    #[serde(flatten)]
    pub pod: PodOptions,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct ExecutorSpec {
    #[serde(flatten)]
    pub pod: PodOptions,
    /// Task slots per executor (`spark.executor.cores`).
    pub vcores: u32,
    /// Executors to run when dynamic allocation is off (`spark.executor.instances`).
    pub instances: u32,
    /// Fixed memory pool, e.g. "8GB". When unset the executor uses
    /// `memoryPoolFraction` of its container memory limit.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_pool_size: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_pool_fraction: Option<f64>,
    /// Seconds running tasks get to finish when an executor is removed.
    pub graceful_shutdown_seconds: u32,
    /// Volume for shuffle files and spills (its name is ignored); emptyDir by default.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub work_dir: Option<Volume>,
    pub dynamic_allocation: DynamicAllocation,
}

impl Default for ExecutorSpec {
    fn default() -> Self {
        Self {
            pod: PodOptions::default(),
            vcores: 4,
            instances: 2,
            memory_pool_size: None,
            memory_pool_fraction: None,
            graceful_shutdown_seconds: 60,
            work_dir: None,
            dynamic_allocation: DynamicAllocation::default(),
        }
    }
}

/// Spark's `spark.dynamicAllocation.*`, driven by the scheduler's task backlog.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct DynamicAllocation {
    pub enabled: bool,
    pub min_executors: u32,
    pub max_executors: u32,
    /// Executors started with the cluster (default: minExecutors).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initial_executors: Option<u32>,
    /// Add executors once tasks have waited this long (`schedulerBacklogTimeout`).
    pub scheduler_backlog_timeout_seconds: u64,
    /// Remove an executor idle this long (`executorIdleTimeout`).
    pub executor_idle_timeout_seconds: u64,
    /// An idle executor holding output of a running job is kept until this has
    /// passed (`shuffleTracking.timeout`); unset = kept while the job runs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shuffle_tracking_timeout_seconds: Option<u64>,
}

impl Default for DynamicAllocation {
    fn default() -> Self {
        Self {
            enabled: false,
            min_executors: 1,
            max_executors: 10,
            initial_executors: None,
            scheduler_backlog_timeout_seconds: 1,
            executor_idle_timeout_seconds: 60,
            shuffle_tracking_timeout_seconds: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct ConnectSpec {
    /// Run a shared Spark Connect server (sc://<cluster>-connect:15002).
    /// Applications run their own connect server in the driver pod either way.
    pub enabled: bool,
    /// Spark settings for every session (`start-connect-server.sh --conf`).
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub spark_conf: BTreeMap<String, String>,
    #[serde(flatten)]
    pub pod: PodOptions,
}

impl Default for ConnectSpec {
    fn default() -> Self {
        Self {
            enabled: true,
            spark_conf: BTreeMap::new(),
            pod: PodOptions::default(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HistorySpec {
    /// Write event logs and run a history server (port 18090).
    #[serde(default)]
    pub enabled: bool,
    /// PersistentVolumeClaim for event logs, shared by the scheduler (writes) and
    /// the history server (reads). Required when enabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_log_claim: Option<String>,
    #[serde(flatten)]
    pub pod: PodOptions,
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BallistaClusterStatus {
    /// Pending | Running | Failed
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// gRPC address of the scheduler.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scheduler_url: Option<String>,
    /// Spark Connect address of the shared connect server.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connect_url: Option<String>,
    /// Secret with the cluster CA, certificate, UI token and Spark Connect token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub security_secret: Option<String>,
    #[serde(default)]
    pub executors: ExecutorCounts,
    /// When tasks started waiting with no free slot (autoscaler state).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backlog_since: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_scale_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorCounts {
    pub desired: u32,
    /// Executor pods that exist and are not being deleted.
    pub current: u32,
    pub ready: u32,
    /// Executors registered with the scheduler.
    pub registered: u32,
    pub pending_vcores: u64,
    pub busy_vcores: u64,
    pub total_vcores: u64,
}

// ---------------------------------------------------------------------------
// BallistaApplication
// ---------------------------------------------------------------------------

/// A batch job, like a `SparkApplication`: a driver pod runs the PySpark script (or
/// SQL) against a Spark Connect server in the same pod, on a shared cluster
/// (`clusterRef`) or on a cluster created for this application only (`cluster`).
#[derive(CustomResource, Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "ballista.apache.org",
    version = "v1alpha1",
    kind = "BallistaApplication",
    namespaced,
    status = "BallistaApplicationStatus",
    shortname = "bapp",
    printcolumn = r#"{"name":"State","type":"string","jsonPath":".status.applicationState.state"}"#,
    printcolumn = r#"{"name":"Cluster","type":"string","jsonPath":".status.clusterName"}"#,
    printcolumn = r#"{"name":"Attempts","type":"integer","jsonPath":".status.submissionAttempts"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct BallistaApplicationSpec {
    /// Python (a PySpark script in `mainApplicationFile`) or Sql (`sql`).
    #[serde(default, rename = "type")]
    pub app_type: AppType,
    /// `local:///path/in/driver/image.py`, `https://...`, or a path in a mounted volume.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub main_application_file: Option<String>,
    /// SQL statements separated by `;` (type Sql).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub arguments: Vec<String>,
    /// Spark settings for the job's sessions (`spark-submit --conf`).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub spark_conf: BTreeMap<String, String>,
    /// Run on this existing BallistaCluster.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_ref: Option<ClusterRef>,
    /// Or create a cluster for this application, deleted when it finishes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster: Option<BallistaClusterSpec>,
    #[serde(default)]
    pub driver: DriverSpec,
    #[serde(default)]
    pub restart_policy: RestartPolicy,
    /// Delete the application this long after it finishes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_to_live_seconds: Option<i64>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub enum AppType {
    #[default]
    Python,
    Sql,
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ClusterRef {
    pub name: String,
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DriverSpec {
    /// Python image with pyspark-client (default: the operator's --driver-image).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image_pull_policy: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_account_name: Option<String>,
    /// pip packages installed before the script runs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub python_packages: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env_from: Vec<EnvFromSource>,
    /// Extra volumes for the driver container only.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub volumes: Vec<Volume>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub volume_mounts: Vec<VolumeMount>,
    /// Resources of the Spark Connect sidecar (it plans the queries).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connect_resources: Option<ResourceRequirements>,
    #[serde(flatten)]
    pub pod: PodOptions,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
pub struct RestartPolicy {
    /// Never | OnFailure
    #[serde(rename = "type")]
    pub policy: String,
    pub on_failure_retries: u32,
    pub on_failure_retry_interval_seconds: u64,
}

impl Default for RestartPolicy {
    fn default() -> Self {
        Self {
            policy: "Never".to_string(),
            on_failure_retries: 0,
            on_failure_retry_interval_seconds: 10,
        }
    }
}

/// `SparkApplication` state names.
pub mod state {
    pub const SUBMITTED: &str = "SUBMITTED";
    pub const RUNNING: &str = "RUNNING";
    pub const COMPLETED: &str = "COMPLETED";
    pub const FAILED: &str = "FAILED";
    pub const SUBMISSION_FAILED: &str = "SUBMISSION_FAILED";
    pub const PENDING_RERUN: &str = "PENDING_RERUN";

    pub fn is_terminal(state: &str) -> bool {
        matches!(state, COMPLETED | FAILED | SUBMISSION_FAILED)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BallistaApplicationStatus {
    #[serde(default)]
    pub application_state: ApplicationState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_name: Option<String>,
    #[serde(default)]
    pub driver_info: DriverInfo,
    #[serde(default)]
    pub submission_attempts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_submission_attempt_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub termination_time: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ApplicationState {
    #[serde(default)]
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DriverInfo {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pod_name: Option<String>,
}
