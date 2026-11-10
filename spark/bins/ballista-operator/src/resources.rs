//! Pure builders for the Kubernetes objects of a `BallistaCluster` and the driver
//! pod of a `BallistaApplication`. No API calls here.

use std::collections::BTreeMap;

use k8s_openapi::api::apps::v1::{Deployment, DeploymentSpec, DeploymentStrategy};
use k8s_openapi::api::core::v1::{
    Affinity, Capabilities, Container, ContainerPort, EmptyDirVolumeSource, EnvVar, EnvVarSource,
    HTTPGetAction, KeyToPath, ObjectFieldSelector, PersistentVolumeClaimVolumeSource, Pod,
    PodAffinity, PodAffinityTerm, PodSecurityContext, PodSpec, PodTemplateSpec, Probe,
    SeccompProfile, SecretKeySelector, SecretVolumeSource, SecurityContext, Service, ServicePort,
    ServiceSpec, TCPSocketAction, Volume, VolumeMount,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta, OwnerReference};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;

use crate::crd::{AppType, BallistaApplicationSpec, BallistaClusterSpec, PodOptions};
use crate::security;

pub const MANAGER: &str = "ballista-operator";
pub const CLUSTER_LABEL: &str = "ballista.apache.org/cluster";
pub const APP_LABEL: &str = "ballista.apache.org/application";
pub const COMPONENT_LABEL: &str = "app.kubernetes.io/component";
pub const IDLE_SINCE_ANNOTATION: &str = "ballista.apache.org/idle-since";

pub const SCHEDULER_PORT: i32 = 50050;
pub const UI_PORT: i32 = 4040;
pub const CONNECT_PORT: i32 = 15002;
pub const HISTORY_PORT: i32 = 18090;

pub fn scheduler_name(cluster: &str) -> String {
    format!("{cluster}-scheduler")
}
pub fn connect_name(cluster: &str) -> String {
    format!("{cluster}-connect")
}
pub fn history_name(cluster: &str) -> String {
    format!("{cluster}-history")
}
pub fn executor_prefix(cluster: &str) -> String {
    format!("{cluster}-exec-")
}
pub fn scheduler_url(cluster: &str, ns: &str) -> String {
    format!(
        "http://{}.{ns}.svc:{SCHEDULER_PORT}",
        scheduler_name(cluster)
    )
}

pub fn labels(cluster: &str, component: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("app.kubernetes.io/name".to_string(), "ballista".to_string()),
        (
            "app.kubernetes.io/instance".to_string(),
            cluster.to_string(),
        ),
        (COMPONENT_LABEL.to_string(), component.to_string()),
        (
            "app.kubernetes.io/managed-by".to_string(),
            MANAGER.to_string(),
        ),
        (CLUSTER_LABEL.to_string(), cluster.to_string()),
    ])
}

/// Label selector string for one component's pods.
pub fn selector(cluster: &str, component: &str) -> String {
    format!("{CLUSTER_LABEL}={cluster},{COMPONENT_LABEL}={component}")
}

fn selector_labels(cluster: &str, component: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        (CLUSTER_LABEL.to_string(), cluster.to_string()),
        (COMPONENT_LABEL.to_string(), component.to_string()),
    ])
}

fn meta(
    name: &str,
    ns: &str,
    labels: BTreeMap<String, String>,
    owner: &OwnerReference,
) -> ObjectMeta {
    ObjectMeta {
        name: Some(name.to_string()),
        namespace: Some(ns.to_string()),
        labels: Some(labels),
        owner_references: Some(vec![owner.clone()]),
        ..Default::default()
    }
}

fn opt_vec<T>(v: Vec<T>) -> Option<Vec<T>> {
    if v.is_empty() { None } else { Some(v) }
}

fn opt_map(m: BTreeMap<String, String>) -> Option<BTreeMap<String, String>> {
    if m.is_empty() { None } else { Some(m) }
}

fn env_field(name: &str, path: &str) -> EnvVar {
    EnvVar {
        name: name.to_string(),
        value_from: Some(EnvVarSource {
            field_ref: Some(ObjectFieldSelector {
                field_path: path.to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn port(name: &str, p: i32) -> ContainerPort {
    ContainerPort {
        name: Some(name.to_string()),
        container_port: p,
        ..Default::default()
    }
}

fn http_probe(path: &str, p: i32, initial: i32, period: i32, failures: i32) -> Probe {
    Probe {
        http_get: Some(HTTPGetAction {
            path: Some(path.to_string()),
            port: IntOrString::Int(p),
            ..Default::default()
        }),
        initial_delay_seconds: Some(initial),
        period_seconds: Some(period),
        failure_threshold: Some(failures),
        ..Default::default()
    }
}

fn https_probe(path: &str, p: i32, initial: i32, period: i32, failures: i32) -> Probe {
    let mut probe = http_probe(path, p, initial, period, failures);
    if let Some(get) = probe.http_get.as_mut() {
        // The kubelet does not verify the certificate.
        get.scheme = Some("HTTPS".to_string());
    }
    probe
}

fn tcp_probe(p: i32, period: i32, failures: i32) -> Probe {
    Probe {
        tcp_socket: Some(TCPSocketAction {
            port: IntOrString::Int(p),
            ..Default::default()
        }),
        period_seconds: Some(period),
        failure_threshold: Some(failures),
        ..Default::default()
    }
}

// --- Security defaults (Pod Security Standard "restricted", Spark's uid 185) ---

/// Non-root uid/gid of Spark's official images.
pub const RUN_AS: i64 = 185;

pub fn default_pod_security() -> PodSecurityContext {
    PodSecurityContext {
        run_as_non_root: Some(true),
        run_as_user: Some(RUN_AS),
        run_as_group: Some(RUN_AS),
        fs_group: Some(RUN_AS),
        seccomp_profile: Some(SeccompProfile {
            type_: "RuntimeDefault".to_string(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub fn default_container_security() -> SecurityContext {
    SecurityContext {
        allow_privilege_escalation: Some(false),
        read_only_root_filesystem: Some(true),
        run_as_non_root: Some(true),
        capabilities: Some(Capabilities {
            drop: Some(vec!["ALL".to_string()]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Writable scratch space: the root filesystem is read-only.
fn tmp_volume() -> Volume {
    Volume {
        name: "tmp".to_string(),
        empty_dir: Some(EmptyDirVolumeSource::default()),
        ..Default::default()
    }
}

fn tmp_mount() -> VolumeMount {
    VolumeMount {
        name: "tmp".to_string(),
        mount_path: "/tmp".to_string(),
        ..Default::default()
    }
}

fn plain_env(name: &str, value: &str) -> EnvVar {
    EnvVar {
        name: name.to_string(),
        value: Some(value.to_string()),
        ..Default::default()
    }
}

// --- Authentication and encryption (see `security`) ---

/// Where components find the cluster certificate, key, CA and UI token.
pub const TLS_DIR: &str = "/etc/ballista/tls";
const TLS_VOLUME: &str = "ballista-tls";

/// What a component needs from the cluster's security Secret.
#[derive(Clone, Copy, PartialEq)]
enum Role {
    /// Serves the web UI and REST API: certificate and UI token.
    Ui,
    /// Cluster member only: certificate.
    Member,
    /// Spark Connect server: certificate and Connect token; `tls` serves
    /// Spark Connect over TLS.
    Connect { tls: bool },
}

/// Env, mounts and volumes for `role`; all empty with security off.
#[derive(Default)]
struct SecurityParts {
    env: Vec<EnvVar>,
    mounts: Vec<VolumeMount>,
    volumes: Vec<Volume>,
}

fn secret_env(name: &str, secret: &str, key: &str) -> EnvVar {
    EnvVar {
        name: name.to_string(),
        value_from: Some(EnvVarSource {
            secret_key_ref: Some(SecretKeySelector {
                name: secret.to_string(),
                key: key.to_string(),
                optional: Some(false),
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn security_parts(cluster: &str, spec: &BallistaClusterSpec, role: Role) -> SecurityParts {
    let Some(secret) = security::secret_name(cluster, spec) else {
        return SecurityParts::default();
    };
    let file = |key: &str| format!("{TLS_DIR}/{key}");
    let mut env = vec![
        plain_env("BALLISTA_TLS_CERT", &file(security::CERT_KEY)),
        plain_env("BALLISTA_TLS_KEY", &file(security::KEY_KEY)),
        plain_env("BALLISTA_TLS_CA", &file(security::CA_KEY)),
    ];
    let mut items = vec![security::CA_KEY, security::CERT_KEY, security::KEY_KEY];
    match role {
        Role::Ui => {
            env.push(plain_env("BALLISTA_UI_TOKEN_FILE", &file(security::UI_TOKEN_KEY)));
            items.push(security::UI_TOKEN_KEY);
        }
        Role::Member => {}
        Role::Connect { tls } => {
            env.push(secret_env(
                "SPARK_CONNECT_AUTHENTICATE_TOKEN",
                &secret,
                security::CONNECT_TOKEN_KEY,
            ));
            if tls {
                env.push(plain_env("BALLISTA_CONNECT_TLS", "true"));
            }
        }
    }
    SecurityParts {
        env,
        mounts: vec![VolumeMount {
            name: TLS_VOLUME.to_string(),
            mount_path: TLS_DIR.to_string(),
            read_only: Some(true),
            ..Default::default()
        }],
        volumes: vec![Volume {
            name: TLS_VOLUME.to_string(),
            secret: Some(SecretVolumeSource {
                secret_name: Some(secret),
                // Only the files this component needs.
                items: Some(
                    items
                        .into_iter()
                        .map(|k| KeyToPath {
                            key: k.to_string(),
                            path: k.to_string(),
                            ..Default::default()
                        })
                        .collect(),
                ),
                ..Default::default()
            }),
            ..Default::default()
        }],
    }
}

/// A container of the cluster image with the cluster-wide env and volume mounts.
fn cluster_container(
    spec: &BallistaClusterSpec,
    name: &str,
    command: &str,
    mut args: Vec<String>,
    opts: &PodOptions,
    mut env: Vec<EnvVar>,
    mut mounts: Vec<VolumeMount>,
) -> Container {
    args.extend(opts.args.iter().cloned());
    env.push(plain_env("HOME", "/tmp"));
    env.extend(spec.env.iter().cloned());
    env.extend(opts.env.iter().cloned());
    mounts.push(tmp_mount());
    mounts.extend(spec.volume_mounts.iter().cloned());
    Container {
        name: name.to_string(),
        image: Some(spec.image.clone()),
        image_pull_policy: spec.image_pull_policy.clone(),
        command: Some(vec![command.to_string()]),
        args: Some(args),
        env: opt_vec(env),
        env_from: opt_vec(spec.env_from.clone()),
        volume_mounts: opt_vec(mounts),
        resources: opts.resources.clone(),
        security_context: Some(
            spec.security_context
                .clone()
                .unwrap_or_else(default_container_security),
        ),
        ..Default::default()
    }
}

fn pod_spec(
    spec: &BallistaClusterSpec,
    opts: &PodOptions,
    containers: Vec<Container>,
    mut volumes: Vec<Volume>,
) -> PodSpec {
    volumes.push(tmp_volume());
    volumes.extend(spec.volumes.iter().cloned());
    PodSpec {
        containers,
        volumes: opt_vec(volumes),
        security_context: Some(
            spec.pod_security_context
                .clone()
                .unwrap_or_else(default_pod_security),
        ),
        // Ballista components never call the Kubernetes API.
        automount_service_account_token: Some(false),
        service_account_name: spec.service_account_name.clone(),
        image_pull_secrets: opt_vec(spec.image_pull_secrets.clone()),
        node_selector: opt_map(opts.node_selector.clone()),
        tolerations: opt_vec(opts.tolerations.clone()),
        ..Default::default()
    }
}

fn template_meta(cluster: &str, component: &str, opts: &PodOptions) -> ObjectMeta {
    let mut l = labels(cluster, component);
    l.extend(opts.labels.clone());
    ObjectMeta {
        labels: Some(l),
        annotations: opt_map(opts.annotations.clone()),
        ..Default::default()
    }
}

/// One replica, Recreate: these components keep state in memory.
fn singleton(
    name: &str,
    ns: &str,
    cluster: &str,
    component: &str,
    owner: &OwnerReference,
    template: PodTemplateSpec,
) -> Deployment {
    Deployment {
        metadata: meta(name, ns, labels(cluster, component), owner),
        spec: Some(DeploymentSpec {
            replicas: Some(1),
            strategy: Some(DeploymentStrategy {
                type_: Some("Recreate".to_string()),
                ..Default::default()
            }),
            selector: LabelSelector {
                match_labels: Some(selector_labels(cluster, component)),
                ..Default::default()
            },
            template,
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn service(
    name: &str,
    ns: &str,
    cluster: &str,
    component: &str,
    owner: &OwnerReference,
    ports: &[(&str, i32)],
) -> Service {
    Service {
        metadata: meta(name, ns, labels(cluster, component), owner),
        spec: Some(ServiceSpec {
            selector: Some(selector_labels(cluster, component)),
            ports: Some(
                ports
                    .iter()
                    .map(|(n, p)| ServicePort {
                        name: Some(n.to_string()),
                        port: *p,
                        target_port: Some(IntOrString::Int(*p)),
                        ..Default::default()
                    })
                    .collect(),
            ),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn event_log(spec: &BallistaClusterSpec) -> Option<(Volume, VolumeMount)> {
    let claim = spec.history.event_log_claim.as_ref()?;
    if !spec.history.enabled {
        return None;
    }
    Some((
        Volume {
            name: "event-log".to_string(),
            persistent_volume_claim: Some(PersistentVolumeClaimVolumeSource {
                claim_name: claim.clone(),
                ..Default::default()
            }),
            ..Default::default()
        },
        VolumeMount {
            name: "event-log".to_string(),
            mount_path: "/history".to_string(),
            ..Default::default()
        },
    ))
}

pub fn scheduler(
    cluster: &str,
    ns: &str,
    spec: &BallistaClusterSpec,
    owner: &OwnerReference,
) -> (Deployment, Service) {
    let name = scheduler_name(cluster);
    let opts = &spec.scheduler.pod;
    let mut args = vec![
        "--bind-host=0.0.0.0".to_string(),
        format!("--bind-port={SCHEDULER_PORT}"),
        format!("--external-host={name}"),
        format!("--web-ui-port={UI_PORT}"),
    ];
    let sec = security_parts(cluster, spec, Role::Ui);
    let (mut volumes, mut mounts) = (sec.volumes, sec.mounts);
    if let Some((v, m)) = event_log(spec) {
        args.push("--event-log-dir=/history".to_string());
        volumes.push(v);
        mounts.push(m);
    }
    let mut c = cluster_container(
        spec,
        "scheduler",
        "ballista-spark-scheduler",
        args,
        opts,
        sec.env,
        mounts,
    );
    c.ports = Some(vec![port("grpc", SCHEDULER_PORT), port("ui", UI_PORT)]);
    if spec.security.enabled {
        // The gRPC port takes cluster members only; probe the HTTPS UI port,
        // whose health routes need no token.
        c.readiness_probe = Some(https_probe("/readyz", UI_PORT, 0, 5, 3));
        c.liveness_probe = Some(https_probe("/healthz", UI_PORT, 10, 20, 6));
    } else {
        c.readiness_probe = Some(http_probe("/api/version", SCHEDULER_PORT, 0, 5, 3));
        c.liveness_probe = Some(http_probe("/api/version", SCHEDULER_PORT, 10, 20, 6));
    }
    let template = PodTemplateSpec {
        metadata: Some(template_meta(cluster, "scheduler", opts)),
        spec: Some(pod_spec(spec, opts, vec![c], volumes)),
    };
    (
        singleton(&name, ns, cluster, "scheduler", owner, template),
        service(
            &name,
            ns,
            cluster,
            "scheduler",
            owner,
            &[("grpc", SCHEDULER_PORT), ("ui", UI_PORT)],
        ),
    )
}

pub fn connect(
    cluster: &str,
    ns: &str,
    spec: &BallistaClusterSpec,
    owner: &OwnerReference,
) -> (Deployment, Service) {
    let name = connect_name(cluster);
    let opts = &spec.connect.pod;
    let role = Role::Connect {
        tls: spec.security.connect_tls,
    };
    let c = connect_container(
        cluster,
        ns,
        spec,
        &spec.connect.spark_conf,
        opts.resources.clone(),
        opts,
        role,
    );
    let volumes = security_parts(cluster, spec, role).volumes;
    let template = PodTemplateSpec {
        metadata: Some(template_meta(cluster, "connect", opts)),
        spec: Some(pod_spec(spec, opts, vec![c], volumes)),
    };
    (
        singleton(&name, ns, cluster, "connect", owner, template),
        service(
            &name,
            ns,
            cluster,
            "connect",
            owner,
            &[("spark-connect", CONNECT_PORT)],
        ),
    )
}

fn connect_container(
    cluster: &str,
    ns: &str,
    spec: &BallistaClusterSpec,
    spark_conf: &BTreeMap<String, String>,
    resources: Option<k8s_openapi::api::core::v1::ResourceRequirements>,
    opts: &PodOptions,
    role: Role,
) -> Container {
    let sec = security_parts(cluster, spec, role);
    let mut args = vec![
        "--ip=0.0.0.0".to_string(),
        format!("--port={CONNECT_PORT}"),
        format!("--scheduler-url={}", scheduler_url(cluster, ns)),
    ];
    args.extend(spark_conf.iter().map(|(k, v)| format!("--conf={k}={v}")));
    let mut c = cluster_container(
        spec,
        "connect",
        "ballista-connect",
        args,
        opts,
        sec.env,
        sec.mounts,
    );
    c.resources = resources;
    c.ports = Some(vec![port("spark-connect", CONNECT_PORT)]);
    c.readiness_probe = Some(tcp_probe(CONNECT_PORT, 2, 3));
    c
}

pub fn history(
    cluster: &str,
    ns: &str,
    spec: &BallistaClusterSpec,
    owner: &OwnerReference,
) -> Option<(Deployment, Service)> {
    let (volume, mut mount) = event_log(spec)?;
    mount.read_only = Some(true);
    let name = history_name(cluster);
    let opts = &spec.history.pod;
    let args = vec![
        "--event-log-dir=/history".to_string(),
        "--bind-host=0.0.0.0".to_string(),
        format!("--bind-port={HISTORY_PORT}"),
    ];
    let sec = security_parts(cluster, spec, Role::Ui);
    let mut c = cluster_container(
        spec,
        "history",
        "ballista-history-server",
        args,
        opts,
        sec.env,
        vec![],
    );
    // Only the event log (and certificates), not the data volumes.
    c.volume_mounts = Some([vec![mount, tmp_mount()], sec.mounts].concat());
    c.ports = Some(vec![port("http", HISTORY_PORT)]);
    c.readiness_probe = Some(tcp_probe(HISTORY_PORT, 10, 3));
    let mut pod = pod_spec(spec, opts, vec![c], vec![]);
    pod.volumes = Some([vec![volume, tmp_volume()], sec.volumes].concat());
    // A ReadWriteOnce claim is attached to the scheduler's node.
    pod.affinity = Some(Affinity {
        pod_affinity: Some(PodAffinity {
            preferred_during_scheduling_ignored_during_execution: None,
            required_during_scheduling_ignored_during_execution: Some(vec![PodAffinityTerm {
                topology_key: "kubernetes.io/hostname".to_string(),
                label_selector: Some(LabelSelector {
                    match_labels: Some(selector_labels(cluster, "scheduler")),
                    ..Default::default()
                }),
                ..Default::default()
            }]),
        }),
        ..Default::default()
    });
    let template = PodTemplateSpec {
        metadata: Some(template_meta(cluster, "history", opts)),
        spec: Some(pod),
    };
    Some((
        singleton(&name, ns, cluster, "history", owner, template),
        service(
            &name,
            ns,
            cluster,
            "history",
            owner,
            &[("http", HISTORY_PORT)],
        ),
    ))
}

/// One executor pod. Executors are pods (not a Deployment) so the autoscaler
/// chooses which one to remove, like Spark's executor pods.
pub fn executor_pod(
    cluster: &str,
    ns: &str,
    spec: &BallistaClusterSpec,
    owner: &OwnerReference,
) -> Pod {
    let e = &spec.executor;
    let mut args = vec![
        "--bind-host=0.0.0.0".to_string(),
        // Other executors fetch shuffle files from this address.
        "--external-host=$(POD_IP)".to_string(),
        "--id=$(POD_NAME)".to_string(),
        format!("--scheduler-host={}", scheduler_name(cluster)),
        format!("--scheduler-port={SCHEDULER_PORT}"),
        format!("--vcores={}", e.vcores),
        "--work-dir=/work".to_string(),
        format!(
            "--graceful-shutdown-timeout-seconds={}",
            e.graceful_shutdown_seconds
        ),
        "--scheduler-connect-timeout-seconds=120".to_string(),
    ];
    match (&e.memory_pool_size, e.memory_pool_fraction) {
        (Some(size), _) => args.push(format!("--memory-pool-size={size}")),
        (None, Some(f)) => args.push(format!("--memory-pool-fraction={f}")),
        (None, None) => {}
    }
    let sec = security_parts(cluster, spec, Role::Member);
    let mut env = vec![
        env_field("POD_IP", "status.podIP"),
        env_field("POD_NAME", "metadata.name"),
    ];
    env.extend(sec.env);
    let work = VolumeMount {
        name: "work".to_string(),
        mount_path: "/work".to_string(),
        ..Default::default()
    };
    let mut c = cluster_container(
        spec,
        "executor",
        "ballista-spark-executor",
        args,
        &e.pod,
        env,
        [vec![work], sec.mounts].concat(),
    );
    c.ports = Some(vec![
        port("flight", 50051),
        port("grpc", 50052),
        port("health", 50053),
    ]);
    c.readiness_probe = Some(http_probe("/readyz", 50053, 0, 5, 3));
    c.liveness_probe = Some(http_probe("/healthz", 50053, 10, 20, 6));
    let work_volume = match &e.work_dir {
        Some(v) => Volume {
            name: "work".to_string(),
            ..v.clone()
        },
        None => Volume {
            name: "work".to_string(),
            empty_dir: Some(EmptyDirVolumeSource::default()),
            ..Default::default()
        },
    };
    let mut pod = pod_spec(spec, &e.pod, vec![c], [vec![work_volume], sec.volumes].concat());
    // A failed executor is replaced by the operator, under a new id.
    pod.restart_policy = Some("Never".to_string());
    pod.termination_grace_period_seconds = Some(i64::from(e.graceful_shutdown_seconds) + 15);
    let mut m = template_meta(cluster, "executor", &e.pod);
    m.generate_name = Some(executor_prefix(cluster));
    m.namespace = Some(ns.to_string());
    m.owner_references = Some(vec![owner.clone()]);
    Pod {
        metadata: m,
        spec: Some(pod),
        ..Default::default()
    }
}

/// The driver pod of an application: the user's script in a Python container and
/// a Spark Connect server as a native sidecar (init container with
/// `restartPolicy: Always`), which ends with the script. Like Spark's cluster
/// deploy mode, where the driver runs in the cluster.
pub fn driver_pod(
    app: &str,
    pod_name: &str,
    ns: &str,
    app_spec: &BallistaApplicationSpec,
    cluster: &str,
    cluster_spec: &BallistaClusterSpec,
    default_driver_image: &str,
    owner: &OwnerReference,
) -> Pod {
    let d = &app_spec.driver;
    let mut conf = cluster_spec.connect.spark_conf.clone();
    conf.extend(app_spec.spark_conf.clone());
    conf.entry("spark.app.name".to_string())
        .or_insert_with(|| app.to_string());
    // The script reaches the sidecar on localhost, so no TLS there; the token
    // keeps other pods off the sidecar's port.
    let role = Role::Connect { tls: false };
    let mut sidecar = connect_container(
        cluster,
        ns,
        cluster_spec,
        &conf,
        d.connect_resources.clone(),
        &PodOptions::default(),
        role,
    );
    sidecar.restart_policy = Some("Always".to_string());
    sidecar.readiness_probe = None;
    // The script starts only once the connect server accepts connections.
    sidecar.startup_probe = Some(tcp_probe(CONNECT_PORT, 1, 300));

    let mut env = vec![
        plain_env("SPARK_REMOTE", &format!("sc://localhost:{CONNECT_PORT}")),
        // Read-only root filesystem: everything writable lives under /tmp.
        plain_env("HOME", "/tmp"),
        plain_env("PIP_CACHE_DIR", "/tmp/pip-cache"),
        plain_env("PYTHONDONTWRITEBYTECODE", "1"),
    ];
    // PySpark sends it to the sidecar.
    env.extend(
        security_parts(cluster, cluster_spec, role)
            .env
            .into_iter()
            .filter(|v| v.name == "SPARK_CONNECT_AUTHENTICATE_TOKEN"),
    );
    let mut set = |k: &str, v: String| {
        env.push(EnvVar {
            name: k.to_string(),
            value: Some(v),
            ..Default::default()
        })
    };
    match app_spec.app_type {
        AppType::Python => {
            set(
                "BALLISTA_APP_FILE",
                app_spec.main_application_file.clone().unwrap_or_default(),
            );
        }
        AppType::Sql => set("BALLISTA_APP_SQL", app_spec.sql.clone().unwrap_or_default()),
    }
    if !d.python_packages.is_empty() {
        set("BALLISTA_PYTHON_PACKAGES", d.python_packages.join(" "));
    }
    env.extend(d.pod.env.iter().cloned());
    let mut args = app_spec.arguments.clone();
    args.extend(d.pod.args.iter().cloned());
    let driver = Container {
        name: "driver".to_string(),
        image: Some(
            d.image
                .clone()
                .unwrap_or_else(|| default_driver_image.to_string()),
        ),
        image_pull_policy: d.image_pull_policy.clone(),
        command: Some(vec![
            "python".to_string(),
            "/opt/ballista/run.py".to_string(),
        ]),
        args: opt_vec(args),
        env: Some(env),
        env_from: opt_vec(d.env_from.clone()),
        volume_mounts: opt_vec([vec![tmp_mount()], d.volume_mounts.clone()].concat()),
        resources: d.pod.resources.clone(),
        termination_message_policy: Some("FallbackToLogsOnError".to_string()),
        security_context: Some(
            cluster_spec
                .security_context
                .clone()
                .unwrap_or_else(default_container_security),
        ),
        ..Default::default()
    };
    let mut volumes = vec![tmp_volume()];
    volumes.extend(security_parts(cluster, cluster_spec, role).volumes);
    volumes.extend(cluster_spec.volumes.iter().cloned());
    volumes.extend(d.volumes.iter().cloned());
    let mut l = labels(cluster, "driver");
    l.insert(APP_LABEL.to_string(), app.to_string());
    l.extend(d.pod.labels.clone());
    Pod {
        metadata: ObjectMeta {
            name: Some(pod_name.to_string()),
            namespace: Some(ns.to_string()),
            labels: Some(l),
            annotations: opt_map(d.pod.annotations.clone()),
            owner_references: Some(vec![owner.clone()]),
            ..Default::default()
        },
        spec: Some(PodSpec {
            init_containers: Some(vec![sidecar]),
            containers: vec![driver],
            restart_policy: Some("Never".to_string()),
            volumes: opt_vec(volumes),
            security_context: Some(
                cluster_spec
                    .pod_security_context
                    .clone()
                    .unwrap_or_else(default_pod_security),
            ),
            automount_service_account_token: Some(false),
            service_account_name: d
                .service_account_name
                .clone()
                .or_else(|| cluster_spec.service_account_name.clone()),
            image_pull_secrets: opt_vec(cluster_spec.image_pull_secrets.clone()),
            node_selector: opt_map(d.pod.node_selector.clone()),
            tolerations: opt_vec(d.pod.tolerations.clone()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::ExecutorSpec;

    fn owner() -> OwnerReference {
        OwnerReference {
            api_version: "ballista.apache.org/v1alpha1".to_string(),
            kind: "BallistaCluster".to_string(),
            name: "c1".to_string(),
            uid: "u".to_string(),
            controller: Some(true),
            ..Default::default()
        }
    }

    fn spec() -> BallistaClusterSpec {
        BallistaClusterSpec {
            image: "img:1".to_string(),
            image_pull_policy: None,
            image_pull_secrets: vec![],
            service_account_name: None,
            env: vec![],
            env_from: vec![],
            volumes: vec![],
            volume_mounts: vec![],
            pod_security_context: None,
            security_context: None,
            scheduler: Default::default(),
            executor: ExecutorSpec {
                memory_pool_size: Some("2GB".to_string()),
                ..Default::default()
            },
            connect: Default::default(),
            history: Default::default(),
            security: Default::default(),
        }
    }

    #[test]
    fn executor_pod_registers_under_its_pod_name_and_ip() {
        let pod = executor_pod("c1", "ns", &spec(), &owner());
        assert_eq!(pod.metadata.generate_name.as_deref(), Some("c1-exec-"));
        let c = pod.spec.as_ref().map(|s| &s.containers[0]);
        let args = c.and_then(|c| c.args.clone()).unwrap_or_default();
        for a in [
            "--external-host=$(POD_IP)",
            "--id=$(POD_NAME)",
            "--scheduler-host=c1-scheduler",
            "--vcores=4",
            "--memory-pool-size=2GB",
        ] {
            assert!(args.iter().any(|x| x == a), "missing {a} in {args:?}");
        }
        assert_eq!(
            pod.spec.and_then(|s| s.restart_policy).as_deref(),
            Some("Never")
        );
    }

    #[test]
    fn driver_pod_runs_connect_as_native_sidecar_with_merged_conf() {
        let mut cs = spec();
        cs.connect
            .spark_conf
            .insert("spark.sql.shuffle.partitions".into(), "100".into());
        let app = BallistaApplicationSpec {
            app_type: AppType::Python,
            main_application_file: Some("local:///opt/app/job.py".into()),
            sql: None,
            arguments: vec!["--date".into(), "2026-11-10".into()],
            spark_conf: BTreeMap::from([("spark.sql.shuffle.partitions".into(), "8".into())]),
            cluster_ref: None,
            cluster: None,
            driver: Default::default(),
            restart_policy: Default::default(),
            time_to_live_seconds: None,
        };
        let pod = driver_pod(
            "job",
            "job-driver-1",
            "ns",
            &app,
            "c1",
            &cs,
            "py:1",
            &owner(),
        );
        let spec = pod.spec.unwrap_or_default();
        let sidecar = &spec.init_containers.unwrap_or_default()[0];
        assert_eq!(sidecar.restart_policy.as_deref(), Some("Always"));
        let args = sidecar.args.clone().unwrap_or_default();
        assert!(args.contains(&"--conf=spark.sql.shuffle.partitions=8".to_string()));
        assert!(args.contains(&"--conf=spark.app.name=job".to_string()));
        assert!(args.contains(&"--scheduler-url=http://c1-scheduler.ns.svc:50050".to_string()));
        let driver = &spec.containers[0];
        assert_eq!(driver.image.as_deref(), Some("py:1"));
        assert_eq!(
            driver.args.clone().unwrap_or_default(),
            vec!["--date", "2026-11-10"]
        );
        // The script presents the token the sidecar requires; the sidecar
        // reaches the cluster over mutual TLS.
        assert!(env_names(driver).contains(&"SPARK_CONNECT_AUTHENTICATE_TOKEN".to_string()));
        assert!(env_names(sidecar).contains(&"SPARK_CONNECT_AUTHENTICATE_TOKEN".to_string()));
        assert!(env_names(sidecar).contains(&"BALLISTA_TLS_CERT".to_string()));
        assert!(spec.volumes.iter().flatten().any(|v| {
            v.secret.as_ref().and_then(|s| s.secret_name.as_deref()) == Some("c1-security")
        }));
    }

    /// Every pod meets Pod Security "restricted" by default and has no API token.
    #[test]
    fn pods_are_restricted_by_default() {
        let s = spec();
        let o = owner();
        let mut pods: Vec<PodSpec> =
            vec![executor_pod("c1", "ns", &s, &o).spec.unwrap_or_default()];
        for (d, _) in [scheduler("c1", "ns", &s, &o), connect("c1", "ns", &s, &o)] {
            pods.push(d.spec.and_then(|d| d.template.spec).unwrap_or_default());
        }
        let app: BallistaApplicationSpec = serde_json::from_value(serde_json::json!({
            "mainApplicationFile": "local:///a.py"
        }))
        .unwrap_or_else(|e| unreachable!("{e}"));
        pods.push(
            driver_pod("a", "a-driver-1", "ns", &app, "c1", &s, "py", &o)
                .spec
                .unwrap_or_default(),
        );
        for p in pods {
            let psc = p.security_context.clone().unwrap_or_default();
            assert_eq!(psc.run_as_non_root, Some(true));
            assert_eq!(psc.run_as_user, Some(RUN_AS));
            assert_eq!(p.automount_service_account_token, Some(false));
            let all = p
                .containers
                .iter()
                .chain(p.init_containers.iter().flatten());
            for c in all {
                let sc = c.security_context.clone().unwrap_or_default();
                assert_eq!(sc.allow_privilege_escalation, Some(false), "{}", c.name);
                assert_eq!(sc.read_only_root_filesystem, Some(true), "{}", c.name);
                let mounts = c.volume_mounts.clone().unwrap_or_default();
                assert!(
                    mounts.iter().any(|m| m.mount_path == "/tmp"),
                    "{} has no /tmp",
                    c.name
                );
            }
        }
    }

    #[test]
    fn history_only_with_claim() {
        let mut s = spec();
        s.history.enabled = true;
        assert!(history("c1", "ns", &s, &owner()).is_none());
        s.history.event_log_claim = Some("logs".into());
        assert!(history("c1", "ns", &s, &owner()).is_some());
        let (d, _) = scheduler("c1", "ns", &s, &owner());
        let args = d
            .spec
            .and_then(|s| s.template.spec)
            .map(|p| p.containers[0].args.clone().unwrap_or_default())
            .unwrap_or_default();
        assert!(args.contains(&"--event-log-dir=/history".to_string()));
    }

    fn env_names(c: &Container) -> Vec<String> {
        c.env
            .iter()
            .flatten()
            .map(|e| e.name.clone())
            .collect()
    }

    fn secret_of(pod: &PodSpec) -> Option<String> {
        pod.volumes
            .iter()
            .flatten()
            .find(|v| v.name == TLS_VOLUME)
            .and_then(|v| v.secret.as_ref())
            .and_then(|s| s.secret_name.clone())
    }

    #[test]
    fn security_is_wired_into_every_component() {
        let s = spec();
        assert!(s.security.enabled, "security is on by default");

        let (d, _) = scheduler("c1", "ns", &s, &owner());
        let pod = d.spec.and_then(|d| d.template.spec).unwrap_or_default();
        let c = &pod.containers[0];
        let names = env_names(c);
        for n in ["BALLISTA_TLS_CERT", "BALLISTA_TLS_KEY", "BALLISTA_TLS_CA", "BALLISTA_UI_TOKEN_FILE"] {
            assert!(names.iter().any(|x| x == n), "scheduler lacks {n}");
        }
        let probe = c.readiness_probe.as_ref().and_then(|p| p.http_get.as_ref());
        assert_eq!(probe.and_then(|g| g.scheme.as_deref()), Some("HTTPS"));
        assert_eq!(probe.map(|g| g.port.clone()), Some(IntOrString::Int(UI_PORT)));
        assert_eq!(secret_of(&pod).as_deref(), Some("c1-security"));

        let exec = executor_pod("c1", "ns", &s, &owner()).spec.unwrap_or_default();
        let names = env_names(&exec.containers[0]);
        assert!(names.iter().any(|x| x == "BALLISTA_TLS_CERT"));
        assert!(!names.iter().any(|x| x == "BALLISTA_UI_TOKEN_FILE"));
        assert_eq!(secret_of(&exec).as_deref(), Some("c1-security"));

        let (d, _) = connect("c1", "ns", &s, &owner());
        let pod = d.spec.and_then(|d| d.template.spec).unwrap_or_default();
        let token = pod.containers[0]
            .env
            .iter()
            .flatten()
            .find(|e| e.name == "SPARK_CONNECT_AUTHENTICATE_TOKEN")
            .and_then(|e| e.value_from.as_ref())
            .and_then(|v| v.secret_key_ref.as_ref())
            .map(|k| (k.name.clone(), k.key.clone()));
        assert_eq!(
            token,
            Some(("c1-security".to_string(), "connect-token".to_string()))
        );
        assert!(!env_names(&pod.containers[0]).contains(&"BALLISTA_CONNECT_TLS".to_string()));

        let mut named = spec();
        named.security.secret_name = Some("mine".into());
        let (d, _) = scheduler("c1", "ns", &named, &owner());
        let pod = d.spec.and_then(|d| d.template.spec).unwrap_or_default();
        assert_eq!(secret_of(&pod).as_deref(), Some("mine"));
    }

    #[test]
    fn security_off_leaves_pods_plain() {
        let mut s = spec();
        s.security.enabled = false;
        let (d, _) = scheduler("c1", "ns", &s, &owner());
        let pod = d.spec.and_then(|d| d.template.spec).unwrap_or_default();
        assert!(secret_of(&pod).is_none());
        assert!(!env_names(&pod.containers[0]).iter().any(|n| n.starts_with("BALLISTA_TLS")));
        let probe = pod.containers[0].readiness_probe.as_ref().and_then(|p| p.http_get.as_ref());
        assert_eq!(probe.and_then(|g| g.scheme.as_deref()), None);
    }
}
