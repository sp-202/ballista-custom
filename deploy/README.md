# Deploying Ballista (Spark-compatible) on Kubernetes

Three ways to run it, from simplest to most Spark-like:

| What | Where | Use it for |
|---|---|---|
| Helm chart `helm/ballista` | one static cluster per release | notebooks, BI, a fixed shared engine |
| Operator `helm/ballista-operator` + `BallistaCluster` | many clusters, executor autoscaling | shared clusters that grow and shrink with load |
| Operator + `BallistaApplication` | one job, on a shared or ephemeral cluster | batch jobs, spark-submit / SparkApplication style |
| `airflow/` package | Airflow DAGs | scheduling BallistaApplications like SparkKubernetesOperator |

Images (built on the host, see `../../cluster/`):
- `ballista-spark`: scheduler, executor, Spark Connect server, history server, operator.
- `ballista-pyspark`: driver image for applications (Python 3.13, pyspark-client 4.0.1, `/opt/ballista/run.py`).

## Helm chart

```sh
helm install ballista deploy/helm/ballista -n ballista --create-namespace
kubectl -n ballista port-forward svc/ballista-connect 15002   # sc://localhost:15002/;token=...
```

Security is on by default: `helm install` prints how to read the Spark Connect and UI tokens (see [Security](#security)).

Data paths must be visible to the connect server, scheduler and executors: use object stores or `dataVolumes`.

## Operator

```sh
kubectl apply --server-side -f deploy/helm/ballista-operator/crds/   # or let helm install them
helm install ballista-operator deploy/helm/ballista-operator -n ballista-system --create-namespace
```

`BallistaCluster` with Spark-style dynamic allocation:

```yaml
apiVersion: ballista.apache.org/v1alpha1
kind: BallistaCluster
metadata: {name: shared, namespace: ballista}
spec:
  image: localhost/ballista-spark:dev
  executor:
    vcores: 4
    resources: {requests: {cpu: "4", memory: 8Gi}, limits: {memory: 8Gi}}
    dynamicAllocation:
      enabled: true
      minExecutors: 1
      maxExecutors: 20
      schedulerBacklogTimeoutSeconds: 1    # spark.dynamicAllocation.schedulerBacklogTimeout
      executorIdleTimeoutSeconds: 60       # spark.dynamicAllocation.executorIdleTimeout
      shuffleTrackingTimeoutSeconds: 600   # keep executors holding shuffle data up to this long
```

The operator polls the scheduler's `GET /api/scaling` (pending and running task slots per executor). It adds executors when tasks have waited longer than the backlog timeout and removes idle ones. It never removes a busy executor, and it keeps an executor that holds shuffle output until the tracking timeout passes. Removed executors get SIGTERM and drain gracefully. The operator manages executor pods directly, as Spark on Kubernetes does, so the cluster needs no HPA or KEDA.

`BallistaApplication` (SparkApplication-like):

```yaml
apiVersion: ballista.apache.org/v1alpha1
kind: BallistaApplication
metadata: {name: daily-report, namespace: ballista}
spec:
  type: Python                       # or Sql with spec.sql
  mainApplicationFile: local:///jobs/report.py
  arguments: ["2026-11-10"]
  sparkConf: {spark.sql.shuffle.partitions: "64"}
  clusterRef: {name: shared}         # or cluster: {...} for an ephemeral cluster
  restartPolicy: {type: OnFailure, onFailureRetries: 2, onFailureRetryIntervalSeconds: 30}
  timeToLiveSeconds: 3600
```

The driver pod runs the user code with a Spark Connect server as a native sidecar, against the cluster's scheduler. `status.applicationState.state` goes through SUBMITTED, RUNNING, then COMPLETED or FAILED (PENDING_RERUN between retries). An ephemeral cluster is deleted when the application finishes.

## Airflow

```sh
pip install deploy/airflow
```

See `airflow/example_dags/ballista_tpch.py`. `BallistaApplicationOperator` creates the resource, waits for it, logs the driver output, and fails the task on FAILED. `BallistaApplicationSensor` waits on an application submitted earlier.

## Security

These defaults follow Spark on Kubernetes and the Pod Security Standard "restricted":

- Pods run as uid/gid 185 with runAsNonRoot and a RuntimeDefault seccomp profile, with no privilege escalation, all capabilities dropped and a read-only root filesystem (only `/tmp` and the work dir are writable).
- Ballista pods mount no service-account token. The operator gets least-privilege RBAC, namespaced when `watchNamespace` is set. Users get BallistaCluster/BallistaApplication rights through the built-in edit/view roles.
- Services are ClusterIP. The chart's `networkPolicy.enabled` restricts ingress to Ballista's own pods plus the clients you list.
- Credentials go in Secrets referenced through `envFrom`, or come from IRSA / Workload Identity, never from values or CRs.

### Authentication and encryption

`security.enabled` (chart) and `spec.security.enabled` (BallistaCluster) default to true. Each cluster gets a Secret, `<fullname>-security` from the chart (`ballista-security` for release "ballista") or `<cluster>-security` from the operator. It holds a private CA, one certificate signed by it, and two random tokens. The Secret is created once and kept across upgrades. Delete it to rotate everything, or point `security.existingSecret` / `spec.security.secretName` at your own Secret with the keys `ca.crt`, `tls.crt`, `tls.key`, `ui-token` and `connect-token`.

| Endpoint | Protection | Spark counterpart |
|---|---|---|
| Scheduler gRPC + REST (50050), executor gRPC and Flight shuffle/results | Mutual TLS: both ends present a certificate from the cluster CA. A process without one cannot register, submit or fetch data. | `spark.authenticate` + `spark.network.crypto.enabled` / `spark.ssl.rpc` |
| Spark Connect (15002) | Bearer token (`;token=` in the `sc://` URL). Optional TLS with `security.connectTls`. | `spark.connect.authenticate.token` |
| Web UI and REST (4040), history server (18090) | HTTPS with the cluster certificate plus the UI token. Browsers get a login prompt (any user name, the token as password). Tools send `Authorization: Bearer` or `X-Ballista-Token`. | `spark.ssl.ui` + `spark.ui.filters` / ACLs |

Liveness and readiness probes (`/healthz`, `/readyz`) need no token. The executor health port (50053) serves only health checks, over plain HTTP.

```sh
NS=ballista; SECRET=ballista-security       # chart release "ballista"
CONNECT_TOKEN=$(kubectl -n $NS get secret $SECRET -o jsonpath='{.data.connect-token}' | base64 -d)
kubectl -n $NS port-forward svc/ballista-connect 15002 &
python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote('sc://localhost:15002/;token=$CONNECT_TOKEN').getOrCreate()
spark.sql('select 1').show()"
```

PySpark sends a token over plaintext only to `localhost` (port-forward). In-cluster clients need `security.connectTls=true` and `;use_ssl=true` in the URL, with `GRPC_DEFAULT_SSL_ROOTS_FILE_PATH` pointing at the Secret's `ca.crt`. A BallistaApplication driver talks to its own sidecar on localhost, so the operator only injects the token.

The same settings work outside Kubernetes through environment variables:

| Variable | Effect |
|---|---|
| `BALLISTA_TLS_CERT`, `BALLISTA_TLS_KEY`, `BALLISTA_TLS_CA` | PEM files. Set all three to turn on mutual TLS. |
| `BALLISTA_TLS_SERVER_NAME` | Name checked in peer certificates (default `ballista-internal`). |
| `BALLISTA_UI_TOKEN` / `BALLISTA_UI_TOKEN_FILE` | UI/REST token (at least 16 characters). Also read by `ballista-cli --tui`. |
| `BALLISTA_UI_TLS=false` | Serve the UI over plain HTTP, for example behind a TLS-terminating ingress. |
| `SPARK_CONNECT_AUTHENTICATE_TOKEN` | Spark Connect token. `--conf spark.connect.authenticate.token=` also works. |
| `BALLISTA_CONNECT_TLS=true` or `--tls` | Serve Spark Connect over TLS with the cluster certificate. |

Not covered yet: encryption of shuffle and spill files at rest (`spark.io.encryption.enabled`), per-user identities and ACLs, and catalog authorization. A token holder is fully trusted. Treat the namespace as the boundary for anything finer.

## Local kind cluster

```sh
KIND=~/.local/bin/kind deploy/kind/up.sh      # cluster + images + chart
```
