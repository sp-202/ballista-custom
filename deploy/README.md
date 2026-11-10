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
kubectl -n ballista port-forward svc/ballista-connect 15002   # sc://localhost:15002
```

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

Not yet available: TLS and authentication on gRPC, Flight and Spark Connect (roadmap step 10, the counterpart of `spark.authenticate` and `spark.ssl`). Until then, treat the namespace as the trust boundary.

## Local kind cluster

```sh
KIND=~/.local/bin/kind deploy/kind/up.sh      # cluster + images + chart
```
