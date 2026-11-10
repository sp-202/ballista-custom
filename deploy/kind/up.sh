#!/usr/bin/env bash
# Create (or reuse) the local kind cluster, load the Ballista image, install the chart.
#   deploy/kind/up.sh                 # Ballista chart
#   deploy/kind/up.sh operator        # + the operator (BallistaCluster / BallistaApplication)
# The cluster gets its own kubeconfig, so ~/.kube/config (and its current context) is
# never touched:  export KUBECONFIG=~/.kube/kind-ballista.yaml
set -euo pipefail
cd "$(dirname "$0")/../.."
export KUBECONFIG=${KUBECONFIG_KIND:-$HOME/.kube/kind-ballista.yaml}
CTX=kind-ballista
IMAGE=${IMAGE:-localhost/ballista-spark:dev}
DRIVER_IMAGE=${DRIVER_IMAGE:-localhost/ballista-pyspark:dev}
KIND=${KIND:-kind}

# Pods run as uid 185 (restricted Pod Security); the hostPath warehouse must be writable.
mkdir -p /mnt/data/ballista-kind/warehouse && chmod 1777 /mnt/data/ballista-kind/warehouse
if ! $KIND get clusters 2>/dev/null | grep -qx ballista; then
  $KIND create cluster --config deploy/kind/kind-config.yaml --wait 120s
fi

# The image is built with podman; kind (docker) loads it from an archive.
ARCHIVE=$(mktemp --suffix .tar)
trap 'rm -f "$ARCHIVE"' EXIT
for img in "$IMAGE" "$DRIVER_IMAGE"; do
  podman image exists "$img" || continue
  rm -f "$ARCHIVE"; podman save --format docker-archive -o "$ARCHIVE" "$img"
  $KIND load image-archive "$ARCHIVE" --name ballista
done

helm --kube-context $CTX upgrade --install ballista deploy/helm/ballista \
  -n ballista --create-namespace -f deploy/kind/values-kind.yaml --wait --timeout 5m
if [[ "${1:-}" == operator ]]; then
  helm --kube-context $CTX upgrade --install ballista-operator deploy/helm/ballista-operator \
    -n ballista-system --create-namespace --set image.pullPolicy=Never --wait --timeout 3m
fi
kubectl --context $CTX -n ballista get pods -o wide
