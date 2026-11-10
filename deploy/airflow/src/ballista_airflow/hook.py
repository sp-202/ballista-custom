"""Thin client for BallistaApplication resources, built on the cncf.kubernetes connection."""

from __future__ import annotations

from typing import Any

from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from kubernetes import client

GROUP = "ballista.apache.org"
VERSION = "v1alpha1"
PLURAL = "ballistaapplications"

# status.applicationState.state values (SparkApplication-compatible).
SUCCESS_STATES = {"COMPLETED"}
FAILURE_STATES = {"FAILED", "SUBMISSION_FAILED"}


class BallistaHook:
    def __init__(self, kubernetes_conn_id: str = "kubernetes_default"):
        self._k8s = KubernetesHook(conn_id=kubernetes_conn_id)

    @property
    def _api(self) -> client.ApiClient:
        return self._k8s.api_client

    def create(self, namespace: str, body: dict[str, Any]) -> dict[str, Any]:
        return client.CustomObjectsApi(self._api).create_namespaced_custom_object(
            GROUP, VERSION, namespace, PLURAL, body
        )

    def get(self, namespace: str, name: str) -> dict[str, Any]:
        return client.CustomObjectsApi(self._api).get_namespaced_custom_object(
            GROUP, VERSION, namespace, PLURAL, name
        )

    def delete(self, namespace: str, name: str) -> None:
        try:
            client.CustomObjectsApi(self._api).delete_namespaced_custom_object(
                GROUP, VERSION, namespace, PLURAL, name
            )
        except client.ApiException as e:
            if e.status != 404:
                raise

    def driver_log(self, namespace: str, pod: str, tail_lines: int | None = None) -> str:
        try:
            return client.CoreV1Api(self._api).read_namespaced_pod_log(
                pod, namespace, container="driver", tail_lines=tail_lines
            )
        except client.ApiException as e:
            return f"<driver log unavailable: {e.status} {e.reason}>"


def app_state(app: dict[str, Any]) -> tuple[str, str]:
    """(state, error message) of a BallistaApplication; state '' before the first reconcile."""
    st = (app.get("status") or {}).get("applicationState") or {}
    return st.get("state") or "", st.get("errorMessage") or ""


def driver_pod(app: dict[str, Any]) -> str | None:
    return ((app.get("status") or {}).get("driverInfo") or {}).get("podName")
