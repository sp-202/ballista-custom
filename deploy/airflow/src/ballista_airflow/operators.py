from __future__ import annotations

import time
import zlib
from pathlib import Path
from typing import Any, Sequence

import yaml
from airflow.exceptions import AirflowException

try:  # Airflow 3
    from airflow.sdk import BaseOperator
except ImportError:  # Airflow 2
    from airflow.models import BaseOperator

from ballista_airflow.hook import (
    FAILURE_STATES,
    GROUP,
    SUCCESS_STATES,
    VERSION,
    BallistaHook,
    app_state,
    driver_pod,
)


class BallistaApplicationOperator(BaseOperator):
    """Run a BallistaApplication and wait for it, like SparkKubernetesOperator.

    Give the application either as ``application_file`` (a YAML path or YAML string,
    templated) or as ``spec`` (the BallistaApplication spec as a dict). Each task try
    creates its own resource, named ``name`` plus the try number unless ``unique_name``
    is False.

    Example::

        BallistaApplicationOperator(
            task_id="daily_agg",
            namespace="ballista",
            name="daily-agg",
            spec={
                "type": "Python",
                "mainApplicationFile": "local:///jobs/daily_agg.py",
                "arguments": ["{{ ds }}"],
                "clusterRef": {"name": "shared"},
            },
        )
    """

    template_fields: Sequence[str] = ("application_file", "spec", "name", "namespace")
    template_ext: Sequence[str] = (".yaml", ".yml")
    template_fields_renderers = {"application_file": "yaml", "spec": "json"}
    ui_color = "#e4f0e8"

    def __init__(
        self,
        *,
        namespace: str = "default",
        name: str | None = None,
        application_file: str | None = None,
        spec: dict[str, Any] | None = None,
        kubernetes_conn_id: str = "kubernetes_default",
        wait_for_completion: bool = True,
        poll_interval: float = 10,
        get_logs: bool = True,
        delete_on_termination: bool = True,
        unique_name: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if (application_file is None) == (spec is None):
            raise ValueError("set exactly one of application_file or spec")
        self.namespace = namespace
        self.name = name
        self.application_file = application_file
        self.spec = spec
        self.kubernetes_conn_id = kubernetes_conn_id
        self.wait_for_completion = wait_for_completion
        self.poll_interval = poll_interval
        self.get_logs = get_logs
        self.delete_on_termination = delete_on_termination
        self.unique_name = unique_name
        self._created: str | None = None

    def _body(self, context: Any) -> dict[str, Any]:
        if self.application_file is not None:
            text = self.application_file
            if "\n" not in text and Path(text).is_file():
                text = Path(text).read_text()
            body = yaml.safe_load(text)
        else:
            body = {"spec": self.spec}
        body["apiVersion"] = f"{GROUP}/{VERSION}"
        body["kind"] = "BallistaApplication"
        meta = body.setdefault("metadata", {})
        base = self.name or meta.get("name") or self.task_id.replace("_", "-").lower()
        if self.unique_name:
            ti = context["ti"]
            run = zlib.crc32(context["run_id"].encode()) % 10**6
            base = f"{base[:40]}-{run}-{ti.try_number}"
        meta["name"] = base
        meta.pop("namespace", None)
        labels = meta.setdefault("labels", {})
        labels["airflow.apache.org/dag-id"] = self.dag_id[:63]
        labels["airflow.apache.org/task-id"] = self.task_id[:63]
        return body

    def execute(self, context: Any) -> dict[str, Any]:
        hook = BallistaHook(self.kubernetes_conn_id)
        body = self._body(context)
        created = hook.create(self.namespace, body)
        self._created = name = created["metadata"]["name"]
        self.log.info("created BallistaApplication %s/%s", self.namespace, name)
        if not self.wait_for_completion:
            return {"name": name, "namespace": self.namespace}
        try:
            app = self._wait(hook, name)
        finally:
            if self.delete_on_termination:
                hook.delete(self.namespace, name)
                self._created = None
        return {"name": name, "namespace": self.namespace, "status": app.get("status")}

    def _wait(self, hook: BallistaHook, name: str) -> dict[str, Any]:
        last = None
        while True:
            app = hook.get(self.namespace, name)
            state, error = app_state(app)
            if state != last:
                self.log.info("%s: %s", name, state or "NEW")
                last = state
            if state in SUCCESS_STATES or state in FAILURE_STATES:
                pod = driver_pod(app)
                if self.get_logs and pod:
                    self.log.info("driver log (%s):\n%s", pod, hook.driver_log(self.namespace, pod))
                if state in FAILURE_STATES:
                    raise AirflowException(f"BallistaApplication {name} {state}: {error}")
                return app
            time.sleep(self.poll_interval)

    def on_kill(self) -> None:
        if self._created:
            BallistaHook(self.kubernetes_conn_id).delete(self.namespace, self._created)
