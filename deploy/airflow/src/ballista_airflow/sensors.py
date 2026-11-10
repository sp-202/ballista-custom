from __future__ import annotations

from typing import Any, Sequence

from airflow.exceptions import AirflowException

try:  # Airflow 3
    from airflow.sdk import BaseSensorOperator
except ImportError:  # Airflow 2
    from airflow.sensors.base import BaseSensorOperator

from ballista_airflow.hook import FAILURE_STATES, SUCCESS_STATES, BallistaHook, app_state, driver_pod


class BallistaApplicationSensor(BaseSensorOperator):
    """Wait for a BallistaApplication to complete (like SparkKubernetesSensor).

    Pair with ``BallistaApplicationOperator(wait_for_completion=False,
    delete_on_termination=False)`` and pull the name from XCom:
    ``application_name="{{ ti.xcom_pull('submit')['name'] }}"``.
    """

    template_fields: Sequence[str] = ("application_name", "namespace")

    def __init__(
        self,
        *,
        application_name: str,
        namespace: str = "default",
        kubernetes_conn_id: str = "kubernetes_default",
        attach_log: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.application_name = application_name
        self.namespace = namespace
        self.kubernetes_conn_id = kubernetes_conn_id
        self.attach_log = attach_log

    def poke(self, context: Any) -> bool:
        hook = BallistaHook(self.kubernetes_conn_id)
        app = hook.get(self.namespace, self.application_name)
        state, error = app_state(app)
        self.log.info("%s: %s", self.application_name, state or "NEW")
        if state in SUCCESS_STATES | FAILURE_STATES:
            pod = driver_pod(app)
            if self.attach_log and pod:
                self.log.info("driver log (%s):\n%s", pod, hook.driver_log(self.namespace, pod))
            if state in FAILURE_STATES:
                raise AirflowException(f"BallistaApplication {self.application_name} {state}: {error}")
            return True
        return False
