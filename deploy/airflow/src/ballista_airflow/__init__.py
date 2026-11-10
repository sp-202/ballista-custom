"""Airflow integration for the Ballista Kubernetes operator.

BallistaApplicationOperator plays the role of SparkKubernetesOperator: it creates a
BallistaApplication custom resource, waits for it to finish, and streams the driver log.
"""

from ballista_airflow.operators import BallistaApplicationOperator
from ballista_airflow.sensors import BallistaApplicationSensor

__all__ = ["BallistaApplicationOperator", "BallistaApplicationSensor", "get_provider_info"]


def get_provider_info():
    return {
        "package-name": "ballista-airflow",
        "name": "Ballista",
        "description": "Submit Spark-compatible jobs to Ballista on Kubernetes.",
        "versions": ["0.1.0"],
    }
