"""Example: run SQL and a PySpark file on Ballista from Airflow.

Needs a `kubernetes_default` connection (in-cluster or kubeconfig) whose identity may
create ballistaapplications in the namespace (the operator chart aggregates that into
the built-in "edit" role).
"""

from datetime import datetime

from airflow import DAG

from ballista_airflow import BallistaApplicationOperator, BallistaApplicationSensor

with DAG(
    "ballista_tpch",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ballista"],
) as dag:
    # SQL on the shared cluster "ballista" (a BallistaCluster in the same namespace).
    count = BallistaApplicationOperator(
        task_id="count_lineitem",
        namespace="ballista",
        spec={
            "type": "Sql",
            "sql": "SELECT count(*) FROM parquet.`/data/tpch/sf1/lineitem`",
            "clusterRef": {"name": "ballista"},
        },
    )

    # A PySpark job on its own ephemeral cluster, created and deleted with the job.
    submit = BallistaApplicationOperator(
        task_id="submit_report",
        namespace="ballista",
        wait_for_completion=False,
        delete_on_termination=False,
        spec={
            "type": "Python",
            "mainApplicationFile": "https://example.com/jobs/report.py",
            "arguments": ["{{ ds }}"],
            "driver": {"pythonPackages": ["requests"]},
            "cluster": {"executor": {"instances": 2, "vcores": 4}},
            "timeToLiveSeconds": 3600,
        },
    )
    wait = BallistaApplicationSensor(
        task_id="wait_report",
        namespace="ballista",
        application_name="{{ ti.xcom_pull('submit_report')['name'] }}",
        poke_interval=15,
    )

    count >> submit >> wait
