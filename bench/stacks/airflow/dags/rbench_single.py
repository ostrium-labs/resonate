"""rbench burst unit: one no-op step (bench/drivers/README.md, workload `burst`)."""

from __future__ import annotations

from airflow.sdk import dag, task


@dag(
    dag_id="rbench_single",
    schedule=None,
    catchup=False,
    max_active_runs=1024,
    tags=["rbench"],
)
def rbench_single():
    @task
    def noop(x: int = 0) -> int:
        return x

    noop()


rbench_single()
