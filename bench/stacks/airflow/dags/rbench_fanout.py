"""rbench fanout: K parallel no-op steps, then a join (bench/drivers/README.md).

Triggered by REST with conf {"k": K}; default K=8. Dynamic task mapping needs the
mapped-over list at run time, so a `plan` task reads conf.k and returns range(K);
`noop` is expanded over it and `join` waits for every mapped instance.
"""

from __future__ import annotations

from airflow.sdk import dag, get_current_context, task

DEFAULT_K = 8


@dag(
    dag_id="rbench_fanout",
    schedule=None,
    catchup=False,
    max_active_runs=256,
    max_active_tasks=1024,
    tags=["rbench"],
)
def rbench_fanout():
    @task
    def plan() -> list[int]:
        conf = get_current_context()["dag_run"].conf or {}
        return list(range(int(conf.get("k", DEFAULT_K))))

    @task
    def noop(i: int) -> int:
        return i

    @task
    def join(results: list[int]) -> int:
        return len(list(results))

    join(noop.expand(i=plan()))


rbench_fanout()
