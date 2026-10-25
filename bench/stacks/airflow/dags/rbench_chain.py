"""rbench chain: L sequential no-op steps, each followed by a durable sleep of S ms.

Triggered by REST with conf {"l": L, "sleep_ms": S}; defaults L=5, S=1000. The task
graph is fixed at parse time, so the DAG is built for MAX_L=5 steps. Each step is a
short-circuit task: steps i < L return their input ({"i": i}, truthy) and the chain goes
on; step L (when L < 5) returns False, and the short-circuit skips every downstream task
in one go (one extra worker task, no per-task skip cascade through the scheduler). The
run still ends in `success`. L > 5 is clamped by the graph (only 5 steps exist).

The sleep is durable: `sleep_i` defers to a DateTimeTrigger with end_from_trigger=True,
so the wait lives in the triggerer (state in the metadata DB, survives worker and
triggerer restarts) and the task is marked done by the triggerer without returning to a
worker. The S ms are measured from the sleep task's start, i.e. just after step_i ended
and the scheduler queued sleep_i.
"""

from __future__ import annotations

from datetime import timedelta

from airflow.providers.standard.triggers.temporal import DateTimeTrigger
from airflow.sdk import BaseOperator, dag, get_current_context, task, timezone

MAX_L = 5
DEFAULT_L = 5
DEFAULT_SLEEP_MS = 1000


class DurableSleep(BaseOperator):
    """Defer for conf.sleep_ms milliseconds; the triggerer completes the task."""

    def execute(self, context):
        conf = context["dag_run"].conf or {}
        ms = int(conf.get("sleep_ms", DEFAULT_SLEEP_MS))
        moment = timezone.utcnow() + timedelta(milliseconds=ms)
        self.defer(
            trigger=DateTimeTrigger(moment=moment, end_from_trigger=True),
            method_name="execute_complete",
        )

    def execute_complete(self, context, event=None):
        return None


@dag(
    dag_id="rbench_chain",
    schedule=None,
    catchup=False,
    max_active_runs=256,
    max_active_tasks=1024,
    tags=["rbench"],
)
def rbench_chain():
    prev = None
    for i in range(MAX_L):

        @task.short_circuit(task_id=f"step_{i}")
        def step(i: int = i):
            conf = get_current_context()["dag_run"].conf or {}
            if i >= int(conf.get("l", DEFAULT_L)):
                return False  # beyond L: skip the rest of the chain
            return {"i": i}  # no-op: returns its input

        s = step()
        z = DurableSleep(task_id=f"sleep_{i}")
        if prev is not None:
            prev >> s
        s >> z
        prev = z


rbench_chain()
