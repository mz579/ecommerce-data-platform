"""phase6_retry_demo — 重试机制演示 DAG（Phase 6，专用、不触碰生产数据）。

目的
----
演示 Phase 6 已显式声明的重试行为（``retries`` 是兜底而非兜底逻辑的替代）：
一个只失败前若干次尝试、之后成功的任务，在默认 ``retries=2`` 下自动重试并
最终绿。任务不做任何数据库写操作（纯内存计算 + 打印），不会影响 ODS/DWD/
DWS/ADS 或任何控制表。

用法
----
    airflow dags trigger phase6_retry_demo --conf '{"fail_first_tries": 1}'

- ``fail_first_tries``（默认 1）：让前 N 次尝试抛异常（人为注入的"瞬态故障"），
  之后成功；配合默认 ``retries=2`` 展示"失败 -> 重试 -> 成功"。
- ``lag_seconds``（默认 1）：每次尝试的延时，模拟工作任务最短耗时。
- 只有这一个任务，没有任何上游/下游；schedule 为 ``None``，只手动触发，
  绝不参与日常调度。任务无副作用：重试多少次结果都一样，这正是幂等决定论
  在无状态任务上给出的安全网。
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

DAG_ID = "phase6_retry_demo"


def _flaky(**context):
    ti = context["ti"]
    try_number = getattr(ti, "try_number", 1)  # 1 = 首次尝试
    dag_run = context.get("dag_run")
    conf = dict(dag_run.conf) if dag_run and dag_run.conf else {}
    fail_first = int(conf.get("fail_first_tries", 1))
    lag = float(conf.get("lag_seconds", 1))
    print(f"[retry-demo] try #{try_number} starting (fail_first_tries={fail_first})")
    time.sleep(max(0.0, lag))
    if try_number <= fail_first:
        raise RuntimeError(
            f"[retry-demo] injected transient failure on try #{try_number} "
            f"(no DB touched; nothing to roll back)"
        )
    print(f"[retry-demo] try #{try_number} Succeeded after {try_number - 1} retries")
    return {"tries_total": try_number, "injected_failures": fail_first}


default_args = {
    "owner": "dataeng",
    # 兜底：与主 DAG 相同的重试策略；展示这一点本身就是本 DAG 的目标。
    "retries": 2,
    "retry_delay": timedelta(seconds=5),
    "retry_exponential_backoff": True,
}

dag = DAG(
    dag_id=DAG_ID,
    description="重试演示：注入瞬态失败后自动重试并成功（不触碰任何生产/控制数据）",
    schedule=None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["ecommerce", "demo", "phase6"],
    doc_md=__doc__,
)

demo = PythonOperator(
    task_id="flaky_but_eventually_succeeds",
    python_callable=_flaky,
    dag=dag,
    execution_timeout=timedelta(minutes=5),
)
