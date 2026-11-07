"""DAG contract tests for ``ecommerce_daily_pipeline`` (Phase 8).

These run against the **real Airflow runtime**; on a host without ``apache-airflow``
the module is skipped (the scheduler container is the canonical place to run it):

    docker compose exec -T airflow-scheduler python -m pytest tests/dag -q

Covered contracts:

1. DAG import is side-effect free: importing the DAG file must not open a MySQL or
   PostgreSQL connection (the DAG only reads env vars / builds operators).
2. The ten tasks form exactly the main-plan linear chain, no extra roots/leaves.
3. Failure-retry policy is the documented one: transient tasks retries=2 /
   retry_delay=2m, deterministic tasks (start / register_batch / publish_success)
   retries=0, and every task has an execution timeout.
4. Schedule semantics: 04:00 UTC, catchup disabled, max_active_runs=1.
5. Bash tasks call the real project CLIs (no dummy placeholders).
"""

from __future__ import annotations

import importlib.util
import os
import sys
from datetime import timedelta
from pathlib import Path

import pytest

pytest.importorskip("airflow.models")

from airflow.models import DagBag  # noqa: E402

DAG_ID = "ecommerce_daily_pipeline"


def _dag_file() -> Path:
    """Locate the DAG both in the repo checkout and in the scheduler container.

    Host: ``<repo>/airflow/dags/...``. Container: DAGs are bind-mounted at
    ``/opt/airflow/dags`` (``AIRFLOW__CORE__DAGS_FOLDER``), not under the repo
    tree, because the Airflow image keeps its own DAG folder.
    """
    candidates = [
        Path(__file__).resolve().parents[2] / "airflow" / "dags" / "ecommerce_daily_pipeline.py",
        Path("/opt/airflow/dags/ecommerce_daily_pipeline.py"),
    ]
    folder = os.environ.get("AIRFLOW__CORE__DAGS_FOLDER")
    if folder:
        candidates.insert(0, Path(folder) / "ecommerce_daily_pipeline.py")
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(f"DAG file not found in any of {candidates}")


DAG_FILE = _dag_file()

CHAIN = (
    "start",
    "check_source_files",
    "register_batch",
    "load_ods",
    "validate_ods",
    "build_dwd",
    "build_dws",
    "build_ads",
    "quality_gate",
    "publish_success",
)
DETERMINISTIC_TASKS = {"start", "register_batch", "publish_success"}
BASH_TASKS = {
    "check_source_files": "python -m ecommerce_dw.cli check-sources",
    "validate_ods": "python -m ecommerce_dw.cli validate-ods",
    "build_dwd": "python -m ecommerce_dw.dwd.cli build",
    "build_dws": "python -m ecommerce_dw.dws.cli build",
    "build_ads": "python -m ecommerce_dw.dws.cli verify",
}


@pytest.fixture(scope="module")
def dag():
    bag = DagBag(dag_folder=str(DAG_FILE.parent), include_examples=False)
    assert bag.import_errors == {}, bag.import_errors
    loaded = bag.get_dag(DAG_ID)
    assert loaded is not None
    return loaded


def test_dag_import_has_no_database_side_effects(monkeypatch):
    """Importing the DAG must not touch either database."""
    import psycopg2
    import pymysql

    calls: list[str] = []

    def _boom(label):
        def _connect(*args, **kwargs):
            calls.append(label)
            raise AssertionError(f"module import opened a {label} connection")

        return _connect

    monkeypatch.setattr(pymysql, "connect", _boom("mysql"))
    monkeypatch.setattr(psycopg2, "connect", _boom("postgres"))

    module_name = "phase8_dag_import_probe"
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(module_name, DAG_FILE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert calls == []
    assert module.dag.dag_id == DAG_ID
    assert len(module.dag.tasks) == len(CHAIN)


def test_task_chain_is_linear_and_complete(dag):
    assert len(dag.tasks) == len(CHAIN)
    assert {t.task_id for t in dag.tasks} == set(CHAIN)

    first = dag.get_task(CHAIN[0])
    last = dag.get_task(CHAIN[-1])
    assert first.upstream_task_ids == set()
    assert last.downstream_task_ids == set()

    for upstream, downstream in zip(CHAIN, CHAIN[1:], strict=False):
        up = dag.get_task(upstream)
        down = dag.get_task(downstream)
        assert down.task_id in up.downstream_task_ids
        assert up.task_id in down.upstream_task_ids

    for task_id in CHAIN[1:-1]:
        task = dag.get_task(task_id)
        assert len(task.upstream_task_ids) == 1, task_id
        assert len(task.downstream_task_ids) == 1, task_id


def test_retry_and_timeout_policy(dag):
    for task_id in CHAIN:
        task = dag.get_task(task_id)
        assert task.execution_timeout is not None, task_id
        if task_id in DETERMINISTIC_TASKS:
            assert task.retries == 0, task_id
        else:
            assert task.retries == 2, task_id
            assert task.retry_delay == timedelta(minutes=2), task_id


def test_schedule_semantics(dag):
    assert dag.schedule_interval == "0 4 * * *"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.default_args["depends_on_past"] is False


def test_bash_tasks_call_real_project_clis(dag):
    for task_id, fragment in BASH_TASKS.items():
        task = dag.get_task(task_id)
        assert fragment in task.bash_command, task_id
    # Jinja-rendered build commands still embed the real CLI, never a dummy.
    for task_id in ("build_dwd", "build_dws", "build_ads"):
        command = dag.get_task(task_id).bash_command
        assert "python -m ecommerce_dw." in command
        assert "exit 0" in command  # legitimate stable-state no-op branch
