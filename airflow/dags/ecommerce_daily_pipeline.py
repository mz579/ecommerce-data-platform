"""ecommerce_daily_pipeline — 电商数仓日任务链（Phase 6，Airflow 编排）。

任务序列（主方案 §7）：
    start -> check_source_files -> register_batch -> load_ods -> validate_ods
    -> build_dwd -> build_dws -> build_ads -> quality_gate -> publish_success

时间语义
--------
- 所有日期都是**业务日期**（UTC 日，``data_interval`` / 逻辑日期驱动），
  不依赖机器当前日期；同一逻辑日期可任意重跑。
- 窗口解析（``ecommerce_dw.ops.window.resolve_window``）：
    * 无 ``conf`` 时 = TAIL 模式：窗口 = 逻辑日期当天，具体重建哪几天由
      Phase 5 水位线规划器在 ``register_batch`` 运行时决定（稳定态 = 无受影响日）。
    * ``conf={"dates": "d1,d2,..."}`` 或 ``conf={"from": d1, "to": d2}`` =
      FORCE 模式：显式回填区间（深修正，与 ``incremental --from/--to`` 语义一致）。
- 回填：用 ``airflow dags trigger --conf '{"from": "2015-09-16", "to": "2015-09-17"}'``。

调度配置（含取值理由）
----------------------
- ``schedule="0 4 * * *"``：每天 04:00 UTC 跑一次；业务数据按 UTC 日界落仓，
  3 点后为 7 天迟到窗口内的重算留出回看余量。
- ``catchup=False``：暂停/中断期间不补跑历史区间（补跑走 conf 显式回填，
  语义可预测，而非隐性堆跑）。
- ``max_active_runs=1``：各层构建对同一 scope 是删除再重插，互相同窗口并发重建
  会互相清表，必须串行。
- ``retries=2`` / ``retry_delay=2m``（默认）：只针对瞬态故障（MySQL/网络抖动）
  做一次兜底；**幂等是根本手段，重试只是兜底**——同一 scope 的重跑内容恒等
  （DWD/DWS/ADS 构建器都是 scope 幂等重建，水位只在实际构建成功后推进）。
  确定性任务（``start``、``register_batch``、``publish_success``）显式
  ``retries=0``：这类失败是输入/逻辑错误而非瞬态，重试只会重复同样的失败。
- ``execution_timeout``：每任务显式设定；最重的单任务是 ODS 全量装载
  （item_properties 2 个文件约 20M 行）与 DWD/DWS 重建，留 1-3h 余量，
  超过即视为卡死失败，不留无限占用。

凭据
----
不用 Airflow Connections：本项目 CLI 一律读环境变量（``MYSQL_*`` / ``POSTGRES_*``），
凭据由 ``docker compose`` 从宿主机 ``.env`` 注入容器、绝不出现在 DAG 源码里
（理由见 ``docs/airflow_design.md``）。

XCom
----
只传小型元数据（窗口、batch_id、受影响日期 CSV、门禁 run_id、发布 id 摘要），
绝不传数据行。
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from ecommerce_dw.config import DbConfig, db_config_from_env
from ecommerce_dw.ops.control import finish_run, plan_and_register, publish_after_gate
from ecommerce_dw.ops.window import parse_dates_csv, resolve_window
from ecommerce_dw.quality.runner import run_quality_gate

DAG_ID = "ecommerce_daily_pipeline"
OWNER = "dataeng"
_MYSQL_DEFAULTS = {"PORT": "3306", "USER": "ecom_app", "DATABASE": "ecommerce_dw"}


def _cfg() -> DbConfig:
    return db_config_from_env("MYSQL", engine="mysql", defaults=_MYSQL_DEFAULTS)


# --------------------------------------------------------------------------- #
# task glue (PythonOperator callables — real project logic, thin XCom)         #
# --------------------------------------------------------------------------- #


def _start(**context):
    """窗口气象台：纯函数解析业务日期窗口并推到 XCom。"""
    ti = context["ti"]
    dag_run = context.get("dag_run")
    conf = dict(dag_run.conf) if dag_run and dag_run.conf else {}
    logical_date = context.get("logical_date")
    data_interval_start = context.get("data_interval_start")
    window = resolve_window(
        logical_date=logical_date.date() if logical_date else None,
        data_interval_start=(data_interval_start.date() if data_interval_start else None),
        conf=conf,
    )
    print(
        f"[start] mode={window.mode} window={window.dates_csv} "
        f"logical_date={window.logical_date or '-'} conf={conf!r}"
    )
    ti.xcom_push(key="mode", value=window.mode)
    ti.xcom_push(key="window", value=window.dates_csv)
    if window.logical_date is not None:
        ti.xcom_push(key="logical_date", value=window.logical_date.isoformat())
    return window.as_dict()


def _register_batch(**context):
    """水位线规划 + 控制批次登记（写 airflow_pipeline_run RUNNING 行）。"""
    ti = context["ti"]
    window = ti.xcom_pull(task_ids="start", key="window")
    mode = ti.xcom_pull(task_ids="start", key="mode")
    dag_run = context.get("dag_run")
    dag_run_id = dag_run.run_id if dag_run else "manual"
    result = plan_and_register(_cfg(), mode=mode, window=window, dag_run_id=dag_run_id)
    print(
        f"[register_batch] batch_id={result['batch_id']} mode={result['mode']} "
        f"window={result['window']} dwd_dates={result['dwd_dates'] or '(none)'} "
        f"agg_dates={result['agg_dates'] or '(none)'} "
        f"(dwd_count={result['dwd_count']} agg_count={result['agg_count']})"
    )
    return result


def _quality_gate(**context):
    """范围硬门禁：只对本次实际重建的日期跑 60 条规则（stable-state 则跳过）。"""
    ti = context["ti"]
    reg = ti.xcom_pull(task_ids="register_batch", key="return_value") or {}
    dwd_dates = reg.get("dwd_dates") or ""
    if not dwd_dates:
        print(
            "[quality_gate] register_batch planned no rebuild dates for this window "
            "- hard gate skipped as a stable-state no-op (nothing changed)"
        )
        return {"status": "SKIPPED", "run_id": None}
    dates = parse_dates_csv(dwd_dates)
    started = time.monotonic()
    report = run_quality_gate(_cfg(), dates=dates)
    duration = time.monotonic() - started
    print(
        f"[quality_gate] run #{report.run_id} {report.status} "
        f"{report.passed}/{report.rules_total} rules passed "
        f"({report.checks_total:,} rows checked) dates={dwd_dates} "
        f"duration={duration:.2f}s"
    )
    return {
        "status": report.status,
        "run_id": report.run_id,
        "passed": report.passed,
        "rules_total": report.rules_total,
        "checks_total": report.checks_total,
    }


def _publish_success(**context):
    """受门禁保护的发布收尾：写 ads_publish_log + 关闭控制批次。"""
    ti = context["ti"]
    reg = ti.xcom_pull(task_ids="register_batch", key="return_value") or {}
    gate = ti.xcom_pull(task_ids="quality_gate", key="return_value") or {}
    batch_id = reg.get("batch_id")
    dwd_dates = reg.get("dwd_dates") or ""
    window = reg.get("window") or ""
    gate_run_id = gate.get("run_id")
    gate_status = gate.get("status")
    if not dwd_dates:
        if batch_id:
            finish_run(
                _cfg(),
                batch_id,
                "SUCCESS",
                published="SKIPPED",
                quality_run_id=gate_run_id,
                error_summary="stable-state re-run: nothing rebuilt in window",
            )
        print(
            f"[publish_success] nothing rebuilt in window={window or '-'} - "
            f"ADS publish skipped (no data change); control batch {batch_id} closed "
            "SUCCESS/SKIPPED"
        )
        return {"published": False, "publish_id": None, "reason": "nothing-rebuilt"}
    if gate_status != "PASS":
        raise RuntimeError(
            f"publish refused: quality gate run #{gate_run_id} status={gate_status} "
            "(not PASS) - fix data and re-run the DAG"
        )
    result = publish_after_gate(
        _cfg(),
        batch_id=batch_id,
        window=window or dwd_dates,
        dwd_dates=dwd_dates,
        gate_run_id=gate_run_id,
        gate_status=gate_status,
    )
    print(
        f"[publish_success] published={result['published']} "
        f"publish_id={result['publish_id']} gate_run={gate_run_id}"
    )
    return result


def _dag_failure_callback(context):
    """把仍 RUNNING 的控制批次标记 FAILED（尽力而为，永不吞掉异常之外的事）。"""
    ti = context.get("ti")
    try:
        reg = ti.xcom_pull(task_ids="register_batch", key="return_value") or {}
        batch_id = reg.get("batch_id")
        if batch_id:
            finish_run(_cfg(), batch_id, "FAILED", error_summary="dag run failed")
            print(f"[dag-failure] control batch {batch_id} marked FAILED")
    except Exception as exc:  # pragma: no cover - callback must stay best-effort
        print(f"[dag-failure] could not mark the control run FAILED: {exc}")
    return None


# --------------------------------------------------------------------------- #
# bash commands (rendered with task-context Jinja, real CLIs only)             #
# --------------------------------------------------------------------------- #

CHECK_SOURCES_CMD = "python -m ecommerce_dw.cli check-sources"

# 绝对路径：BashOperator 的子进程 cwd 不保证是仓库根，而 ingestion.cli load
# 用 Path(file).resolve()（相对 cwd）。容器内仓库根固定 /opt/ecommerce_dw
# （docker-compose 绑定挂载），各 CLI 的 PROJECT_ROOT 也解析到这里（editable 安装）。
LOAD_ODS_CMD = (
    "python -m ecommerce_dw.ingestion.cli load "
    "/opt/ecommerce_dw/data/raw/retailrocket/category_tree.csv &&\n"
    "python -m ecommerce_dw.ingestion.cli load "
    "/opt/ecommerce_dw/data/raw/retailrocket/events.csv &&\n"
    "python -m ecommerce_dw.ingestion.cli load "
    "/opt/ecommerce_dw/data/raw/retailrocket/item_properties_part1.csv &&\n"
    "python -m ecommerce_dw.ingestion.cli load "
    "/opt/ecommerce_dw/data/raw/retailrocket/item_properties_part2.csv"
)

VALIDATE_ODS_CMD = "python -m ecommerce_dw.cli validate-ods"

# 稳定态（register 规划无受影响日期）时各构建任务是真实逻辑的合法 no-op：
# 跳过原因由 register_batch 的水位线比对产生，这里把原因打日志并 exit 0。
BUILD_DWD_CMD = r"""
{% set reg = ti.xcom_pull(task_ids='register_batch', key='return_value') or {} -%}
{% set dwd = reg.get('dwd_dates') or '' -%}
{% if dwd -%}
python -m ecommerce_dw.dwd.cli build --dates {{ dwd }}
{%- else -%}
echo "[build_dwd] no business dates to rebuild (register_batch planned none:"
echo "             stable state, watermark consumed / no ODS change in window)"
exit 0
{%- endif %}
"""

BUILD_DWS_CMD = r"""
{% set reg = ti.xcom_pull(task_ids='register_batch', key='return_value') or {} -%}
{% set agg = reg.get('agg_dates') or '' -%}
{% if agg -%}
python -m ecommerce_dw.dws.cli build --dates {{ agg }}
{%- else -%}
echo "[build_dws] no agg scope to rebuild (register_batch planned none)"
echo "             -> legitimate idempotent no-op"
exit 0
{%- endif %}
"""

# build_ads：ADS 输出由 dws build 同一命令产出（Phase 4 既定），本任务承担
# “ADS 构建后独立复核”（dws.cli verify 用独立 SQL 重算并 diff），是除门禁外
# 对 ADS 层的第二道硬闸。
BUILD_ADS_CMD = r"""
{% set reg = ti.xcom_pull(task_ids='register_batch', key='return_value') or {} -%}
{% set agg = reg.get('agg_dates') or '' -%}
{% if agg -%}
python -m ecommerce_dw.dws.cli verify --dates {{ agg }}
{%- else -%}
echo "[build_ads] no agg scope to verify (register_batch planned none)"
echo "             -> legitimate idempotent no-op"
exit 0
{%- endif %}
"""


# --------------------------------------------------------------------------- #
# DAG definition                                                               #
# --------------------------------------------------------------------------- #

default_args = {
    "owner": OWNER,
    "depends_on_past": False,
    "retries": 2,  # 瞬态故障兜底；确定性失败在 start/register/publish 上被显式关掉
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(hours=3),
}

dag = DAG(
    dag_id=DAG_ID,
    description="电商数仓日任务链：ODS -> DWD -> DWS/ADS -> 质量门禁 -> 受控发布",
    schedule="0 4 * * *",  # 每日 04:00 UTC
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["ecommerce", "daily", "phase6"],
    doc_md=__doc__,
    on_failure_callback=_dag_failure_callback,
)

start = PythonOperator(
    task_id="start",
    python_callable=_start,
    dag=dag,
    retries=0,
    execution_timeout=timedelta(minutes=5),
)

check_source_files = BashOperator(
    task_id="check_source_files",
    bash_command=CHECK_SOURCES_CMD,
    dag=dag,
    execution_timeout=timedelta(minutes=10),
)

register_batch = PythonOperator(
    task_id="register_batch",
    python_callable=_register_batch,
    dag=dag,
    retries=0,  # 确定性规划；失败=输入/环境问题，重试无意义
    execution_timeout=timedelta(minutes=15),
)

load_ods = BashOperator(
    task_id="load_ods",
    bash_command=LOAD_ODS_CMD,
    dag=dag,
    execution_timeout=timedelta(hours=2),  # 全量 20M 行 item_properties 装载需较长上限
)

validate_ods = BashOperator(
    task_id="validate_ods",
    bash_command=VALIDATE_ODS_CMD,
    dag=dag,
    execution_timeout=timedelta(minutes=15),
)

build_dwd = BashOperator(
    task_id="build_dwd",
    bash_command=BUILD_DWD_CMD,
    dag=dag,
    execution_timeout=timedelta(hours=1),
)

build_dws = BashOperator(
    task_id="build_dws",
    bash_command=BUILD_DWS_CMD,
    dag=dag,
    execution_timeout=timedelta(hours=1),
)

build_ads = BashOperator(
    task_id="build_ads",
    bash_command=BUILD_ADS_CMD,
    dag=dag,
    execution_timeout=timedelta(minutes=45),
)

quality_gate = PythonOperator(
    task_id="quality_gate",
    python_callable=_quality_gate,
    dag=dag,
    execution_timeout=timedelta(minutes=45),  # 60 条规则，最重四连全表分组约 3 分钟
)

publish_success = PythonOperator(
    task_id="publish_success",
    python_callable=_publish_success,
    dag=dag,
    retries=0,  # 发布是幂等审计事件；失败后人工复核比自动重试更安全
    execution_timeout=timedelta(minutes=15),
)

# 线性依赖（主方案 §7）：前序任一失败即停止，后经 on_failure_callback 关批次。
start >> check_source_files >> register_batch >> load_ods >> validate_ods
validate_ods >> build_dwd >> build_dws >> build_ads >> quality_gate >> publish_success
