"""Every-15-minute lakehouse DAG (Airflow 2.x on MWAA).

Per business unit: COPY new Parquet files from the S3 stage into Bronze, then run that unit's Silver dbt models.
Then the shared Silver models (money movements, partner records), then the push of changed app profiles.
Source freshness runs in parallel as the silent-stall alarm. See docs/design.md sections 5-7.
The daily partner files and the finance build live in lakehouse_partner_daily.py, gated on completeness.
"""
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup

UNITS = ["lending", "insurance", "recharge", "customer"]
DBT = "dbt {cmd} --project-dir /usr/local/airflow/dbt"
# cautious: run a test only when ALL its parents are in this build, so the daily reconciliation
# balance test (which also reads finance Gold) is not pulled into the 15-minute shared build
CAUTIOUS = " --indirect-selection cautious"


def notify_on_call(context):
    """Stub: page the on-call engineer (PagerDuty / Slack) with the failed task and run id."""


def push_app_profiles(**_):
    """Changed rows of gold_app.app_customer_profile -> S3 unload -> DynamoDB upsert.

    app_lending_customer_summary (loans, next EMI due) feeds that profile."""


def copy_sql(unit: str) -> str:
    # COPY skips files it has already loaded, so scanning today and yesterday is safe.
    return f"""
        COPY INTO bronze.{unit}_events
        FROM (
            SELECT $1:topic::string, $1:kafka_partition::number, $1:kafka_offset::number, $1:kafka_ts::timestamp_ntz,
                   $1:event_id::string, $1:event_type::string, $1:aggregate_id::string, $1:sequence::number,
                   $1:occurred_at::timestamp_tz, $1:payload::variant,
                   METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, current_timestamp()
            FROM @bronze_stage/{unit}/
        )
        FILE_FORMAT = (TYPE = PARQUET)
        PATTERN = '.*ingest_date=({{{{ ds }}}}|{{{{ macros.ds_add(ds, -1) }}}})/.*[.]parquet'
    """


with DAG(
    dag_id="lakehouse_15min",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": notify_on_call,
    },
    tags=["lakehouse"],
) as dag:
    # Alternative: astronomer-cosmos can render each dbt model as its own Airflow task.
    freshness = BashOperator(task_id="dbt_source_freshness", bash_command=DBT.format(cmd="source freshness"))

    groups = []
    for unit in UNITS:
        with TaskGroup(group_id=unit) as group:
            copy = SQLExecuteQueryOperator(
                task_id=f"copy_{unit}", conn_id="snowflake_default", sql=copy_sql(unit)
            )
            silver = BashOperator(
                task_id=f"dbt_silver_{unit}",
                bash_command=DBT.format(cmd="build") + CAUTIOUS + f""" --select tag:{unit} --vars '{{run_date: "{{{{ ds }}}}"}}'""",
            )
            copy >> silver
        groups.append(group)

    shared = BashOperator(
        task_id="dbt_silver_shared",
        bash_command=DBT.format(cmd="build") + CAUTIOUS + """ --select tag:shared --vars '{run_date: "{{ ds }}"}'""",
    )
    app = BashOperator(task_id="dbt_gold_app", bash_command=DBT.format(cmd="build") + CAUTIOUS + " --select tag:app")
    push = PythonOperator(task_id="push_app_profiles", python_callable=push_app_profiles)

    groups >> shared >> app >> push
