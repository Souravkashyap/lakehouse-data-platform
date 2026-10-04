"""Daily partner-file DAG. Partners send a FULL snapshot each day (~500M rows, ~95% unchanged).

Per vendor: COPY the full file and its control trailer into Bronze, check the truncation guard, run
`dbt snapshot` (SCD2 change detection on a row hash), then prune Bronze to the last two files.
The finance build waits for every vendor AND for the internal completeness gate (ops.completeness),
so reconciliation never runs on a partial day. Internal data is loaded by lakehouse_15min.
See docs/design.md sections 5, 6 and 9.
"""
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.task_group import TaskGroup

VENDORS = [("lending", "nbfc_017")]  # (business unit, vendor); add the rest the same way
DBT = "dbt {cmd} --project-dir /usr/local/airflow/dbt"
RUN_DATE = """ --vars '{run_date: "{{ ds }}"}'"""  # logical date = business day D; never the wall clock


def copy_full_file_sql(unit: str, vendor: str) -> str:
    # CSV columns are positional; metadata comes from COPY, and snapshot_date from the run's logical date.
    return f"""
        COPY INTO bronze_partner.{unit}_{vendor}
        FROM (
            SELECT t.$1, t.$2, t.$3, t.$4, '{{{{ ds }}}}', t.$5, t.$6, t.$7, t.$8,
                   METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, current_timestamp(), current_timestamp()
            FROM @partner_stage/{unit}/{vendor}/snapshot_date={{{{ ds }}}}/ AS t
        )
        PATTERN = '.*data_.*[.]csv[.]gz'
        FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' COMPRESSION = GZIP);

        COPY INTO bronze_partner.{unit}_{vendor}_control (vendor, snapshot_date, trailer_rows, trailer_paise)
        FROM (SELECT t.$1, t.$2, t.$3, t.$4 FROM @partner_stage/{unit}/{vendor}/snapshot_date={{{{ ds }}}}/ AS t)
        PATTERN = '.*_control[.]csv'
        FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);
    """


with DAG(
    dag_id="lakehouse_partner_daily",
    schedule="15 2 * * *",  # partner files are due by 02:00 IST
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["lakehouse", "partner"],
) as dag:
    internal_gate = SqlSensor(
        task_id="internal_day_complete",
        conn_id="snowflake_default",
        sql="select count(*) from ops.completeness where business_date = '{{ ds }}' and scope = 'INTERNAL' and is_complete",
        mode="reschedule",
        poke_interval=300,
        timeout=4 * 3600,  # escalation happens at 04:30 if the gate is still closed (design §6)
    )
    finance = BashOperator(
        task_id="dbt_build_finance",
        bash_command=DBT.format(cmd="build") + " --select tag:finance" + RUN_DATE,
    )

    for unit, vendor in VENDORS:
        with TaskGroup(group_id=f"{unit}_{vendor}") as group:
            copy = SQLExecuteQueryOperator(
                task_id="copy_full_file", conn_id="snowflake_default",
                sql=copy_full_file_sql(unit, vendor), split_statements=True,
            )
            # pages vendor ops if the newest file is not approved; the snapshot reads only approved files anyway
            guard = BashOperator(
                task_id="truncation_guard",
                bash_command=DBT.format(cmd="build") + " --select partner_snapshot_approval assert_partner_snapshot_not_truncated",
            )
            snapshot = BashOperator(
                task_id="dbt_snapshot",
                bash_command=DBT.format(cmd="snapshot") + " --select snap_partner_records",
            )
            prune = SQLExecuteQueryOperator(
                task_id="prune_bronze", conn_id="snowflake_default",
                sql=f"delete from bronze_partner.{unit}_{vendor} "
                    "where to_date(snapshot_date) < dateadd(day, -1, '{{ ds }}'::date)",  # keep the last two files
            )
            copy >> guard >> snapshot >> prune
        group >> finance

    internal_gate >> finance
