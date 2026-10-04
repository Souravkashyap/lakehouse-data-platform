"""Daily DAG for the two external sources: a payment-gateway settlements API and the Ops fee-rules spreadsheet.

API: every raw page is stored untouched in S3 under a deterministic key (a rerun overwrites the same pages).
Sheet: every pull is kept as a full CSV snapshot, so who-changed-what is history. Then COPY INTO Bronze and dbt.
See docs/design.md section 5.
"""
import csv
import io
import json
import time
from datetime import timedelta

import pendulum
import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.google.suite.hooks.sheets import GSheetsHook

BUCKET = "lakehouse-raw"
API_URL = "https://api.gateway.example/v1/settlements"
DBT_BUILD = "dbt build --project-dir /usr/local/airflow/dbt --select {models}"  # each branch builds only its own models


def notify_on_call(context):
    """Stub: page the on-call engineer (PagerDuty / Slack) with the failed task and run id."""


def get_with_retry(url, headers, params, max_attempts=6):
    """GET with a timeout. 429: sleep for Retry-After. 5xx: exponential backoff. Anything else raises."""
    for attempt in range(max_attempts):
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        if resp.status_code == 429:
            time.sleep(int(resp.headers.get("Retry-After", "5")))
        elif resp.status_code >= 500:
            time.sleep(2 ** attempt)
        else:
            resp.raise_for_status()
            return resp
    raise RuntimeError(f"gateway API still failing after {max_attempts} attempts: {url}")


def pull_gateway_settlements(ds, **_):
    token = BaseHook.get_connection("gateway_api").password
    headers = {"Authorization": f"Bearer {token}"}
    s3 = S3Hook(aws_conn_id="aws_default")
    cursor, page = None, 1
    while True:
        params = {"date": ds, "limit": 500}
        if cursor:
            params["cursor"] = cursor
        body = get_with_retry(API_URL, headers, params).text    # raw page, stored exactly as received
        s3.load_string(
            body,
            key=f"raw/api/gateway_settlements/ingest_date={ds}/page_{page:05d}.json",
            bucket_name=BUCKET,
            replace=True,        # deterministic key + replace: a rerun overwrites the same pages
        )
        cursor = json.loads(body).get("next_cursor")
        if not cursor:
            break
        page += 1


def export_fee_rules_sheet(ts_nodash, **_):
    rows = GSheetsHook(gcp_conn_id="google_sheets").get_values(
        spreadsheet_id=Variable.get("fee_rules_sheet_id"), range_="fee_rules!A1:F"
    )
    buf = io.StringIO()
    # the API omits trailing empty cells, so pad every row to the 6 columns; blank cells load as NULL
    csv.writer(buf).writerows(row + [""] * (6 - len(row)) for row in rows)   # the FULL sheet, header included
    S3Hook(aws_conn_id="aws_default").load_string(
        buf.getvalue(),
        key=f"raw/sheets/fee_rules/snapshot_ts={ts_nodash}/fee_rules.csv",
        bucket_name=BUCKET,
        replace=True,
    )


COPY_API = """
    COPY INTO bronze_api.gateway_settlements (raw, _file_name, _loaded_at)
    FROM (
        SELECT $1, METADATA$FILENAME, current_timestamp()
        FROM @raw_stage/api/gateway_settlements/ingest_date={{ ds }}/
    )
    FILE_FORMAT = (TYPE = JSON STRIP_OUTER_ARRAY = FALSE)
    PATTERN = '.*page_.*[.]json'
"""

# snapshot_ts comes from the folder name; the header row is skipped
COPY_SHEET = """
    COPY INTO bronze_sheets.fee_rules
        (product_code, fee_type, fee_bps, flat_fee_inr, effective_from, effective_to, snapshot_ts, _file_name, _loaded_at)
    FROM (
        SELECT $1, $2, $3, $4, $5, $6,
               regexp_substr(METADATA$FILENAME, 'snapshot_ts=([0-9T]+)', 1, 1, 'e', 1),
               METADATA$FILENAME, current_timestamp()
        FROM @raw_stage/sheets/fee_rules/snapshot_ts={{ ts_nodash }}/
    )
    FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
"""

with DAG(
    dag_id="lakehouse_external_daily",
    schedule="30 1 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": notify_on_call,
    },
    tags=["lakehouse", "external"],
) as dag:
    pull = PythonOperator(task_id="pull_gateway_settlements", python_callable=pull_gateway_settlements)
    copy_api = SQLExecuteQueryOperator(task_id="copy_gateway_settlements", conn_id="snowflake_default", sql=COPY_API)
    dbt_api = BashOperator(
        task_id="dbt_build_api",
        bash_command=DBT_BUILD.format(models="stg_api_gateway_settlements silver_gateway_settlements"),
    )
    pull >> copy_api >> dbt_api

    export = PythonOperator(task_id="export_fee_rules_sheet", python_callable=export_fee_rules_sheet)
    copy_sheet = SQLExecuteQueryOperator(task_id="copy_fee_rules", conn_id="snowflake_default", sql=COPY_SHEET)
    dbt_sheet = BashOperator(  # a failed fee-rules test pages the sheet owner; ref_fee_rules keeps the last good version
        task_id="dbt_build_sheet",
        bash_command=DBT_BUILD.format(models="ref_fee_rules assert_fee_rules_latest_snapshot_valid"),
    )
    export >> copy_sheet >> dbt_sheet
