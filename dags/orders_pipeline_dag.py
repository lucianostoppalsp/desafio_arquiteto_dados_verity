from __future__ import annotations

import subprocess
from datetime import datetime, timedelta

import psycopg2
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context

DB_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "dbname": "challenge_db",
    "user": "airflow",
    "password": "airflow",
}


def _run_job(script_name: str, window: dict) -> None:
    cmd = [
        "python",
        f"/opt/airflow/src/jobs/{script_name}",
        "--reference-date",
        window["reference_date"],
        "--lookback-days",
        str(window["lookback_days"]),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print(result.stderr)
    if result.returncode != 0:
        raise AirflowFailException(f"Job {script_name} failed with return code {result.returncode}")


@dag(
    dag_id="orders_pipeline_dag",
    description="Pipeline local com Airflow + PySpark para processamento incremental de orders_raw",
    start_date=datetime(2026, 3, 1),
    schedule=None,
    catchup=False,
    tags=["orders", "pyspark", "challenge"],
    default_args={"owner": "Stoppa"},
)
def orders_pipeline_dag():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task(task_id="compute_window")
    def compute_window() -> dict:
        context = get_current_context()
        dag_run = context.get("dag_run")
        conf = dag_run.conf if dag_run else {}

        reference_date = conf.get("reference_date") or context["ds"]
        lookback_days = int(conf.get("lookback_days", 7))

        end_date = datetime.strptime(reference_date, "%Y-%m-%d").date()
        start_date = end_date - timedelta(days=lookback_days)

        window = {
            "reference_date": reference_date,
            "lookback_days": lookback_days,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }
        print(f"Processing window: {window}")
        return window

    @task(task_id="prepare_target_window")
    def prepare_target_window(window: dict) -> dict:
        delete_statements = [
            "DELETE FROM analytics.orders_daily_summary WHERE business_date BETWEEN %s AND %s",
            "DELETE FROM analytics.customer_orders_daily WHERE business_date BETWEEN %s AND %s",
            "DELETE FROM refined.orders_events_dedup WHERE business_date BETWEEN %s AND %s",
        ]

        conn = psycopg2.connect(**DB_CONFIG)
        try:
            with conn:
                with conn.cursor() as cur:
                    for sql in delete_statements:
                        cur.execute(sql, (window["start_date"], window["end_date"]))
            print(
                "Deleted target rows for window "
                f"[{window['start_date']}, {window['end_date']}]"
            )
        finally:
            conn.close()

        return window

    @task(task_id="build_orders_events_dedup")
    def build_orders_events_dedup(window: dict) -> dict:
        _run_job("build_orders_events_dedup.py", window)
        return window

    @task(task_id="build_customer_orders_daily")
    def build_customer_orders_daily(window: dict) -> dict:
        _run_job("build_customer_orders_daily.py", window)
        return window

    @task(task_id="build_orders_daily_summary")
    def build_orders_daily_summary(window: dict) -> dict:
        _run_job("build_orders_daily_summary.py", window)
        return window

    @task(task_id="validate_outputs")
    def validate_outputs(window: dict) -> None:
        queries = {
            "raw_count": "SELECT COUNT(*) FROM raw.orders_raw WHERE business_date BETWEEN %s AND %s",
            "dedup_count": "SELECT COUNT(*) FROM refined.orders_events_dedup WHERE business_date BETWEEN %s AND %s",
            "customer_daily_count": "SELECT COUNT(*) FROM analytics.customer_orders_daily WHERE business_date BETWEEN %s AND %s",
            "daily_summary_count": "SELECT COUNT(*) FROM analytics.orders_daily_summary WHERE business_date BETWEEN %s AND %s",
        }

        counts = {}
        conn = psycopg2.connect(**DB_CONFIG)
        try:
            with conn.cursor() as cur:
                for key, sql in queries.items():
                    cur.execute(sql, (window["start_date"], window["end_date"]))
                    counts[key] = cur.fetchone()[0]
        finally:
            conn.close()

        print(f"Validation counts: {counts}")

        if counts["raw_count"] > 0 and counts["dedup_count"] == 0:
            raise AirflowFailException("Source window has rows, but refined.orders_events_dedup is empty.")
        if counts["dedup_count"] > 0 and counts["customer_daily_count"] == 0:
            raise AirflowFailException("Refined window has rows, but analytics.customer_orders_daily is empty.")
        if counts["customer_daily_count"] > 0 and counts["daily_summary_count"] == 0:
            raise AirflowFailException("Customer daily window has rows, but analytics.orders_daily_summary is empty.")

    window = compute_window()
    prepared_window = prepare_target_window(window)
    dedup_done = build_orders_events_dedup(prepared_window)
    customer_daily_done = build_customer_orders_daily(dedup_done)
    summary_done = build_orders_daily_summary(customer_daily_done)
    validated = validate_outputs(summary_done)

    start >> window >> prepared_window >> dedup_done >> customer_daily_done >> summary_done >> validated >> end


orders_pipeline_dag()
