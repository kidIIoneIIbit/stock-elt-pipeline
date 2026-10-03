"""Daily stock ELT: yfinance -> bronze -> silver -> gold -> PostgreSQL."""
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

SRC = "/opt/airflow/src"

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="stock_elt_daily",
    description="Daily US tech/semiconductor prices: bronze -> silver -> gold -> Postgres",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    # 22:00 UTC วันจันทร์-ศุกร์ = หลังตลาดสหรัฐฯ ปิด (20:00 หรือ 21:00 UTC ตามฤดูกาล)
    schedule="0 22 * * 1-5",
    catchup=False,
    max_active_runs=1,
    tags=["stocks", "elt"],
) as dag:

    ingest = BashOperator(
        task_id="ingest_bronze",
        bash_command=f"python {SRC}/ingest.py",
    )

    silver = BashOperator(
        task_id="transform_silver",
        bash_command=f"python {SRC}/transform_silver.py",
        retries=1,
    )

    gold = BashOperator(
        task_id="transform_gold",
        bash_command=f"python {SRC}/transform_gold.py",
        retries=1,
    )

    load = BashOperator(
        task_id="load_postgres",
        bash_command=f"python {SRC}/load_postgres.py",
    )

    ingest >> silver >> gold >> load