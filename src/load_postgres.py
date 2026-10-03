"""Gold (Parquet) -> PostgreSQL (stocks-db) with idempotent upsert."""
import io
import logging
import os
import sys
from pathlib import Path

import pandas as pd
import psycopg2

DATA_DIR = Path("/opt/airflow/data")
SQL_FILE = Path("/opt/airflow/sql/create_tables.sql")

TABLES = {
    "daily_features": (DATA_DIR / "gold" / "daily_features", ["ticker", "trade_date"]),
    "monthly_summary": (DATA_DIR / "gold" / "monthly_summary", ["ticker", "month"]),
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("load")


def connect():
    return psycopg2.connect(
        host=os.environ["STOCKS_DB_HOST"],
        port=os.environ["STOCKS_DB_PORT"],
        dbname=os.environ["STOCKS_DB_NAME"],
        user=os.environ["STOCKS_DB_USER"],
        password=os.environ["STOCKS_DB_PASSWORD"],
    )


def load_table(cur, name: str, path: Path, key: list) -> int:
    df = pd.read_parquet(path)
    df["ticker"] = df["ticker"].astype(str)  # partition column มาเป็น category
    df = df.drop(columns=[c for c in ["year"] if c in df.columns])
    cols = list(df.columns)

    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="")  # NaN -> ค่าว่าง -> NULL
    buf.seek(0)

    # staging table ชั่วคราว (ชื่อแยกตามตาราง เพราะอยู่ใน transaction เดียวกัน)
    # แล้ว upsert เข้าตารางจริง -> รันซ้ำกี่รอบข้อมูลก็ไม่ซ้ำ
    stg = f"stg_{name}"
    cur.execute(f"CREATE TEMP TABLE {stg} (LIKE {name}) ON COMMIT DROP")
    cur.copy_expert(
        f"COPY {stg} ({', '.join(cols)}) FROM STDIN WITH (FORMAT csv, NULL '')", buf
    )
    col_list = ", ".join(cols)
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c not in key)
    cur.execute(
        f"INSERT INTO {name} ({col_list}) SELECT {col_list} FROM {stg} "
        f"ON CONFLICT ({', '.join(key)}) DO UPDATE SET {updates}"
    )
    return len(df)


def main() -> int:
    conn = connect()
    try:
        with conn:  # transaction เดียว: สำเร็จทั้งหมดหรือยกเลิกทั้งหมด
            with conn.cursor() as cur:
                cur.execute(SQL_FILE.read_text())
                for name, (path, key) in TABLES.items():
                    n = load_table(cur, name, path, key)
                    cur.execute(f"SELECT count(*) FROM {name}")
                    log.info("%s: loaded %d rows, table now has %d rows",
                             name, n, cur.fetchone()[0])
        return 0
    except Exception:
        log.exception("Load failed; transaction rolled back")
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())