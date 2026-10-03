"""Data quality checks for the silver layer. Raises DataQualityError on critical failures."""
import logging
from datetime import date

from pyspark.sql import DataFrame, Window, functions as F

log = logging.getLogger("quality")

EXPECTED_COLUMNS = {
    "ticker", "trade_date", "open", "high", "low", "close", "adj_close",
    "volume", "dividends", "stock_splits", "adj_factor", "source", "ingested_at",
}
MAX_GAP_DAYS = 7        # ช่องว่างระหว่างวันซื้อขายที่เกินนี้ถือว่าน่าสงสัย
MAX_STALE_DAYS = 5      # ข้อมูลล่าสุดเก่ากว่านี้ถือว่าไม่สด


class DataQualityError(Exception):
    pass


def run_silver_checks(df: DataFrame) -> None:
    failures, warnings = [], []

    # 1) schema
    missing = EXPECTED_COLUMNS - set(df.columns)
    if missing:
        raise DataQualityError(f"Missing columns: {sorted(missing)}")

    # 2) ตรวจกฎหลักในรอบเดียว
    agg = df.agg(
        F.count("*").alias("rows"),
        F.sum(F.when(F.col("ticker").isNull() | F.col("trade_date").isNull(), 1).otherwise(0)).alias("null_keys"),
        F.sum(F.when(F.col("close") <= 0, 1).otherwise(0)).alias("non_positive_price"),
        F.sum(F.when(F.col("high") < F.col("low"), 1).otherwise(0)).alias("high_lt_low"),
        F.sum(F.when(F.col("volume") < 0, 1).otherwise(0)).alias("neg_volume"),
    ).first()

    if agg["rows"] == 0:
        failures.append("silver is empty")
    for name in ["null_keys", "non_positive_price", "high_lt_low", "neg_volume"]:
        if agg[name] and agg[name] > 0:
            failures.append(f"{name}: {agg[name]} rows")

    # 3) ข้อมูลซ้ำตาม key
    dups = df.groupBy("ticker", "trade_date").count().filter("count > 1").count()
    if dups > 0:
        failures.append(f"duplicate (ticker, trade_date) keys: {dups}")

    # 4) ช่องว่างของวันที่ต่อ ticker (เทียบแต่ละวันกับวันก่อนหน้า จึงไม่ติดกับหุ้นที่เพิ่ง IPO)
    w = Window.partitionBy("ticker").orderBy("trade_date")
    gaps = (
        df.withColumn("gap", F.datediff("trade_date", F.lag("trade_date").over(w)))
          .groupBy("ticker").agg(F.max("gap").alias("max_gap"))
          .filter(F.col("max_gap") > MAX_GAP_DAYS)
          .collect()
    )
    for r in gaps:
        warnings.append(f"{r['ticker']}: max gap {r['max_gap']} days")

    # 5) ความสดของข้อมูล
    latest = df.agg(F.max("trade_date")).first()[0]
    if latest is not None and (date.today() - latest).days > MAX_STALE_DAYS:
        warnings.append(f"latest trade_date {latest} is stale")

    for w_msg in warnings:
        log.warning("DQ WARN: %s", w_msg)
    if failures:
        for f_msg in failures:
            log.error("DQ FAIL: %s", f_msg)
        raise DataQualityError("; ".join(failures))
    log.info("DQ passed: %d rows, %d warnings", agg["rows"], len(warnings))