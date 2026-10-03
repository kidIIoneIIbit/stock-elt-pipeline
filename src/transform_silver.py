"""Bronze -> Silver: type cast, dedupe, quarantine invalid rows, dividend-adjusted OHLC."""
import logging
import sys

from pyspark.sql import SparkSession, Window, functions as F

from quality_checks import DataQualityError, run_silver_checks

DATA_DIR = "/opt/airflow/data"
BRONZE = f"{DATA_DIR}/bronze/prices"
SILVER = f"{DATA_DIR}/silver/prices"
QUARANTINE = f"{DATA_DIR}/quarantine/prices"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("silver")


def get_spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[2]").appName("silver")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        # เขียนทับเฉพาะ partition ที่มีข้อมูลใหม่ -> รันซ้ำได้ ไม่ซ้ำซ้อน (idempotent)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def main() -> int:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    exit_code = 0
    try:
        bronze = spark.read.parquet(BRONZE)  # ได้คอลัมน์ ticker จาก partition อัตโนมัติ
        n_bronze = bronze.count()

        # 1) cast ชนิดข้อมูลให้ชัดเจน
        typed = (
            bronze
            .withColumn("trade_date", F.col("trade_date").cast("date"))
            .withColumn("ticker", F.upper(F.trim("ticker")))
            .withColumn("volume", F.col("volume").cast("long"))
            .withColumn("ingested_at", F.col("ingested_at").cast("timestamp"))
        )

        # 2) ลบข้อมูลซ้ำ: เก็บแถวที่ ingest ล่าสุดต่อ (ticker, trade_date)
        w = Window.partitionBy("ticker", "trade_date").orderBy(F.col("ingested_at").desc())
        deduped = (
            typed.withColumn("rn", F.row_number().over(w))
            .filter("rn = 1").drop("rn")
            .cache()
        )
        n_dedup = deduped.count()

        # 3) แยกแถวผิดปกติเข้า quarantine แทนการลบทิ้งเงียบๆ
        is_valid = (
            F.col("ticker").isNotNull() & F.col("trade_date").isNotNull()
            & (F.col("open") > 0) & (F.col("high") > 0) & (F.col("low") > 0)
            & (F.col("close") > 0) & (F.col("adj_close") > 0)
            & (F.col("high") >= F.col("low")) & (F.col("volume") >= 0)
        )
        valid = deduped.filter(is_valid)
        invalid = deduped.filter(~is_valid)
        n_invalid = invalid.count()
        if n_invalid > 0:
            invalid.write.mode("overwrite").parquet(QUARANTINE)
            log.warning("Quarantined %d invalid rows -> %s", n_invalid, QUARANTINE)

        # 4) ราคาที่ปรับ dividend: Close จาก Yahoo ปรับ split แล้ว, adj_close ปรับ dividend เพิ่ม
        silver = (
            valid
            .withColumn("adj_factor", F.col("adj_close") / F.col("close"))
            .withColumn("adj_open", F.col("open") * F.col("adj_factor"))
            .withColumn("adj_high", F.col("high") * F.col("adj_factor"))
            .withColumn("adj_low", F.col("low") * F.col("adj_factor"))
            .withColumn("year", F.year("trade_date"))
            .select(
                "ticker", "trade_date", "open", "high", "low", "close", "adj_close",
                "adj_open", "adj_high", "adj_low", "adj_factor", "volume",
                "dividends", "stock_splits", "source", "ingested_at", "year",
            )
        )

        # repartition ตาม key เดียวกับ partitionBy -> ได้ 1 ไฟล์ต่อโฟลเดอร์
        (silver.repartition("ticker", "year")
               .write.mode("overwrite").partitionBy("ticker", "year").parquet(SILVER))
        deduped.unpersist()

        # 5) ตรวจคุณภาพจากข้อมูลที่เขียนจริง
        result = spark.read.parquet(SILVER)
        log.info("bronze=%d, after_dedupe=%d, invalid=%d, silver=%d",
                 n_bronze, n_dedup, n_invalid, result.count())
        run_silver_checks(result)
    except DataQualityError as exc:
        log.error("Quality checks failed: %s", exc)
        exit_code = 1
    finally:
        spark.stop()
    return exit_code


if __name__ == "__main__":
    sys.exit(main())