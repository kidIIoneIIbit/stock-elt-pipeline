"""Silver -> Gold: daily technical features and monthly summary."""
import logging
import sys

from pyspark.sql import SparkSession, Window, functions as F

DATA_DIR = "/opt/airflow/data"
SILVER = f"{DATA_DIR}/silver/prices"
GOLD_DAILY = f"{DATA_DIR}/gold/daily_features"
GOLD_MONTHLY = f"{DATA_DIR}/gold/monthly_summary"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("gold")


def get_spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[2]").appName("gold")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def rolling(col, n, fn):
    """Rolling window ต่อ ticker; คืน null ถ้ายังมีข้อมูลไม่ครบ n แถว (ไม่แสดงค่าที่หลอกตา)."""
    w = Window.partitionBy("ticker").orderBy("trade_date").rowsBetween(-(n - 1), 0)
    return F.when(F.count(col).over(w) >= n, fn(F.col(col)).over(w))


def main() -> int:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        s = spark.read.parquet(SILVER)
        order = Window.partitionBy("ticker").orderBy("trade_date")

        # --- Daily features (คำนวณจาก adj_close เพราะรวม dividend แล้ว) ---
        d = (
            s.withColumn("prev_adj_close", F.lag("adj_close").over(order))
             .withColumn("daily_return", F.col("adj_close") / F.col("prev_adj_close") - 1)
             .withColumn("log_return", F.log(F.col("adj_close") / F.col("prev_adj_close")))
             .withColumn("change", F.col("adj_close") - F.col("prev_adj_close"))
             # แถวแรกของแต่ละหุ้นไม่มี change -> ให้ gain/loss เป็น null (ไม่นับเป็น 0)
             .withColumn("gain", F.when(F.col("change").isNotNull(),
                                        F.greatest(F.col("change"), F.lit(0.0))))
             .withColumn("loss", F.when(F.col("change").isNotNull(),
                                        F.greatest(-F.col("change"), F.lit(0.0))))
        )

        d = (
            d.withColumn("ma_20", rolling("adj_close", 20, F.avg))
             .withColumn("ma_50", rolling("adj_close", 50, F.avg))
             .withColumn("ma_200", rolling("adj_close", 200, F.avg))
             .withColumn("volatility_20d",
                         rolling("daily_return", 20, F.stddev) * F.sqrt(F.lit(252.0)))
             .withColumn("high_252d", rolling("adj_close", 252, F.max))
             .withColumn("low_252d", rolling("adj_close", 252, F.min))
             .withColumn("avg_gain_14", rolling("gain", 14, F.avg))
             .withColumn("avg_loss_14", rolling("loss", 14, F.avg))
        )

        # RSI แบบ simple average (Cutler's RSI) ไม่ใช่ Wilder smoothing
        d = (
            d.withColumn(
                "rsi_14",
                F.when(F.col("avg_loss_14") == 0, F.lit(100.0))
                 .when(F.col("avg_loss_14").isNotNull(),
                       100 - 100 / (1 + F.col("avg_gain_14") / F.col("avg_loss_14"))),
            )
            .withColumn("drawdown_from_252d_high", F.col("adj_close") / F.col("high_252d") - 1)
        )

        daily = d.select(
            "ticker", "trade_date", "year", "close", "adj_close", "volume",
            "daily_return", "log_return", "ma_20", "ma_50", "ma_200",
            "volatility_20d", "rsi_14", "high_252d", "low_252d", "drawdown_from_252d_high",
        )
        (daily.repartition("ticker", "year")
              .write.mode("overwrite").partitionBy("ticker", "year").parquet(GOLD_DAILY))

        # --- Monthly summary ---
        m = (
            s.withColumn("month", F.date_trunc("month", "trade_date").cast("date"))
             .groupBy("ticker", "month")
             .agg(
                 F.max_by("adj_close", "trade_date").alias("month_end_adj_close"),
                 F.sum("volume").alias("total_volume"),
                 F.avg("volume").alias("avg_daily_volume"),
                 F.sum("dividends").alias("total_dividends"),
                 F.count("*").alias("trading_days"),
             )
        )
        mw = Window.partitionBy("ticker").orderBy("month")
        m = m.withColumn(
            "monthly_return",
            F.col("month_end_adj_close") / F.lag("month_end_adj_close").over(mw) - 1,
        )
        (m.repartition("ticker")
          .write.mode("overwrite").partitionBy("ticker").parquet(GOLD_MONTHLY))

        log.info("gold daily=%d rows, monthly=%d rows",
                 spark.read.parquet(GOLD_DAILY).count(),
                 spark.read.parquet(GOLD_MONTHLY).count())
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())