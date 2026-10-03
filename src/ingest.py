"""Ingest daily OHLCV for a watchlist -> data/raw (CSV) and data/bronze (Parquet)."""
import argparse
import logging
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf

# ปรับ watchlist ได้ตามต้องการ (เน้น semiconductor / AI / autonomous driving)
WATCHLIST = [
    "NVDA", "TSM", "AMD", "ASML", "INTC", "AVGO", "QCOM", "MU", "AMAT", "LRCX",
    "KLAC", "ARM", "MRVL", "TXN", "TSLA", "MBLY", "GOOGL", "MSFT", "META",
    "AMZN", "AAPL", "PLTR", "ON", "NXPI", "SMCI",
]

DATA_DIR = Path("/opt/airflow/data")
SOURCE = "yfinance"
MAX_RETRIES = 3

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ingest")


def fetch_ticker(ticker: str, start: str, end: str) -> pd.DataFrame:
    """ดึงข้อมูลราย ticker พร้อม retry; auto_adjust=False เพื่อเก็บ Close จริง + Adj Close."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            df = yf.Ticker(ticker).history(
                start=start, end=end, auto_adjust=False, actions=True
            )
            if df.empty:
                raise ValueError("empty dataframe")
            return df
        except Exception as exc:  # noqa: BLE001
            log.warning("%s attempt %d/%d failed: %s", ticker, attempt, MAX_RETRIES, exc)
            time.sleep(2 * attempt)
    raise RuntimeError(f"{ticker}: failed after {MAX_RETRIES} attempts")


def to_bronze_frame(df: pd.DataFrame, ticker: str, ingested_at: datetime) -> pd.DataFrame:
    out = df.reset_index()
    out.columns = [c.strip().lower().replace(" ", "_") for c in out.columns]
    out = out.rename(columns={"date": "trade_date"})
    out["trade_date"] = pd.to_datetime(out["trade_date"]).dt.tz_localize(None).dt.date
    out["ticker"] = ticker
    out["source"] = SOURCE
    out["ingested_at"] = ingested_at
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="2016-01-01")
    parser.add_argument("--end", default=date.today().isoformat())
    parser.add_argument("--tickers", nargs="*", default=WATCHLIST)
    args = parser.parse_args()

    ingested_at = datetime.now(timezone.utc)
    raw_dir = DATA_DIR / "raw" / f"ingest_date={date.today().isoformat()}"
    raw_dir.mkdir(parents=True, exist_ok=True)

    failed = []
    for ticker in args.tickers:
        try:
            df = fetch_ticker(ticker, args.start, args.end)

            # 1) raw: เก็บตามที่ได้มา ไม่แก้ไข เผื่อต้องดีบักย้อนหลัง
            df.to_csv(raw_dir / f"{ticker}.csv")

            # 2) bronze: Parquet + metadata แยก partition ตาม ticker
            bronze = to_bronze_frame(df, ticker, ingested_at)
            out_dir = DATA_DIR / "bronze" / "prices" / f"ticker={ticker}"
            out_dir.mkdir(parents=True, exist_ok=True)
            bronze.drop(columns=["ticker"]).to_parquet(out_dir / "data.parquet", index=False)

            log.info("%s OK: %d rows (%s -> %s)", ticker, len(bronze),
                     bronze["trade_date"].min(), bronze["trade_date"].max())
        except Exception as exc:  # noqa: BLE001
            log.error("%s FAILED: %s", ticker, exc)
            failed.append(ticker)

    if failed:
        log.error("Failed tickers: %s", failed)
        return 1
    log.info("All %d tickers ingested", len(args.tickers))
    return 0


if __name__ == "__main__":
    sys.exit(main())