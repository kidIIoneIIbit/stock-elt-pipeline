CREATE TABLE IF NOT EXISTS daily_features (
    ticker                   TEXT             NOT NULL,
    trade_date               DATE             NOT NULL,
    close                    DOUBLE PRECISION,
    adj_close                DOUBLE PRECISION,
    volume                   BIGINT,
    daily_return             DOUBLE PRECISION,
    log_return               DOUBLE PRECISION,
    ma_20                    DOUBLE PRECISION,
    ma_50                    DOUBLE PRECISION,
    ma_200                   DOUBLE PRECISION,
    volatility_20d           DOUBLE PRECISION,
    rsi_14                   DOUBLE PRECISION,
    high_252d                DOUBLE PRECISION,
    low_252d                 DOUBLE PRECISION,
    drawdown_from_252d_high  DOUBLE PRECISION,
    PRIMARY KEY (ticker, trade_date)
);

CREATE TABLE IF NOT EXISTS monthly_summary (
    ticker               TEXT             NOT NULL,
    month                DATE             NOT NULL,
    month_end_adj_close  DOUBLE PRECISION,
    total_volume         BIGINT,
    avg_daily_volume     DOUBLE PRECISION,
    total_dividends      DOUBLE PRECISION,
    trading_days         BIGINT,
    monthly_return       DOUBLE PRECISION,
    PRIMARY KEY (ticker, month)
);