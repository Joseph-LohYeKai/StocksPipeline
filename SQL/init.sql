CREATE DATABASE airflow;




CREATE TABLE IF NOT EXISTS stock_ticks_stream (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(10),
    timestamp TIMESTAMPTZ NOT NULL,
    price NUMERIC(10, 4)
);

CREATE INDEX IF NOT EXISTS idx_symbol_time 
ON stock_ticks_stream(symbol, timestamp);

CREATE TABLE IF NOT EXISTS stock_eod_summary (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(10),
    date DATE NOT NULL,
    open NUMERIC(10, 4),
    high NUMERIC(10, 4),
    low NUMERIC(10, 4), 
    close NUMERIC(10, 4),
    UNIQUE(symbol, date)
);
CREATE INDEX IF NOT EXISTS idx_eod_symbol_date 
ON stock_eod_summary(symbol, date);
