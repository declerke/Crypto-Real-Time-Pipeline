ALTER USER cryptouser REPLICATION;

CREATE TABLE crypto_ticker_24h (
    symbol               VARCHAR(20)  PRIMARY KEY,
    price_usdt           NUMERIC(24, 8),
    price_change_percent NUMERIC(12, 4),
    volume_usdt          NUMERIC(30, 4),
    high_price_24h       NUMERIC(24, 8),
    low_price_24h        NUMERIC(24, 8),
    last_update          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_change_percent     ON crypto_ticker_24h (price_change_percent DESC);
CREATE INDEX idx_volume_usdt        ON crypto_ticker_24h (volume_usdt DESC);
CREATE INDEX idx_last_update        ON crypto_ticker_24h (last_update DESC);

CREATE TABLE ticker_history_pg (
    id                   BIGSERIAL    PRIMARY KEY,
    symbol               VARCHAR(20)  NOT NULL,
    price_usdt           NUMERIC(24, 8),
    price_change_percent NUMERIC(12, 4),
    volume_usdt          NUMERIC(30, 4),
    high_price_24h       NUMERIC(24, 8),
    low_price_24h        NUMERIC(24, 8),
    captured_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_history_symbol_time ON ticker_history_pg (symbol, captured_at DESC);
CREATE INDEX idx_history_time        ON ticker_history_pg (captured_at DESC);

CREATE PUBLICATION dbz_publication FOR TABLE crypto_ticker_24h;