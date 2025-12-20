


CREATE TABLE ohlcv (
    symbol TEXT NOT NULL,                -- BTCUSDT
    interval TEXT NOT NULL,              -- 1m, 5m, 1h, 1d

    open_time TIMESTAMPTZ NOT NULL,
    close_time TIMESTAMPTZ NOT NULL,

    open  NUMERIC(18,8) NOT NULL,
    high  NUMERIC(18,8) NOT NULL,
    low   NUMERIC(18,8) NOT NULL,
    close NUMERIC(18,8) NOT NULL,

    volume NUMERIC(28,10) NOT NULL,       -- base asset volume
    quote_volume NUMERIC(28,10) NOT NULL, -- quote asset volume

    trades_count INTEGER NOT NULL,

    taker_buy_base_volume  NUMERIC(28,10) NOT NULL,
    taker_buy_quote_volume NUMERIC(28,10) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (symbol, interval, open_time)
);







CREATE TABLE rd_ohlcv (
    symbol TEXT NOT NULL,                -- BTCUSDT
    interval TEXT NOT NULL,              -- 1m, 5m, 1h, 1d

    open_time TIMESTAMPTZ NOT NULL,
    close_time TIMESTAMPTZ NOT NULL,

    open  NUMERIC(18,8) NOT NULL,
    high  NUMERIC(18,8) NOT NULL,
    low   NUMERIC(18,8) NOT NULL,
    close NUMERIC(18,8) NOT NULL,

    volume NUMERIC(28,10) NOT NULL,       -- base asset volume
    quote_volume NUMERIC(28,10) NOT NULL, -- quote asset volume

    trades_count INTEGER NOT NULL,

    taker_buy_base_volume  NUMERIC(28,10) NOT NULL,
    taker_buy_quote_volume NUMERIC(28,10) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);


CREATE TABLE crypto_assets (
    symbol TEXT PRIMARY KEY,      -- BTCUSDT, ETHUSDT
    base_asset TEXT NOT NULL,     -- BTC, ETH
    is_active BOOLEAN NOT NULL DEFAULT TRUE
);



INSERT INTO crypto_assets (symbol, base_asset, is_active)
VALUES
('BTCUSDT', 'BTC', true),
('ETHUSDT', 'ETH', true),
('SOLUSDT', 'SOL', true);