CREATE TABLE IF NOT EXISTS products (
    product_id    VARCHAR(128) PRIMARY KEY,
    product_name  TEXT NOT NULL,
    category      VARCHAR(64),
    created_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS current_prices (
    id            SERIAL PRIMARY KEY,
    product_id    VARCHAR(128) REFERENCES products(product_id),
    source        VARCHAR(32) NOT NULL,
    price         NUMERIC(10, 2) NOT NULL,
    currency      CHAR(3) DEFAULT 'EUR',
    url           TEXT,
    scraped_at    TIMESTAMP NOT NULL,
    updated_at    TIMESTAMP DEFAULT NOW(),
    UNIQUE (product_id, source)
);

CREATE TABLE IF NOT EXISTS price_alerts (
    id            SERIAL PRIMARY KEY,
    product_id    VARCHAR(128) REFERENCES products(product_id),
    user_email    VARCHAR(256) NOT NULL,
    threshold     NUMERIC(10, 2) NOT NULL,
    active        BOOLEAN DEFAULT TRUE,
    created_at    TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_current_prices_product ON current_prices(product_id);
