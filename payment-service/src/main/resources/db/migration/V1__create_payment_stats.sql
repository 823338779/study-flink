CREATE TABLE processed_event (
    handler_name VARCHAR(64) NOT NULL,
    event_id VARCHAR(100) NOT NULL,
    shop_id VARCHAR(64) NOT NULL,
    version BIGINT NOT NULL CHECK (version > 0),
    processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (handler_name, event_id)
);
CREATE TABLE shop_payment_stat (
    shop_id VARCHAR(64) PRIMARY KEY,
    total_amount_cents BIGINT NOT NULL CHECK (total_amount_cents > 0),
    payment_count BIGINT NOT NULL CHECK (payment_count > 0),
    version BIGINT NOT NULL CHECK (version = payment_count),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
