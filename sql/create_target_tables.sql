CREATE SCHEMA IF NOT EXISTS refined;
CREATE SCHEMA IF NOT EXISTS analytics;

DROP TABLE IF EXISTS analytics.orders_daily_summary;
DROP TABLE IF EXISTS analytics.customer_orders_daily;
DROP TABLE IF EXISTS refined.orders_events_dedup;

CREATE TABLE refined.orders_events_dedup (
    event_id BIGINT PRIMARY KEY,
    order_id BIGINT NOT NULL,
    customer_id BIGINT NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('pending', 'paid', 'canceled')),
    amount NUMERIC(12,2) NOT NULL,
    business_date DATE NOT NULL,
    ingested_at TIMESTAMP NOT NULL,
    source_file VARCHAR(50) NOT NULL,
    processing_ts TIMESTAMP NOT NULL
);

CREATE INDEX idx_orders_events_dedup_business_date
    ON refined.orders_events_dedup (business_date);

CREATE INDEX idx_orders_events_dedup_customer_id
    ON refined.orders_events_dedup (customer_id);

CREATE INDEX idx_orders_events_dedup_order_id
    ON refined.orders_events_dedup (order_id);

CREATE INDEX idx_orders_events_dedup_ingested_at
    ON refined.orders_events_dedup (ingested_at);

CREATE TABLE analytics.customer_orders_daily (
    business_date DATE NOT NULL,
    customer_id BIGINT NOT NULL,
    total_orders INTEGER NOT NULL,
    pending_orders INTEGER NOT NULL,
    paid_orders INTEGER NOT NULL,
    canceled_orders INTEGER NOT NULL,
    total_amount NUMERIC(14,2) NOT NULL,
    pending_amount NUMERIC(14,2) NOT NULL,
    paid_amount NUMERIC(14,2) NOT NULL,
    canceled_amount NUMERIC(14,2) NOT NULL,
    last_ingested_at TIMESTAMP NOT NULL,
    processing_ts TIMESTAMP NOT NULL,
    PRIMARY KEY (business_date, customer_id)
);

CREATE INDEX idx_customer_orders_daily_business_date
    ON analytics.customer_orders_daily (business_date);

CREATE INDEX idx_customer_orders_daily_customer_id
    ON analytics.customer_orders_daily (customer_id);

CREATE TABLE analytics.orders_daily_summary (
    business_date DATE PRIMARY KEY,
    total_customers INTEGER NOT NULL,
    total_orders INTEGER NOT NULL,
    pending_orders INTEGER NOT NULL,
    paid_orders INTEGER NOT NULL,
    canceled_orders INTEGER NOT NULL,
    total_amount NUMERIC(14,2) NOT NULL,
    pending_amount NUMERIC(14,2) NOT NULL,
    paid_amount NUMERIC(14,2) NOT NULL,
    canceled_amount NUMERIC(14,2) NOT NULL,
    last_ingested_at TIMESTAMP NOT NULL,
    processing_ts TIMESTAMP NOT NULL
);

CREATE INDEX idx_orders_daily_summary_business_date
    ON analytics.orders_daily_summary (business_date);
