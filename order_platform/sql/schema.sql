CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS processed_events (
    consumer_name TEXT NOT NULL,
    event_id UUID NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_name, event_id)
);

CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    payment_method_id TEXT NOT NULL,
    status TEXT NOT NULL,
    total_cents INTEGER NOT NULL DEFAULT 0,
    trace_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS order_items (
    order_id TEXT NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    sku TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price_cents INTEGER NOT NULL CHECK (unit_price_cents >= 0),
    PRIMARY KEY (order_id, sku)
);

CREATE TABLE IF NOT EXISTS inventory (
    sku TEXT PRIMARY KEY,
    available INTEGER NOT NULL CHECK (available >= 0),
    reserved INTEGER NOT NULL CHECK (reserved >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS inventory_reservations (
    order_id TEXT NOT NULL,
    reservation_id TEXT NOT NULL,
    sku TEXT NOT NULL REFERENCES inventory(sku),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, sku)
);

CREATE TABLE IF NOT EXISTS payments (
    payment_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    amount_cents INTEGER NOT NULL CHECK (amount_cents >= 0),
    status TEXT NOT NULL,
    charge_id TEXT,
    failure_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS shipments (
    shipment_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL,
    trace_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS saga_compensations (
    order_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    inventory_released BOOLEAN NOT NULL DEFAULT false,
    order_cancelled BOOLEAN NOT NULL DEFAULT false,
    reason TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dlq_messages (
    id BIGSERIAL PRIMARY KEY,
    original_topic TEXT NOT NULL,
    event_id UUID,
    payload JSONB NOT NULL,
    error TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    replayed_at TIMESTAMPTZ
);

INSERT INTO inventory (sku, available, reserved)
VALUES
    ('SKU-RED-CHAIR', 1000, 0),
    ('SKU-BLUE-DESK', 500, 0),
    ('SKU-GREEN-LAMP', 250, 0)
ON CONFLICT (sku) DO NOTHING;
