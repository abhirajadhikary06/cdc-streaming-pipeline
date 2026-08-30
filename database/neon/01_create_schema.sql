-- CDC Source Table
CREATE TABLE IF NOT EXISTS customer_transactions (
    transaction_id BIGSERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    customer_name VARCHAR(100),
    product_id INTEGER,
    product_name VARCHAR(150),
    quantity INTEGER,
    unit_price DECIMAL(10, 2),
    total_amount DECIMAL(12, 2),
    status VARCHAR(30),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Enabling WAAL logical replicartion publication for Debezium
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'dbz_publication') THEN
        CREATE PUBLICATION dbz_publication FOR TABLE customer_transactions;
    END IF;
END $$