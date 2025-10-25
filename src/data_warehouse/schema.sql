CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(50) PRIMARY KEY,

    customer_id VARCHAR(50) NOT NULL,
    customer_name VARCHAR(255),
    customer_tier VARCHAR(20),
    amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(20) NOT NULL,

    order_created_at TIMESTAMP,
    order_updated_at TIMESTAMP,

    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    cdc_operation VARCHAR(20)
);

CREATE INDEX idx_customer_id ON orders(customer_id);
CREATE INDEX idx_processed_at ON orders(processed_at);