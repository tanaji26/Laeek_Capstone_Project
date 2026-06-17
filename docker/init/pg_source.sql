-- pg_source.sql
-- Initialises the retail source PostgreSQL database.
-- This runs automatically when the pg_source container first starts.

CREATE TABLE IF NOT EXISTS products (
    product_id          VARCHAR(20)    PRIMARY KEY,
    product_name        VARCHAR(255)   NOT NULL,
    product_category    VARCHAR(50)    NOT NULL,
    cost_price_per_unit NUMERIC(10,2)  NOT NULL,
    retail_price        NUMERIC(10,2)  NOT NULL,
    created_at          TIMESTAMP      DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS customers (
    customer_id         VARCHAR(20)    PRIMARY KEY,
    customer_name       VARCHAR(255)   NOT NULL,
    customer_email      VARCHAR(255),
    customer_location   VARCHAR(100),
    customer_region     VARCHAR(50),
    is_prime_customer   BOOLEAN        DEFAULT FALSE,
    created_at          TIMESTAMP      DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orders (
    order_id            VARCHAR(20)    PRIMARY KEY,
    product_id          VARCHAR(20)    REFERENCES products(product_id),
    customer_id         VARCHAR(20)    REFERENCES customers(customer_id),
    order_date          DATE           NOT NULL,
    order_status        VARCHAR(20)    NOT NULL,
    final_amount        NUMERIC(12,2),
    created_at          TIMESTAMP      DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS feedback (
    feedback_id     VARCHAR(20)   PRIMARY KEY,
    order_id        VARCHAR(20)   REFERENCES orders(order_id),
    review_date     DATE          NOT NULL,
    product_review  TEXT,
    review_score    SMALLINT      NOT NULL CHECK (review_score BETWEEN 1 AND 5),
    created_at      TIMESTAMP     DEFAULT NOW()
);