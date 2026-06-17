-- mysql_source.sql
-- Initialises the catalog MySQL source database.
-- This runs automatically when the mysql_source container first starts.

CREATE TABLE IF NOT EXISTS product_catalog (
    product_id          VARCHAR(20)    PRIMARY KEY,
    product_name        VARCHAR(255)   NOT NULL,
    product_category    VARCHAR(50)    NOT NULL,
    cost_price          DECIMAL(10,2)  NOT NULL,
    retail_price        DECIMAL(10,2)  NOT NULL,
    stock_qty           INT            DEFAULT 0,
    is_active           TINYINT(1)     DEFAULT 1,
    created_at          TIMESTAMP      DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS product_reviews (
    review_id           VARCHAR(20)    PRIMARY KEY,
    product_id          VARCHAR(20),
    review_score        TINYINT        NOT NULL,
    review_text         TEXT,
    reviewed_at         TIMESTAMP      DEFAULT CURRENT_TIMESTAMP
);