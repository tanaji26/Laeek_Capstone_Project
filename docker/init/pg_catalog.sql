
-- Ensure the iceberg user owns the database objects
ALTER DATABASE iceberg OWNER TO iceberg;

-- Grant all privileges (needed for Iceberg catalog to create its own tables)
GRANT ALL PRIVILEGES ON DATABASE iceberg TO iceberg;
GRANT ALL ON SCHEMA public TO iceberg;