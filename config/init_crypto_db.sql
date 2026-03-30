-- Runs automatically when the postgres container starts for the first time
-- Creates a dedicated database and user for the crypto ETL pipeline

CREATE USER crypto WITH PASSWORD 'crypto';
CREATE DATABASE crypto_db OWNER crypto;
GRANT ALL PRIVILEGES ON DATABASE crypto_db TO crypto;
