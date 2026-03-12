-- ============================================================
-- init_postgres.sql
-- Runs automatically on first Postgres container startup.
-- Creates the app database and user (Airflow DB is created by
-- the POSTGRES_DB env var in docker-compose).
-- ============================================================

-- Create the application user
DO
$$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'app_user') THEN
      CREATE ROLE app_user WITH LOGIN PASSWORD 'app_password';
   END IF;
END
$$;

-- Create the application database
SELECT 'CREATE DATABASE financial_dq OWNER app_user'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'financial_dq')\gexec

-- Connect to the app database and grant privileges
\c financial_dq

GRANT ALL PRIVILEGES ON SCHEMA public TO app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO app_user;