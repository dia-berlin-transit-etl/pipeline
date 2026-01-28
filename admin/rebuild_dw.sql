\set ON_ERROR_STOP on

-- variables expected:
-- :dw_user  (in docker compose setup: dia_user)

DROP SCHEMA IF EXISTS dw CASCADE;

\i schema.sql

-- extensions
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;

-- If dw_user is the same role running this script, these are harmless but redundant.
GRANT USAGE ON SCHEMA dw TO :dw_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA dw TO :dw_user;
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA dw TO :dw_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA dw
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO :dw_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA dw
  GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO :dw_user;
