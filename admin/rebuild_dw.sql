-- admin/rebuild_dw.sql
\set ON_ERROR_STOP on

-- variables expected:
-- :dw_user

DROP SCHEMA IF EXISTS dw CASCADE;

-- your schema.sql will recreate dw + tables
\i schema.sql

-- extensions live in the database; needs superuser
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;

-- allow using the schema
GRANT USAGE ON SCHEMA dw TO :dw_user;

-- allow working with ALL CURRENT tables
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA dw TO :dw_user;

-- allow sequences (bigserial)
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA dw TO :dw_user;

-- ensure FUTURE tables/sequences created in dw are accessible (for the role running schema.sql: postgres)
ALTER DEFAULT PRIVILEGES IN SCHEMA dw
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO :dw_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA dw
GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO :dw_user;
