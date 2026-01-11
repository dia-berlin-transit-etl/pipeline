-- Run: 
-- sudo -u postgres psql -d public_transport_db -f bootstrap_extensions.sql
create extension if not exists pg_trgm;
create extension if not exists fuzzystrmatch;
