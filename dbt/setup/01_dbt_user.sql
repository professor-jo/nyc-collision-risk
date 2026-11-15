-- One-time Redshift setup for dbt.
--
-- Run in Query Editor v2, connected to the nyc-collision-workgroup as
-- warehouse_admin (AWS Secrets Manager login), database collision_analytics.
-- Review it before running. Nothing here is run automatically.

-- 1. Expose the Silver Iceberg tables through Redshift Spectrum.
--    IAM_ROLE default is the Spectrum role the stack sets on the namespace.
CREATE EXTERNAL SCHEMA IF NOT EXISTS silver
FROM DATA CATALOG
DATABASE 'nyc_collision_lakehouse'
IAM_ROLE default;

-- 2. A normal (non-superuser) login for dbt.
--    Replace the placeholder with a new password: 8-64 characters with an
--    upper-case letter, a lower-case letter and a digit. Keep it in a
--    password manager and give it to dbt through DBT_REDSHIFT_PASSWORD.
--    Don't commit the real password.
CREATE USER dbt_user PASSWORD '<REPLACE_WITH_A_STRONG_PASSWORD>';

-- 3. Let dbt create its own schemas (such as dbt_dev). It owns what it
--    creates, but can't touch other users' schemas.
GRANT CREATE ON DATABASE collision_analytics TO dbt_user;

-- 4. Let dbt read Silver. Spectrum access to external tables is granted
--    through USAGE on the external schema.
GRANT USAGE ON SCHEMA silver TO dbt_user;

-- Check: dbt_user exists and is not a superuser (usesuper = false).
SELECT usename, usesuper FROM pg_user WHERE usename = 'dbt_user';
