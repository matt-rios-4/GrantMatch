-- ============================================================
-- PSet 2 - Bootstrap de Snowflake (ejecutar UNA vez, con ACCOUNTADMIN, en un worksheet)
-- Crea warehouse, database, esquemas por capa y un rol de trabajo para el equipo.
-- Los nombres deben coincidir con los valores por defecto de docker-compose.yml / .env
-- ============================================================
USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS PSET2_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS PSET2_DB;
CREATE SCHEMA IF NOT EXISTS PSET2_DB.BRONZE;   -- carga cruda (Kestra / COPY INTO)
CREATE SCHEMA IF NOT EXISTS PSET2_DB.SILVER;   -- limpieza (dbt)
CREATE SCHEMA IF NOT EXISTS PSET2_DB.GOLD;     -- star schema (dbt)
CREATE SCHEMA IF NOT EXISTS PSET2_DB.OBT;      -- One Big Table (Spark)

CREATE ROLE IF NOT EXISTS PSET2_ROLE;
GRANT USAGE, OPERATE ON WAREHOUSE PSET2_WH TO ROLE PSET2_ROLE;
GRANT ALL ON DATABASE PSET2_DB TO ROLE PSET2_ROLE;
GRANT ALL ON ALL SCHEMAS IN DATABASE PSET2_DB TO ROLE PSET2_ROLE;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE PSET2_DB TO ROLE PSET2_ROLE;
GRANT ALL ON FUTURE TABLES IN DATABASE PSET2_DB TO ROLE PSET2_ROLE;

-- Dar el rol a cada integrante (reemplazar por los usuarios reales):
-- GRANT ROLE PSET2_ROLE TO USER <USUARIO_1>;
-- GRANT ROLE PSET2_ROLE TO USER <USUARIO_2>;

-- Verificacion
SHOW SCHEMAS IN DATABASE PSET2_DB;
