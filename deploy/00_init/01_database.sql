-- ============================================
-- SNOWGOAL - Database & Schemas Setup
-- ============================================

USE ROLE ACCOUNTADMIN;

-- Database
CREATE DATABASE IF NOT EXISTS SNOWGOAL_DB;

USE DATABASE SNOWGOAL_DB;

-- Schemas (Medallion Architecture)
CREATE SCHEMA IF NOT EXISTS RAW;        -- JSON brut depuis API
CREATE SCHEMA IF NOT EXISTS STAGING;    -- Views FLATTEN
CREATE SCHEMA IF NOT EXISTS SILVER;     -- Tables nettoyées
CREATE SCHEMA IF NOT EXISTS GOLD;       -- Dynamic Tables + Agrégations
CREATE SCHEMA IF NOT EXISTS COMMON;     -- Stages, File Formats, Sequences

-- Warehouses
CREATE WAREHOUSE IF NOT EXISTS SNOWGOAL_WH_XS
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for SnowGoal ETL and queries';

CREATE WAREHOUSE IF NOT EXISTS SNOWGOAL_WH_LOADING
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for Snowpipe loading';

-- Set default context
USE WAREHOUSE SNOWGOAL_WH_XS;
USE SCHEMA RAW;

-- Verify setup
SHOW SCHEMAS IN DATABASE SNOWGOAL_DB;
SHOW WAREHOUSES LIKE 'SNOWGOAL%';
