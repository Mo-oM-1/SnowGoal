-- ============================================
-- SNOWGOAL - File Formats & Stages
-- ============================================

USE ROLE SNOWGOAL_ROLE;
USE DATABASE SNOWGOAL_DB;
USE SCHEMA COMMON;

-- JSON File Format (pour données API)
CREATE OR REPLACE FILE FORMAT JSON_FORMAT
    TYPE = 'JSON'
    COMPRESSION = 'AUTO'
    STRIP_OUTER_ARRAY = FALSE
    ENABLE_OCTAL = FALSE
    ALLOW_DUPLICATE = FALSE
    STRIP_NULL_VALUES = FALSE
    IGNORE_UTF8_ERRORS = FALSE
    COMMENT = 'JSON format for football-data.org API responses';

-- Internal Stage pour les données API
CREATE OR REPLACE STAGE FOOTBALL_API_STAGE
    FILE_FORMAT = JSON_FORMAT
    COMMENT = 'Internal stage for football API data';

-- Verify
SHOW FILE FORMATS IN SCHEMA COMMON;
SHOW STAGES IN SCHEMA COMMON;
