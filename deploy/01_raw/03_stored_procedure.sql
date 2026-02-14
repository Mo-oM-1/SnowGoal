-- ============================================
-- SNOWGOAL - Stored Procedure (API Fetch)
-- ============================================
USE ROLE SNOWGOAL_ROLE;
USE WAREHOUSE SNOWGOAL_WH_XS;
USE DATABASE SNOWGOAL_DB;
USE SCHEMA COMMON;

-- ============================================
-- IMPORTANT: Créer le secret manuellement dans Snowflake
-- Ne jamais commiter la clé API dans le code !
-- ============================================
-- CREATE OR REPLACE SECRET SNOWGOAL_DB.COMMON.FOOTBALL_API_KEY
--     TYPE = GENERIC_STRING
--     SECRET_STRING = '<ta-cle-api>';
-- ============================================

-- Network rule pour autoriser l'accès à l'API
CREATE OR REPLACE NETWORK RULE FOOTBALL_API_NETWORK_RULE
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('api.football-data.org:443');

-- External Access Integration
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION FOOTBALL_API_ACCESS
    ALLOWED_NETWORK_RULES = (SNOWGOAL_DB.COMMON.FOOTBALL_API_NETWORK_RULE)
    ALLOWED_AUTHENTICATION_SECRETS = (SNOWGOAL_DB.COMMON.FOOTBALL_API_KEY)
    ENABLED = TRUE;

-- Stored Procedure qui référence le fichier Python sur FOOTBALL_API_STAGE
CREATE OR REPLACE PROCEDURE FETCH_FOOTBALL_DATA(COMPETITION_CODE VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests')
IMPORTS = ('@SNOWGOAL_DB.RAW.PYTHON_CODE/fetch_football_api.py')
HANDLER = 'fetch_football_api.main'
EXTERNAL_ACCESS_INTEGRATIONS = (FOOTBALL_API_ACCESS)
SECRETS = ('api_key' = SNOWGOAL_DB.COMMON.FOOTBALL_API_KEY);
