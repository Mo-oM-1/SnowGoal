-- ============================================
-- SNOWGOAL - Fetch All Leagues Procedure
-- ============================================
USE ROLE SNOWGOAL_ROLE;
USE DATABASE SNOWGOAL_DB;
USE SCHEMA COMMON;

-- Upload the Python file first:
-- PUT file:///path/to/snowgoal/snowpark/procedures/fetch_all_leagues.py @FOOTBALL_API_STAGE/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- Create procedure that fetches all 5 leagues with rate limiting
CREATE OR REPLACE PROCEDURE FETCH_ALL_LEAGUES()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests')
IMPORTS = ('@SNOWGOAL_DB.RAW.PYTHON_CODE/fetch_all_leagues.py')
HANDLER = 'fetch_all_leagues.main'
EXTERNAL_ACCESS_INTEGRATIONS = (FOOTBALL_API_ACCESS)
SECRETS = ('api_key' = SNOWGOAL_DB.COMMON.FOOTBALL_API_KEY)
COMMENT = 'Fetches data for all 5 European leagues with 60s delay between each';