-- ============================================
-- SNOWGOAL - Deploy Streamlit App in Snowflake
-- ============================================

USE DATABASE SNOWGOAL_DB;
USE SCHEMA COMMON;

-- Create a stage for Streamlit files
CREATE OR REPLACE STAGE STREAMLIT_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for Streamlit app files';

-- Upload files manually using:
-- PUT file:///path/to/snowgoal/streamlit/Home.py @STREAMLIT_STAGE/;
-- PUT file:///path/to/snowgoal/streamlit/pages/* @STREAMLIT_STAGE/pages/;

-- Or use SnowCLI:
-- snow streamlit deploy --file Home.py

-- Create Streamlit App
CREATE OR REPLACE STREAMLIT SNOWGOAL_DASHBOARD
    ROOT_LOCATION = '@SNOWGOAL_DB.COMMON.STREAMLIT_STAGE'
    MAIN_FILE = 'Home.py'
    QUERY_WAREHOUSE = SNOWGOAL_WH
    COMMENT = 'SnowGoal Football Analytics Dashboard';

-- Grant access to roles
GRANT USAGE ON STREAMLIT SNOWGOAL_DASHBOARD TO ROLE PUBLIC;

-- Show Streamlit apps
SHOW STREAMLITS IN SCHEMA COMMON;

-- Get the URL
SELECT SYSTEM$GET_STREAMLIT_APP_URL('SNOWGOAL_DB.COMMON.SNOWGOAL_DASHBOARD');
