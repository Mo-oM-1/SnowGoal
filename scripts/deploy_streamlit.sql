-- ============================================
-- SNOWGOAL - Deploy Streamlit App
-- Run this script to update the Streamlit app
-- ============================================

USE ROLE ACCOUNTADMIN;
USE DATABASE SNOWGOAL_DB;
USE SCHEMA COMMON;

-- Upload all Streamlit files
PUT file:///Users/moom/DataProjects/snowgoal/streamlit/Home.py @STREAMLIT_STAGE/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file:///Users/moom/DataProjects/snowgoal/streamlit/pages/1_Standings.py @STREAMLIT_STAGE/pages/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file:///Users/moom/DataProjects/snowgoal/streamlit/pages/2_Top_Scorers.py @STREAMLIT_STAGE/pages/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file:///Users/moom/DataProjects/snowgoal/streamlit/pages/3_Matches.py @STREAMLIT_STAGE/pages/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file:///Users/moom/DataProjects/snowgoal/streamlit/pages/4_Teams.py @STREAMLIT_STAGE/pages/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- Verify upload
LIST @STREAMLIT_STAGE;

-- The Streamlit app auto-refreshes after files are updated
SELECT 'Streamlit app updated successfully!' AS STATUS;
