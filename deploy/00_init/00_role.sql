-- ============================================
-- SNOWGOAL - Role Setup
-- Run ONCE with ACCOUNTADMIN before all other scripts
-- ============================================

USE ROLE ACCOUNTADMIN;

-- Create dedicated role for SnowGoal pipeline
CREATE ROLE IF NOT EXISTS SNOWGOAL_ROLE;

-- Grant role to current user
GRANT ROLE SNOWGOAL_ROLE TO USER IDENTIFIER(CURRENT_USER());

-- Grant warehouse creation (needed for 01_database.sql)
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE SNOWGOAL_ROLE;

-- Grant database creation (needed for 01_database.sql)
GRANT CREATE DATABASE ON ACCOUNT TO ROLE SNOWGOAL_ROLE;

-- Grant integration creation (needed for External Access)
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE SNOWGOAL_ROLE;

-- Grant task execution (needed for scheduled tasks)
GRANT EXECUTE TASK ON ACCOUNT TO ROLE SNOWGOAL_ROLE;

-- ============================================
-- After running this script:
-- USE ROLE SNOWGOAL_ROLE;
-- Then run all other scripts with this role
-- ============================================
