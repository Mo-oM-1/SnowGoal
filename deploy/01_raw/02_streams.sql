-- ============================================
-- SNOWGOAL - Streams (CDC)
-- ============================================

USE ROLE SNOWGOAL_ROLE;
USE WAREHOUSE SNOWGOAL_WH_XS;
USE DATABASE SNOWGOAL_DB;
USE SCHEMA RAW;

-- Stream sur RAW_MATCHES pour capturer nouveaux matchs/scores
CREATE OR REPLACE STREAM STREAM_RAW_MATCHES
    ON TABLE RAW_MATCHES
    APPEND_ONLY = FALSE
    SHOW_INITIAL_ROWS = FALSE
    COMMENT = 'CDC stream for new matches and score updates';

-- Stream sur RAW_STANDINGS pour capturer changements de classement
CREATE OR REPLACE STREAM STREAM_RAW_STANDINGS
    ON TABLE RAW_STANDINGS
    APPEND_ONLY = FALSE
    SHOW_INITIAL_ROWS = FALSE
    COMMENT = 'CDC stream for standings updates';

-- Stream sur RAW_SCORERS pour capturer nouveaux buteurs
CREATE OR REPLACE STREAM STREAM_RAW_SCORERS
    ON TABLE RAW_SCORERS
    APPEND_ONLY = FALSE
    SHOW_INITIAL_ROWS = FALSE
    COMMENT = 'CDC stream for top scorers updates';

-- Stream sur RAW_TEAMS pour capturer changements équipes
CREATE OR REPLACE STREAM STREAM_RAW_TEAMS
    ON TABLE RAW_TEAMS
    APPEND_ONLY = FALSE
    SHOW_INITIAL_ROWS = FALSE
    COMMENT = 'CDC stream for team info updates';

-- Verify
SHOW STREAMS IN SCHEMA RAW;
