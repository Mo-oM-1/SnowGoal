-- ============================================
-- SNOWGOAL - RAW Tables (VARIANT JSON)
-- ============================================

USE ROLE SNOWGOAL_ROLE;
USE DATABASE SNOWGOAL_DB;
USE SCHEMA RAW;

-- ----------------------------------------
-- Competitions (Ligues)
-- ----------------------------------------
CREATE OR REPLACE TABLE RAW_COMPETITIONS (
    ID NUMBER AUTOINCREMENT PRIMARY KEY,
    RAW_DATA VARIANT NOT NULL,
    COMPETITION_CODE VARCHAR(10),
    LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    SOURCE VARCHAR(50) DEFAULT 'football-data.org'
);

-- ----------------------------------------
-- Teams (Équipes)
-- ----------------------------------------
CREATE OR REPLACE TABLE RAW_TEAMS (
    ID NUMBER AUTOINCREMENT PRIMARY KEY,
    RAW_DATA VARIANT NOT NULL,
    COMPETITION_CODE VARCHAR(10),
    LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    SOURCE VARCHAR(50) DEFAULT 'football-data.org'
);

-- ----------------------------------------
-- Matches (Fixtures)
-- ----------------------------------------
CREATE OR REPLACE TABLE RAW_MATCHES (
    ID NUMBER AUTOINCREMENT PRIMARY KEY,
    RAW_DATA VARIANT NOT NULL,
    COMPETITION_CODE VARCHAR(10),
    SEASON_YEAR NUMBER,
    LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    SOURCE VARCHAR(50) DEFAULT 'football-data.org'
);

-- ----------------------------------------
-- Standings (Classements)
-- ----------------------------------------
CREATE OR REPLACE TABLE RAW_STANDINGS (
    ID NUMBER AUTOINCREMENT PRIMARY KEY,
    RAW_DATA VARIANT NOT NULL,
    COMPETITION_CODE VARCHAR(10),
    SEASON_YEAR NUMBER,
    LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    SOURCE VARCHAR(50) DEFAULT 'football-data.org'
);

-- ----------------------------------------
-- Scorers (Buteurs)
-- ----------------------------------------
CREATE OR REPLACE TABLE RAW_SCORERS (
    ID NUMBER AUTOINCREMENT PRIMARY KEY,
    RAW_DATA VARIANT NOT NULL,
    COMPETITION_CODE VARCHAR(10),
    SEASON_YEAR NUMBER,
    LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    SOURCE VARCHAR(50) DEFAULT 'football-data.org'
);

-- ----------------------------------------
-- Persons (Joueurs / Staff)
-- ----------------------------------------
CREATE OR REPLACE TABLE RAW_PERSONS (
    ID NUMBER AUTOINCREMENT PRIMARY KEY,
    RAW_DATA VARIANT NOT NULL,
    TEAM_ID NUMBER,
    LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    SOURCE VARCHAR(50) DEFAULT 'football-data.org'
);

-- Verify
SHOW TABLES IN SCHEMA RAW;
