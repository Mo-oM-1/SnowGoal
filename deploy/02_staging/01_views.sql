-- ============================================
-- SNOWGOAL - Staging Views (FLATTEN JSON)
-- ============================================

USE DATABASE SNOWGOAL_DB;
USE SCHEMA STAGING;

-- ----------------------------------------
-- V_MATCHES - Flatten matches JSON
-- Supports two formats:
--   1. Individual match JSON (fetch_all_leagues) where RAW_DATA:id exists
--   2. Array of matches (legacy) where RAW_DATA:matches exists
-- ----------------------------------------
CREATE OR REPLACE VIEW V_MATCHES AS
-- Format 1: Individual match JSON (fetch_all_leagues procedure)
SELECT
    m.ID AS RAW_ID,
    m.COMPETITION_CODE,
    COALESCE(m.SEASON_YEAR, YEAR(m.RAW_DATA:season:startDate::DATE)) AS SEASON_YEAR,
    m.LOADED_AT,
    m.RAW_DATA:id::INT AS MATCH_ID,
    m.RAW_DATA:utcDate::TIMESTAMP_NTZ AS MATCH_DATE,
    m.RAW_DATA:status::STRING AS STATUS,
    m.RAW_DATA:matchday::INT AS MATCHDAY,
    m.RAW_DATA:stage::STRING AS STAGE,
    -- Home Team
    m.RAW_DATA:homeTeam:id::INT AS HOME_TEAM_ID,
    m.RAW_DATA:homeTeam:name::STRING AS HOME_TEAM_NAME,
    m.RAW_DATA:homeTeam:shortName::STRING AS HOME_TEAM_SHORT,
    m.RAW_DATA:homeTeam:tla::STRING AS HOME_TEAM_TLA,
    -- Away Team
    m.RAW_DATA:awayTeam:id::INT AS AWAY_TEAM_ID,
    m.RAW_DATA:awayTeam:name::STRING AS AWAY_TEAM_NAME,
    m.RAW_DATA:awayTeam:shortName::STRING AS AWAY_TEAM_SHORT,
    m.RAW_DATA:awayTeam:tla::STRING AS AWAY_TEAM_TLA,
    -- Score
    m.RAW_DATA:score:fullTime:home::INT AS HOME_SCORE,
    m.RAW_DATA:score:fullTime:away::INT AS AWAY_SCORE,
    m.RAW_DATA:score:halfTime:home::INT AS HOME_SCORE_HT,
    m.RAW_DATA:score:halfTime:away::INT AS AWAY_SCORE_HT,
    m.RAW_DATA:score:winner::STRING AS WINNER,
    -- Referee
    m.RAW_DATA:referees[0]:name::STRING AS REFEREE_NAME,
    -- Season info
    m.RAW_DATA:season:id::INT AS SEASON_ID,
    m.RAW_DATA:season:startDate::DATE AS SEASON_START,
    m.RAW_DATA:season:endDate::DATE AS SEASON_END,
    m.RAW_DATA:lastUpdated::TIMESTAMP_NTZ AS LAST_UPDATED
FROM SNOWGOAL_DB.RAW.RAW_MATCHES m
WHERE m.RAW_DATA:id IS NOT NULL

UNION ALL

-- Format 2: Array of matches (legacy format with matches array)
SELECT
    m.ID AS RAW_ID,
    m.COMPETITION_CODE,
    m.SEASON_YEAR,
    m.LOADED_AT,
    f.value:id::INT AS MATCH_ID,
    f.value:utcDate::TIMESTAMP_NTZ AS MATCH_DATE,
    f.value:status::STRING AS STATUS,
    f.value:matchday::INT AS MATCHDAY,
    f.value:stage::STRING AS STAGE,
    -- Home Team
    f.value:homeTeam:id::INT AS HOME_TEAM_ID,
    f.value:homeTeam:name::STRING AS HOME_TEAM_NAME,
    f.value:homeTeam:shortName::STRING AS HOME_TEAM_SHORT,
    f.value:homeTeam:tla::STRING AS HOME_TEAM_TLA,
    -- Away Team
    f.value:awayTeam:id::INT AS AWAY_TEAM_ID,
    f.value:awayTeam:name::STRING AS AWAY_TEAM_NAME,
    f.value:awayTeam:shortName::STRING AS AWAY_TEAM_SHORT,
    f.value:awayTeam:tla::STRING AS AWAY_TEAM_TLA,
    -- Score
    f.value:score:fullTime:home::INT AS HOME_SCORE,
    f.value:score:fullTime:away::INT AS AWAY_SCORE,
    f.value:score:halfTime:home::INT AS HOME_SCORE_HT,
    f.value:score:halfTime:away::INT AS AWAY_SCORE_HT,
    f.value:score:winner::STRING AS WINNER,
    -- Referee
    f.value:referees[0]:name::STRING AS REFEREE_NAME,
    -- Season info
    f.value:season:id::INT AS SEASON_ID,
    f.value:season:startDate::DATE AS SEASON_START,
    f.value:season:endDate::DATE AS SEASON_END,
    f.value:lastUpdated::TIMESTAMP_NTZ AS LAST_UPDATED
FROM SNOWGOAL_DB.RAW.RAW_MATCHES m,
LATERAL FLATTEN(input => m.RAW_DATA:matches) f
WHERE m.RAW_DATA:matches IS NOT NULL;

-- ----------------------------------------
-- V_STANDINGS - Flatten standings JSON
-- ----------------------------------------
CREATE OR REPLACE VIEW V_STANDINGS AS
SELECT
    s.ID AS RAW_ID,
    s.COMPETITION_CODE,
    COALESCE(s.SEASON_YEAR, YEAR(s.RAW_DATA:season:startDate::DATE)) AS SEASON_YEAR,
    s.LOADED_AT,
    t.value:position::INT AS POSITION,
    t.value:team:id::INT AS TEAM_ID,
    t.value:team:name::STRING AS TEAM_NAME,
    t.value:team:shortName::STRING AS TEAM_SHORT,
    t.value:team:tla::STRING AS TEAM_TLA,
    t.value:team:crest::STRING AS TEAM_CREST,
    t.value:playedGames::INT AS PLAYED,
    t.value:won::INT AS WON,
    t.value:draw::INT AS DRAW,
    t.value:lost::INT AS LOST,
    t.value:points::INT AS POINTS,
    t.value:goalsFor::INT AS GOALS_FOR,
    t.value:goalsAgainst::INT AS GOALS_AGAINST,
    t.value:goalDifference::INT AS GOAL_DIFF,
    t.value:form::STRING AS FORM
FROM SNOWGOAL_DB.RAW.RAW_STANDINGS s,
LATERAL FLATTEN(input => s.RAW_DATA:standings[0]:table) t;

-- ----------------------------------------
-- V_TEAMS - Flatten teams JSON
-- Supports two formats:
--   1. Individual team JSON (fetch_all_leagues) where RAW_DATA:id exists
--   2. Array of teams (legacy) where RAW_DATA:teams exists
-- ----------------------------------------
CREATE OR REPLACE VIEW V_TEAMS AS
-- Format 1: Individual team JSON (fetch_all_leagues procedure)
SELECT
    t.ID AS RAW_ID,
    t.COMPETITION_CODE,
    t.LOADED_AT,
    t.RAW_DATA:id::INT AS TEAM_ID,
    t.RAW_DATA:name::STRING AS TEAM_NAME,
    t.RAW_DATA:shortName::STRING AS TEAM_SHORT,
    t.RAW_DATA:tla::STRING AS TEAM_TLA,
    t.RAW_DATA:crest::STRING AS TEAM_CREST,
    t.RAW_DATA:address::STRING AS ADDRESS,
    t.RAW_DATA:website::STRING AS WEBSITE,
    t.RAW_DATA:founded::INT AS FOUNDED,
    t.RAW_DATA:clubColors::STRING AS CLUB_COLORS,
    t.RAW_DATA:venue::STRING AS VENUE,
    t.RAW_DATA:coach:id::INT AS COACH_ID,
    t.RAW_DATA:coach:name::STRING AS COACH_NAME,
    t.RAW_DATA:coach:nationality::STRING AS COACH_NATIONALITY
FROM SNOWGOAL_DB.RAW.RAW_TEAMS t
WHERE t.RAW_DATA:id IS NOT NULL

UNION ALL

-- Format 2: Array of teams (legacy format)
SELECT
    t.ID AS RAW_ID,
    t.COMPETITION_CODE,
    t.LOADED_AT,
    f.value:id::INT AS TEAM_ID,
    f.value:name::STRING AS TEAM_NAME,
    f.value:shortName::STRING AS TEAM_SHORT,
    f.value:tla::STRING AS TEAM_TLA,
    f.value:crest::STRING AS TEAM_CREST,
    f.value:address::STRING AS ADDRESS,
    f.value:website::STRING AS WEBSITE,
    f.value:founded::INT AS FOUNDED,
    f.value:clubColors::STRING AS CLUB_COLORS,
    f.value:venue::STRING AS VENUE,
    f.value:coach:id::INT AS COACH_ID,
    f.value:coach:name::STRING AS COACH_NAME,
    f.value:coach:nationality::STRING AS COACH_NATIONALITY
FROM SNOWGOAL_DB.RAW.RAW_TEAMS t,
LATERAL FLATTEN(input => t.RAW_DATA:teams) f
WHERE t.RAW_DATA:teams IS NOT NULL;

-- ----------------------------------------
-- V_SCORERS - Flatten scorers JSON
-- Supports two formats:
--   1. Individual scorer JSON (fetch_all_leagues) where RAW_DATA:player:id exists
--   2. Array of scorers (legacy) where RAW_DATA:scorers exists
-- ----------------------------------------
CREATE OR REPLACE VIEW V_SCORERS AS
-- Format 1: Individual scorer JSON (fetch_all_leagues procedure)
SELECT
    s.ID AS RAW_ID,
    s.COMPETITION_CODE,
    COALESCE(s.SEASON_YEAR, YEAR(CURRENT_DATE())) AS SEASON_YEAR,
    s.LOADED_AT,
    s.RAW_DATA:player:id::INT AS PLAYER_ID,
    s.RAW_DATA:player:name::STRING AS PLAYER_NAME,
    s.RAW_DATA:player:firstName::STRING AS FIRST_NAME,
    s.RAW_DATA:player:lastName::STRING AS LAST_NAME,
    s.RAW_DATA:player:nationality::STRING AS NATIONALITY,
    s.RAW_DATA:player:position::STRING AS POSITION,
    s.RAW_DATA:player:dateOfBirth::DATE AS DATE_OF_BIRTH,
    s.RAW_DATA:team:id::INT AS TEAM_ID,
    s.RAW_DATA:team:name::STRING AS TEAM_NAME,
    s.RAW_DATA:team:shortName::STRING AS TEAM_SHORT,
    s.RAW_DATA:goals::INT AS GOALS,
    s.RAW_DATA:assists::INT AS ASSISTS,
    s.RAW_DATA:penalties::INT AS PENALTIES,
    s.RAW_DATA:playedMatches::INT AS PLAYED_MATCHES
FROM SNOWGOAL_DB.RAW.RAW_SCORERS s
WHERE s.RAW_DATA:player:id IS NOT NULL

UNION ALL

-- Format 2: Array of scorers (legacy format)
SELECT
    s.ID AS RAW_ID,
    s.COMPETITION_CODE,
    s.SEASON_YEAR,
    s.LOADED_AT,
    f.value:player:id::INT AS PLAYER_ID,
    f.value:player:name::STRING AS PLAYER_NAME,
    f.value:player:firstName::STRING AS FIRST_NAME,
    f.value:player:lastName::STRING AS LAST_NAME,
    f.value:player:nationality::STRING AS NATIONALITY,
    f.value:player:position::STRING AS POSITION,
    f.value:player:dateOfBirth::DATE AS DATE_OF_BIRTH,
    f.value:team:id::INT AS TEAM_ID,
    f.value:team:name::STRING AS TEAM_NAME,
    f.value:team:shortName::STRING AS TEAM_SHORT,
    f.value:goals::INT AS GOALS,
    f.value:assists::INT AS ASSISTS,
    f.value:penalties::INT AS PENALTIES,
    f.value:playedMatches::INT AS PLAYED_MATCHES
FROM SNOWGOAL_DB.RAW.RAW_SCORERS s,
LATERAL FLATTEN(input => s.RAW_DATA:scorers) f
WHERE s.RAW_DATA:scorers IS NOT NULL;

-- ----------------------------------------
-- V_COMPETITIONS - Flatten competitions JSON
-- ----------------------------------------
CREATE OR REPLACE VIEW V_COMPETITIONS AS
SELECT
    c.ID AS RAW_ID,
    c.COMPETITION_CODE,
    c.LOADED_AT,
    c.RAW_DATA:id::INT AS COMPETITION_ID,
    c.RAW_DATA:name::STRING AS COMPETITION_NAME,
    c.RAW_DATA:code::STRING AS CODE,
    c.RAW_DATA:type::STRING AS TYPE,
    c.RAW_DATA:emblem::STRING AS EMBLEM,
    c.RAW_DATA:area:name::STRING AS AREA_NAME,
    c.RAW_DATA:area:code::STRING AS AREA_CODE,
    c.RAW_DATA:area:flag::STRING AS AREA_FLAG,
    c.RAW_DATA:currentSeason:id::INT AS CURRENT_SEASON_ID,
    c.RAW_DATA:currentSeason:startDate::DATE AS SEASON_START,
    c.RAW_DATA:currentSeason:endDate::DATE AS SEASON_END,
    c.RAW_DATA:currentSeason:currentMatchday::INT AS CURRENT_MATCHDAY
FROM SNOWGOAL_DB.RAW.RAW_COMPETITIONS c;

-- Verify
SHOW VIEWS IN SCHEMA STAGING;
