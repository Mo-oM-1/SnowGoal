-- ============================================
-- SNOWGOAL - Staging Views (FLATTEN JSON)
-- ============================================

USE DATABASE SNOWGOAL_DB;
USE SCHEMA STAGING;

-- ----------------------------------------
-- V_MATCHES - Flatten matches JSON
-- ----------------------------------------
CREATE OR REPLACE VIEW V_MATCHES AS
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
LATERAL FLATTEN(input => m.RAW_DATA:matches) f;

-- ----------------------------------------
-- V_STANDINGS - Flatten standings JSON
-- ----------------------------------------
CREATE OR REPLACE VIEW V_STANDINGS AS
SELECT
    s.ID AS RAW_ID,
    s.COMPETITION_CODE,
    s.SEASON_YEAR,
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
-- ----------------------------------------
CREATE OR REPLACE VIEW V_TEAMS AS
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
LATERAL FLATTEN(input => t.RAW_DATA:teams) f;

-- ----------------------------------------
-- V_SCORERS - Flatten scorers JSON
-- ----------------------------------------
CREATE OR REPLACE VIEW V_SCORERS AS
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
LATERAL FLATTEN(input => s.RAW_DATA:scorers) f;

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
