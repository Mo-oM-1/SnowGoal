-- ============================================
-- SNOWGOAL - Staging Views (FLATTEN JSON from Streams)
-- ============================================
-- Views read from STREAMS (CDC) to only process new/changed data
-- ============================================

USE ROLE SNOWGOAL_ROLE;
USE WAREHOUSE SNOWGOAL_WH_XS;
USE DATABASE SNOWGOAL_DB;
USE SCHEMA STAGING;

-- ----------------------------------------
-- V_MATCHES - Flatten matches JSON from Stream
-- ----------------------------------------
CREATE OR REPLACE VIEW V_MATCHES AS
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
    m.RAW_DATA:referees[0]:nationality::STRING AS REFEREE_NATIONALITY,
    m.RAW_DATA:referees[0]:id::INT AS REFEREE_ID,
    -- Match context
    m.RAW_DATA:score:duration::STRING AS MATCH_DURATION,
    m.RAW_DATA:area:name::STRING AS AREA_NAME,
    m.RAW_DATA:area:code::STRING AS AREA_CODE,
    -- Time analytics
    DAYNAME(m.RAW_DATA:utcDate::TIMESTAMP_NTZ) AS DAY_OF_WEEK,
    HOUR(m.RAW_DATA:utcDate::TIMESTAMP_NTZ) AS MATCH_HOUR,
    -- Season info
    m.RAW_DATA:season:id::INT AS SEASON_ID,
    m.RAW_DATA:season:startDate::DATE AS SEASON_START,
    m.RAW_DATA:season:endDate::DATE AS SEASON_END,
    m.RAW_DATA:season:currentMatchday::INT AS CURRENT_MATCHDAY,
    m.RAW_DATA:lastUpdated::TIMESTAMP_NTZ AS LAST_UPDATED
FROM SNOWGOAL_DB.RAW.STREAM_RAW_MATCHES m
WHERE m.METADATA$ACTION = 'INSERT'
  AND m.RAW_DATA:id IS NOT NULL;

-- ----------------------------------------
-- V_STANDINGS - Flatten standings JSON from Stream
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
FROM SNOWGOAL_DB.RAW.STREAM_RAW_STANDINGS s,
LATERAL FLATTEN(input => s.RAW_DATA:standings[0]:table) t
WHERE s.METADATA$ACTION = 'INSERT';

-- ----------------------------------------
-- V_TEAMS - Flatten teams JSON from Stream
-- ----------------------------------------
CREATE OR REPLACE VIEW V_TEAMS AS
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
FROM SNOWGOAL_DB.RAW.STREAM_RAW_TEAMS t
WHERE t.METADATA$ACTION = 'INSERT'
  AND t.RAW_DATA:id IS NOT NULL;

-- ----------------------------------------
-- V_SCORERS - Flatten scorers JSON from Stream
-- ----------------------------------------
CREATE OR REPLACE VIEW V_SCORERS AS
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
FROM SNOWGOAL_DB.RAW.STREAM_RAW_SCORERS s
WHERE s.METADATA$ACTION = 'INSERT'
  AND s.RAW_DATA:player:id IS NOT NULL;

-- ----------------------------------------
-- V_COMPETITIONS - Flatten competitions JSON
-- Note: No stream for competitions (table is small, full read is fine)
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

-- ----------------------------------------
-- V_ODDS - Flatten odds JSON from Stream
-- ----------------------------------------
CREATE OR REPLACE VIEW V_ODDS AS
WITH outcomes_flattened AS (
    SELECT
        o.COMPETITION_CODE,
        o.LOADED_AT,
        o.RAW_DATA:id::STRING AS GAME_ID,
        o.RAW_DATA:commence_time::TIMESTAMP_NTZ AS COMMENCE_TIME,
        o.RAW_DATA:home_team::STRING AS HOME_TEAM,
        o.RAW_DATA:away_team::STRING AS AWAY_TEAM,
        b.value:key::STRING AS BOOKMAKER_KEY,
        b.value:title::STRING AS BOOKMAKER_TITLE,
        b.value:last_update::TIMESTAMP_NTZ AS LAST_UPDATE,
        m.value:key::STRING AS MARKET_KEY,
        out.value:name::STRING AS OUTCOME_NAME,
        out.value:price::FLOAT AS ODDS
    FROM SNOWGOAL_DB.RAW.STREAM_RAW_ODDS o,
        LATERAL FLATTEN(input => o.RAW_DATA:bookmakers) b,
        LATERAL FLATTEN(input => b.value:markets) m,
        LATERAL FLATTEN(input => m.value:outcomes) out
    WHERE m.value:key::STRING = 'h2h'
      AND o.METADATA$ACTION = 'INSERT'
)
SELECT
    COMPETITION_CODE,
    LOADED_AT,
    GAME_ID,
    COMMENCE_TIME,
    HOME_TEAM,
    AWAY_TEAM,
    BOOKMAKER_KEY,
    BOOKMAKER_TITLE,
    LAST_UPDATE,
    MAX(CASE WHEN OUTCOME_NAME = HOME_TEAM THEN ODDS END) AS HOME_ODDS,
    MAX(CASE WHEN OUTCOME_NAME = 'Draw' THEN ODDS END) AS DRAW_ODDS,
    MAX(CASE WHEN OUTCOME_NAME = AWAY_TEAM THEN ODDS END) AS AWAY_ODDS
FROM outcomes_flattened
GROUP BY COMPETITION_CODE, LOADED_AT, GAME_ID, COMMENCE_TIME, HOME_TEAM, AWAY_TEAM, BOOKMAKER_KEY, BOOKMAKER_TITLE, LAST_UPDATE;

-- Verify
SHOW VIEWS IN SCHEMA STAGING;
