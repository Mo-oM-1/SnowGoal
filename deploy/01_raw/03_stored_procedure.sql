-- ============================================
-- SNOWGOAL - Stored Procedure (API Fetch)
-- ============================================

USE DATABASE SNOWGOAL_DB;
USE SCHEMA RAW;

-- ============================================
-- IMPORTANT: Créer le secret manuellement dans Snowflake
-- Ne jamais commiter la clé API dans le code !
-- ============================================
-- Exécuter cette commande dans Snowflake avec ta vraie clé :
--
-- CREATE OR REPLACE SECRET SNOWGOAL_DB.RAW.FOOTBALL_API_KEY
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
    ALLOWED_NETWORK_RULES = (FOOTBALL_API_NETWORK_RULE)
    ALLOWED_AUTHENTICATION_SECRETS = (FOOTBALL_API_KEY)
    ENABLED = TRUE
    COMMENT = 'Access to football-data.org API';

-- Stored Procedure pour fetch les données
CREATE OR REPLACE PROCEDURE FETCH_FOOTBALL_DATA(COMPETITION_CODE VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests')
HANDLER = 'main'
EXTERNAL_ACCESS_INTEGRATIONS = (FOOTBALL_API_ACCESS)
SECRETS = ('api_key' = FOOTBALL_API_KEY)
AS
$$
import snowflake.snowpark as snowpark
import requests
import json
from datetime import datetime
import _snowflake

BASE_URL = "https://api.football-data.org/v4"

def fetch_api(endpoint: str, api_key: str) -> dict:
    headers = {"X-Auth-Token": api_key}
    response = requests.get(f"{BASE_URL}{endpoint}", headers=headers)
    response.raise_for_status()
    return response.json()

def escape_json(data: dict) -> str:
    """Escape JSON for SQL insertion"""
    return json.dumps(data).replace("'", "''")

def main(session: snowpark.Session, competition_code: str) -> str:
    api_key = _snowflake.get_generic_secret_string('api_key')
    results = []
    current_year = datetime.now().year

    try:
        # 1. Competition Info
        comp_data = fetch_api(f"/competitions/{competition_code}", api_key)
        session.sql(f"""
            INSERT INTO SNOWGOAL_DB.RAW.RAW_COMPETITIONS (RAW_DATA, COMPETITION_CODE)
            SELECT PARSE_JSON($${escape_json(comp_data)}$$), '{competition_code}'
        """).collect()
        results.append("Competition: OK")

        # 2. Teams
        teams_data = fetch_api(f"/competitions/{competition_code}/teams", api_key)
        session.sql(f"""
            INSERT INTO SNOWGOAL_DB.RAW.RAW_TEAMS (RAW_DATA, COMPETITION_CODE)
            SELECT PARSE_JSON($${escape_json(teams_data)}$$), '{competition_code}'
        """).collect()
        results.append(f"Teams: {len(teams_data.get('teams', []))}")

        # 3. Matches
        matches_data = fetch_api(f"/competitions/{competition_code}/matches", api_key)
        session.sql(f"""
            INSERT INTO SNOWGOAL_DB.RAW.RAW_MATCHES (RAW_DATA, COMPETITION_CODE, SEASON_YEAR)
            SELECT PARSE_JSON($${escape_json(matches_data)}$$), '{competition_code}', {current_year}
        """).collect()
        results.append(f"Matches: {len(matches_data.get('matches', []))}")

        # 4. Standings
        standings_data = fetch_api(f"/competitions/{competition_code}/standings", api_key)
        session.sql(f"""
            INSERT INTO SNOWGOAL_DB.RAW.RAW_STANDINGS (RAW_DATA, COMPETITION_CODE, SEASON_YEAR)
            SELECT PARSE_JSON($${escape_json(standings_data)}$$), '{competition_code}', {current_year}
        """).collect()
        results.append("Standings: OK")

        # 5. Top Scorers
        scorers_data = fetch_api(f"/competitions/{competition_code}/scorers?limit=50", api_key)
        session.sql(f"""
            INSERT INTO SNOWGOAL_DB.RAW.RAW_SCORERS (RAW_DATA, COMPETITION_CODE, SEASON_YEAR)
            SELECT PARSE_JSON($${escape_json(scorers_data)}$$), '{competition_code}', {current_year}
        """).collect()
        results.append(f"Scorers: {len(scorers_data.get('scorers', []))}")

        return f"SUCCESS [{competition_code}]: " + " | ".join(results)

    except Exception as e:
        return f"ERROR [{competition_code}]: {str(e)}"
$$;
