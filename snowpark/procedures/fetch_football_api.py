"""
SnowGoal - Fetch Football Data API
Stored Procedure Snowpark Python

Endpoints football-data.org (Free Tier):
- /competitions/{code}
- /competitions/{code}/teams
- /competitions/{code}/matches
- /competitions/{code}/standings
- /competitions/{code}/scorers
"""

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, lit, current_timestamp
import requests
import json
from datetime import datetime

# Competition codes (Top 5 leagues)
COMPETITIONS = {
    'PL': 'Premier League',
    'PD': 'La Liga',
    'BL1': 'Bundesliga',
    'SA': 'Serie A',
    'FL1': 'Ligue 1'
}

BASE_URL = "https://api.football-data.org/v4"


def fetch_api(endpoint: str, api_key: str) -> dict:
    """Fetch data from football-data.org API"""
    headers = {"X-Auth-Token": api_key}
    response = requests.get(f"{BASE_URL}{endpoint}", headers=headers)
    response.raise_for_status()
    return response.json()


def main(session: snowpark.Session, api_key: str, competition_code: str = 'PL') -> str:
    """
    Main procedure to fetch football data and insert into RAW tables.

    Args:
        session: Snowpark session
        api_key: football-data.org API key
        competition_code: League code (PL, PD, BL1, SA, FL1)

    Returns:
        Status message
    """
    results = []
    current_year = datetime.now().year

    try:
        # 1. Fetch Competition Info
        comp_data = fetch_api(f"/competitions/{competition_code}", api_key)
        session.sql(f"""
            INSERT INTO SNOWGOAL_DB.RAW.RAW_COMPETITIONS (RAW_DATA, COMPETITION_CODE)
            SELECT PARSE_JSON('{json.dumps(comp_data)}'), '{competition_code}'
        """).collect()
        results.append(f"Competition {competition_code}: OK")

        # 2. Fetch Teams
        teams_data = fetch_api(f"/competitions/{competition_code}/teams", api_key)
        session.sql(f"""
            INSERT INTO SNOWGOAL_DB.RAW.RAW_TEAMS (RAW_DATA, COMPETITION_CODE)
            SELECT PARSE_JSON('{json.dumps(teams_data)}'), '{competition_code}'
        """).collect()
        results.append(f"Teams: {len(teams_data.get('teams', []))} loaded")

        # 3. Fetch Matches (current season)
        matches_data = fetch_api(f"/competitions/{competition_code}/matches", api_key)
        session.sql(f"""
            INSERT INTO SNOWGOAL_DB.RAW.RAW_MATCHES (RAW_DATA, COMPETITION_CODE, SEASON_YEAR)
            SELECT PARSE_JSON('{json.dumps(matches_data)}'), '{competition_code}', {current_year}
        """).collect()
        results.append(f"Matches: {len(matches_data.get('matches', []))} loaded")

        # 4. Fetch Standings
        standings_data = fetch_api(f"/competitions/{competition_code}/standings", api_key)
        session.sql(f"""
            INSERT INTO SNOWGOAL_DB.RAW.RAW_STANDINGS (RAW_DATA, COMPETITION_CODE, SEASON_YEAR)
            SELECT PARSE_JSON('{json.dumps(standings_data)}'), '{competition_code}', {current_year}
        """).collect()
        results.append("Standings: OK")

        # 5. Fetch Top Scorers
        scorers_data = fetch_api(f"/competitions/{competition_code}/scorers?limit=50", api_key)
        session.sql(f"""
            INSERT INTO SNOWGOAL_DB.RAW.RAW_SCORERS (RAW_DATA, COMPETITION_CODE, SEASON_YEAR)
            SELECT PARSE_JSON('{json.dumps(scorers_data)}'), '{competition_code}', {current_year}
        """).collect()
        results.append(f"Scorers: {len(scorers_data.get('scorers', []))} loaded")

        return f"SUCCESS - {competition_code}: " + " | ".join(results)

    except Exception as e:
        return f"ERROR - {competition_code}: {str(e)}"
