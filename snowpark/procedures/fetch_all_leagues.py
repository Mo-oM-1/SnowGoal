"""
SnowGoal - Fetch All Leagues
Fetches data for 11 competitions (Top 5 leagues + 6 other competitions) with rate limit handling
"""

import snowflake.snowpark as snowpark
import requests
import json
import time
from datetime import datetime
import _snowflake

BASE_URL = "https://api.football-data.org/v4"

COMPETITIONS = [
    # Top 5 European Leagues
    "PL",   # Premier League (England)
    "PD",   # La Liga (Spain)
    "BL1",  # Bundesliga (Germany)
    "SA",   # Serie A (Italy)
    "FL1",  # Ligue 1 (France)

    # International Competitions
    "CL",   # UEFA Champions League
    "EC",   # UEFA European Championship

    # Other European Leagues
    "PPL",  # Primeira Liga (Portugal)
    "DED",  # Eredivisie (Netherlands)
    "ELC",  # Championship (England)
    "BSA",  # Brasileirão (Brazil)
]

# Rate limit: 10 calls/minute on free tier
# 5 endpoints per league = need 30s between leagues to stay safe
RATE_LIMIT_DELAY = 30  # seconds between competitions

def fetch_api(endpoint, api_key):
    headers = {"X-Auth-Token": api_key}
    url = BASE_URL + endpoint
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def escape_sql(s):
    if s is None:
        return ""
    return str(s).replace("\\", "\\\\").replace("'", "''")

def fetch_competition_data(session, competition_code, api_key):
    results = {"competition": competition_code, "teams": 0, "matches": 0, "standings": False, "scorers": 0}

    try:
        # 1. Competition info
        comp_data = fetch_api(f"/competitions/{competition_code}", api_key)
        json_str = escape_sql(json.dumps(comp_data))
        session.sql(f"""
            INSERT INTO RAW.RAW_COMPETITIONS (COMPETITION_CODE, RAW_DATA)
            SELECT '{competition_code}', PARSE_JSON('{json_str}')
        """).collect()

        # 2. Teams
        teams_data = fetch_api(f"/competitions/{competition_code}/teams", api_key)
        for team in teams_data.get("teams", []):
            json_str = escape_sql(json.dumps(team))
            session.sql(f"""
                INSERT INTO RAW.RAW_TEAMS (COMPETITION_CODE, RAW_DATA)
                SELECT '{competition_code}', PARSE_JSON('{json_str}')
            """).collect()
            results["teams"] += 1

        # 3. Matches
        matches_data = fetch_api(f"/competitions/{competition_code}/matches", api_key)
        for match in matches_data.get("matches", []):
            json_str = escape_sql(json.dumps(match))
            session.sql(f"""
                INSERT INTO RAW.RAW_MATCHES (COMPETITION_CODE, RAW_DATA)
                SELECT '{competition_code}', PARSE_JSON('{json_str}')
            """).collect()
            results["matches"] += 1

        # 4. Standings
        standings_data = fetch_api(f"/competitions/{competition_code}/standings", api_key)
        json_str = escape_sql(json.dumps(standings_data))
        session.sql(f"""
            INSERT INTO RAW.RAW_STANDINGS (COMPETITION_CODE, RAW_DATA)
            SELECT '{competition_code}', PARSE_JSON('{json_str}')
        """).collect()
        results["standings"] = True

        # 5. Scorers
        scorers_data = fetch_api(f"/competitions/{competition_code}/scorers", api_key)
        for scorer in scorers_data.get("scorers", []):
            json_str = escape_sql(json.dumps(scorer))
            session.sql(f"""
                INSERT INTO RAW.RAW_SCORERS (COMPETITION_CODE, RAW_DATA)
                SELECT '{competition_code}', PARSE_JSON('{json_str}')
            """).collect()
            results["scorers"] += 1

        return results

    except Exception as e:
        results["error"] = str(e)
        return results

def main(session: snowpark.Session) -> str:
    api_key = _snowflake.get_generic_secret_string('api_key')

    all_results = []

    for i, comp_code in enumerate(COMPETITIONS):
        # Rate limit: wait between competitions (except first)
        if i > 0:
            time.sleep(RATE_LIMIT_DELAY)

        result = fetch_competition_data(session, comp_code, api_key)
        all_results.append(result)

    # Summary
    total_teams = sum(r.get("teams", 0) for r in all_results)
    total_matches = sum(r.get("matches", 0) for r in all_results)
    total_scorers = sum(r.get("scorers", 0) for r in all_results)
    errors = [r for r in all_results if "error" in r]

    summary = f"SUCCESS: {len(COMPETITIONS)} leagues | Teams: {total_teams} | Matches: {total_matches} | Scorers: {total_scorers}"
    if errors:
        summary += f" | Errors: {len(errors)}"

    return summary
