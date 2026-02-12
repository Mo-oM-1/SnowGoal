import snowflake.snowpark as snowpark
import requests
import json
from datetime import datetime
import _snowflake

BASE_URL = "https://api.football-data.org/v4"

# Competition codes (football-data.org)
COMPETITIONS = {
    "PL": "PL",      # Premier League
    "PD": "PD",      # La Liga
    "BL1": "BL1",    # Bundesliga
    "SA": "SA",      # Serie A
    "FL1": "FL1"     # Ligue 1
}


def fetch_api(endpoint, api_key):
    headers = {"X-Auth-Token": api_key}
    url = BASE_URL + endpoint
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def escape_sql(s):
    return s.replace("\\", "\\\\").replace("'", "''")


def main(session: snowpark.Session, competition_code: str) -> str:
    api_key = _snowflake.get_generic_secret_string("api_key")
    results = []

    if competition_code not in COMPETITIONS:
        return "ERROR: Unknown competition code " + competition_code

    try:
        # 1. Competition Info
        comp_data = fetch_api("/competitions/" + competition_code, api_key)
        season_year = comp_data.get("currentSeason", {}).get("startDate", "")[:4]
        json_str = escape_sql(json.dumps(comp_data))
        session.sql(f"INSERT INTO SNOWGOAL_DB.RAW.RAW_COMPETITIONS (RAW_DATA, COMPETITION_CODE) SELECT PARSE_JSON('{json_str}'), '{competition_code}'").collect()
        results.append("Competition: OK")

        # 2. Teams
        teams_data = fetch_api("/competitions/" + competition_code + "/teams", api_key)
        json_str = escape_sql(json.dumps(teams_data))
        session.sql(f"INSERT INTO SNOWGOAL_DB.RAW.RAW_TEAMS (RAW_DATA, COMPETITION_CODE) SELECT PARSE_JSON('{json_str}'), '{competition_code}'").collect()
        team_count = len(teams_data.get("teams", []))
        results.append("Teams: " + str(team_count))

        # 3. Matches
        matches_data = fetch_api("/competitions/" + competition_code + "/matches", api_key)
        json_str = escape_sql(json.dumps(matches_data))
        session.sql(f"INSERT INTO SNOWGOAL_DB.RAW.RAW_MATCHES (RAW_DATA, COMPETITION_CODE, SEASON_YEAR) SELECT PARSE_JSON('{json_str}'), '{competition_code}', {season_year}").collect()
        match_count = len(matches_data.get("matches", []))
        results.append("Matches: " + str(match_count))

        # 4. Standings
        standings_data = fetch_api("/competitions/" + competition_code + "/standings", api_key)
        json_str = escape_sql(json.dumps(standings_data))
        session.sql(f"INSERT INTO SNOWGOAL_DB.RAW.RAW_STANDINGS (RAW_DATA, COMPETITION_CODE, SEASON_YEAR) SELECT PARSE_JSON('{json_str}'), '{competition_code}', {season_year}").collect()
        results.append("Standings: OK")

        # 5. Top Scorers
        scorers_data = fetch_api("/competitions/" + competition_code + "/scorers", api_key)
        json_str = escape_sql(json.dumps(scorers_data))
        session.sql(f"INSERT INTO SNOWGOAL_DB.RAW.RAW_SCORERS (RAW_DATA, COMPETITION_CODE, SEASON_YEAR) SELECT PARSE_JSON('{json_str}'), '{competition_code}', {season_year}").collect()
        scorer_count = len(scorers_data.get("scorers", []))
        results.append("Scorers: " + str(scorer_count))

        return "SUCCESS [" + competition_code + "]: " + " | ".join(results)

    except Exception as e:
        return "ERROR [" + competition_code + "]: " + str(e)
