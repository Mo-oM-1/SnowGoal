import snowflake.snowpark as snowpark
import requests
import json
from datetime import datetime
import _snowflake

BASE_URL = "https://v3.football.api-sports.io"

LEAGUE_IDS = {
    "PL": 39,
    "PD": 140,
    "BL1": 78,
    "SA": 135,
    "FL1": 61
}


def fetch_api(endpoint, api_key):
    headers = {"x-apisports-key": api_key}
    url = BASE_URL + endpoint
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def escape_sql(s):
    return s.replace("\\", "\\\\").replace("'", "''")


def main(session: snowpark.Session, competition_code: str) -> str:
    api_key = _snowflake.get_generic_secret_string("api_key")
    results = []
    season = 2024

    league_id = LEAGUE_IDS.get(competition_code)
    if not league_id:
        return "ERROR: Unknown competition code " + competition_code

    try:
        # 1. League Info
        league_data = fetch_api("/leagues?id=" + str(league_id), api_key)
        json_str = escape_sql(json.dumps(league_data))
        session.sql(f"INSERT INTO SNOWGOAL_DB.RAW.RAW_COMPETITIONS (RAW_DATA, COMPETITION_CODE) SELECT PARSE_JSON('{json_str}'), '{competition_code}'").collect()
        results.append("League: OK")

        # 2. Teams
        teams_data = fetch_api("/teams?league=" + str(league_id) + "&season=" + str(season), api_key)
        json_str = escape_sql(json.dumps(teams_data))
        session.sql(f"INSERT INTO SNOWGOAL_DB.RAW.RAW_TEAMS (RAW_DATA, COMPETITION_CODE) SELECT PARSE_JSON('{json_str}'), '{competition_code}'").collect()
        team_count = len(teams_data.get("response", []))
        results.append("Teams: " + str(team_count))

        # 3. Fixtures (Matches)
        fixtures_data = fetch_api("/fixtures?league=" + str(league_id) + "&season=" + str(season), api_key)
        json_str = escape_sql(json.dumps(fixtures_data))
        session.sql(f"INSERT INTO SNOWGOAL_DB.RAW.RAW_MATCHES (RAW_DATA, COMPETITION_CODE, SEASON_YEAR) SELECT PARSE_JSON('{json_str}'), '{competition_code}', {season}").collect()
        match_count = len(fixtures_data.get("response", []))
        results.append("Matches: " + str(match_count))

        # 4. Standings
        standings_data = fetch_api("/standings?league=" + str(league_id) + "&season=" + str(season), api_key)
        json_str = escape_sql(json.dumps(standings_data))
        session.sql(f"INSERT INTO SNOWGOAL_DB.RAW.RAW_STANDINGS (RAW_DATA, COMPETITION_CODE, SEASON_YEAR) SELECT PARSE_JSON('{json_str}'), '{competition_code}', {season}").collect()
        results.append("Standings: OK")

        # 5. Top Scorers
        scorers_data = fetch_api("/players/topscorers?league=" + str(league_id) + "&season=" + str(season), api_key)
        json_str = escape_sql(json.dumps(scorers_data))
        session.sql(f"INSERT INTO SNOWGOAL_DB.RAW.RAW_SCORERS (RAW_DATA, COMPETITION_CODE, SEASON_YEAR) SELECT PARSE_JSON('{json_str}'), '{competition_code}', {season}").collect()
        scorer_count = len(scorers_data.get("response", []))
        results.append("Scorers: " + str(scorer_count))

        return "SUCCESS [" + competition_code + "]: " + " | ".join(results)

    except Exception as e:
        return "ERROR [" + competition_code + "]: " + str(e)
