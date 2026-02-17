"""
SnowGoal - Fetch All Leagues
Fetches data for 11 competitions (Top 5 leagues + 6 other competitions) with rate limit handling
Optimized: batch inserts with PARSE_JSON to preserve VARIANT type
"""

import snowflake.snowpark as snowpark
import pandas as pd
import requests
import json
import time
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

def fetch_competition_data(competition_code, api_key):
    """Fetch all data for a competition and return as dicts (no DB calls)"""
    results = {"competition": competition_code, "teams": 0, "matches": 0, "standings": 0, "scorers": 0}
    data = {"competitions": [], "teams": [], "matches": [], "standings": [], "scorers": []}

    try:
        # 1. Competition info
        comp_data = fetch_api(f"/competitions/{competition_code}", api_key)
        data["competitions"].append({
            "COMPETITION_CODE": competition_code,
            "RAW_DATA": json.dumps(comp_data)
        })

        # 2. Teams
        teams_data = fetch_api(f"/competitions/{competition_code}/teams", api_key)
        for team in teams_data.get("teams", []):
            data["teams"].append({
                "COMPETITION_CODE": competition_code,
                "RAW_DATA": json.dumps(team)
            })
            results["teams"] += 1

        # 3. Matches
        matches_data = fetch_api(f"/competitions/{competition_code}/matches", api_key)
        for match in matches_data.get("matches", []):
            data["matches"].append({
                "COMPETITION_CODE": competition_code,
                "RAW_DATA": json.dumps(match)
            })
            results["matches"] += 1

        # 4. Standings
        standings_data = fetch_api(f"/competitions/{competition_code}/standings", api_key)
        data["standings"].append({
            "COMPETITION_CODE": competition_code,
            "RAW_DATA": json.dumps(standings_data)
        })
        results["standings"] = 1

        # 5. Scorers
        scorers_data = fetch_api(f"/competitions/{competition_code}/scorers", api_key)
        for scorer in scorers_data.get("scorers", []):
            data["scorers"].append({
                "COMPETITION_CODE": competition_code,
                "RAW_DATA": json.dumps(scorer)
            })
            results["scorers"] += 1

    except Exception as e:
        results["error"] = str(e)

    return results, data


def batch_insert(session, all_data, key, table_name):
    """write_pandas to temp table (VARCHAR), then INSERT SELECT PARSE_JSON into real table (VARIANT)"""
    rows = []
    for d in all_data:
        rows.extend(d.get(key, []))
    if not rows:
        return

    # 1. Build pandas DataFrame
    df = pd.DataFrame(rows)  # columns: COMPETITION_CODE, RAW_DATA (both str)

    # 2. write_pandas to a temp table (fast bulk load via COPY INTO)
    temp_table = f"TEMP_{table_name}"
    session.write_pandas(df, temp_table, auto_create_table=True, overwrite=True, table_type="temp", quote_identifiers=False)

    # 3. INSERT into real table with PARSE_JSON to convert VARCHAR -> VARIANT
    session.sql(f"""
        INSERT INTO RAW.{table_name} (COMPETITION_CODE, RAW_DATA)
        SELECT COMPETITION_CODE, PARSE_JSON(RAW_DATA)
        FROM {temp_table}
    """).collect()


def main(session: snowpark.Session) -> str:
    api_key = _snowflake.get_generic_secret_string('api_key')

    all_results = []
    all_data = []

    # Fetch all data from API (with rate limit delays)
    for i, comp_code in enumerate(COMPETITIONS):
        if i > 0:
            time.sleep(RATE_LIMIT_DELAY)

        result, data = fetch_competition_data(comp_code, api_key)
        all_results.append(result)
        all_data.append(data)

    # Batch insert all data into Snowflake (1 call per table instead of 1 per row)
    batch_insert(session, all_data, "competitions", "RAW_COMPETITIONS")
    batch_insert(session, all_data, "teams", "RAW_TEAMS")
    batch_insert(session, all_data, "matches", "RAW_MATCHES")
    batch_insert(session, all_data, "standings", "RAW_STANDINGS")
    batch_insert(session, all_data, "scorers", "RAW_SCORERS")

    # Summary
    total_teams = sum(r.get("teams", 0) for r in all_results)
    total_matches = sum(r.get("matches", 0) for r in all_results)
    total_scorers = sum(r.get("scorers", 0) for r in all_results)
    errors = [r for r in all_results if "error" in r]

    summary = f"SUCCESS: {len(COMPETITIONS)} leagues | Teams: {total_teams} | Matches: {total_matches} | Scorers: {total_scorers}"
    if errors:
        summary += f" | Errors: {len(errors)}"

    return summary
