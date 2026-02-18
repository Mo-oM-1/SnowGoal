"""
SnowGoal - Fetch All Leagues
Version: 2.0 - With Partial Success Logic & Intelligent Status
"""

import snowflake.snowpark as snowpark
import pandas as pd
import requests
import json
import time
import _snowflake
import traceback

BASE_URL = "https://api.football-data.org/v4"

COMPETITIONS = [
    "PL", "PD", "BL1", "SA", "FL1", "CL", "EC", "PPL", "DED", "ELC", "BSA"
]

# 10 calls/minute = 30s delay between leagues for 5 endpoints per league
RATE_LIMIT_DELAY = 30 

def fetch_api(endpoint, api_key):
    headers = {"X-Auth-Token": api_key}
    url = BASE_URL + endpoint
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def fetch_competition_data(competition_code, api_key):
    """Fetch all data for a competition and return as dicts"""
    results = {"competition": competition_code, "teams": 0, "matches": 0, "standings": 0, "scorers": 0}
    data = {"competitions": [], "teams": [], "matches": [], "standings": [], "scorers": []}

    try:
        # 1. Competition info
        comp_data = fetch_api(f"/competitions/{competition_code}", api_key)
        data["competitions"].append({"COMPETITION_CODE": competition_code, "RAW_DATA": json.dumps(comp_data)})

        # 2. Teams
        teams_data = fetch_api(f"/competitions/{competition_code}/teams", api_key)
        for team in teams_data.get("teams", []):
            data["teams"].append({"COMPETITION_CODE": competition_code, "RAW_DATA": json.dumps(team)})
            results["teams"] += 1

        # 3. Matches
        matches_data = fetch_api(f"/competitions/{competition_code}/matches", api_key)
        for match in matches_data.get("matches", []):
            data["matches"].append({"COMPETITION_CODE": competition_code, "RAW_DATA": json.dumps(match)})
            results["matches"] += 1

        # 4. Standings
        standings_data = fetch_api(f"/competitions/{competition_code}/standings", api_key)
        data["standings"].append({"COMPETITION_CODE": competition_code, "RAW_DATA": json.dumps(standings_data)})
        results["standings"] = 1

        # 5. Scorers
        scorers_data = fetch_api(f"/competitions/{competition_code}/scorers", api_key)
        for scorer in scorers_data.get("scorers", []):
            data["scorers"].append({"COMPETITION_CODE": competition_code, "RAW_DATA": json.dumps(scorer)})
            results["scorers"] += 1

    except Exception as e:
        results["error"] = str(e)

    return results, data


def batch_insert(session, all_data, key, table_name):
    """Bulk insert using write_pandas + PARSE_JSON"""
    rows = []
    for d in all_data:
        rows.extend(d.get(key, []))
    if not rows:
        return

    df = pd.DataFrame(rows)
    temp_table = f"TEMP_{table_name}"
    session.write_pandas(df, temp_table, auto_create_table=True, overwrite=True, table_type="temp", quote_identifiers=False)

    session.sql(f"""
        INSERT INTO RAW.{table_name} (COMPETITION_CODE, RAW_DATA)
        SELECT COMPETITION_CODE, PARSE_JSON(RAW_DATA)
        FROM {temp_table}
    """).collect()


def main(session: snowpark.Session) -> str:
    COMPONENT_NAME = 'FETCH_ALL_LEAGUES'
    
    try:
        api_key = _snowflake.get_generic_secret_string('api_key')
        all_results = []
        all_data = []

        # 1. RÉCUPÉRATION DES DONNÉES
        for i, comp_code in enumerate(COMPETITIONS):
            if i > 0:
                time.sleep(RATE_LIMIT_DELAY)

            result, data = fetch_competition_data(comp_code, api_key)
            all_results.append(result)
            all_data.append(data)

        # 2. BATCH INSERT DANS SNOWFLAKE
        batch_insert(session, all_data, "competitions", "RAW_COMPETITIONS")
        batch_insert(session, all_data, "teams", "RAW_TEAMS")
        batch_insert(session, all_data, "matches", "RAW_MATCHES")
        batch_insert(session, all_data, "standings", "RAW_STANDINGS")
        batch_insert(session, all_data, "scorers", "RAW_SCORERS")

        # 3. ANALYSE DES RÉSULTATS
        total_teams = sum(r.get("teams", 0) for r in all_results)
        total_matches = sum(r.get("matches", 0) for r in all_results)
        total_scorers = sum(r.get("scorers", 0) for r in all_results)
        
        # On extrait la liste propre des erreurs par ligue
        league_errors = [f"{r['competition']}: {r['error']}" for r in all_results if "error" in r]
        
        num_leagues = len(COMPETITIONS)
        num_errors = len(league_errors)

        # --- LOGIQUE DE STATUT INTELLIGENTE ---
        if num_errors == 0:
            status = "SUCCESS"
            log_level = "INFO"
        elif num_errors < num_leagues:
            status = "PARTIAL SUCCESS"
            log_level = "WARNING"
        else:
            status = "FAILED"
            log_level = "ERROR"

        summary = f"{status}: {num_leagues - num_errors}/{num_leagues} leagues loaded | Teams: {total_teams} | Matches: {total_matches}"
        
        if league_errors:
            summary += f" | {num_errors} error(s) detected"

        # 4. LOGGING CENTRALISÉ
        session.sql(
            "INSERT INTO SNOWGOAL_DB.COMMON.PIPELINE_LOGS (LEVEL, COMPONENT_NAME, MESSAGE, STACK_TRACE) VALUES (?, ?, ?, ?)",
            params=[log_level, COMPONENT_NAME, summary, "; ".join(league_errors) if league_errors else None]
        ).collect()

        return summary

    except Exception as e:
        error_msg = str(e)
        stack_trace = traceback.format_exc()
        try:
            session.sql(
                "INSERT INTO SNOWGOAL_DB.COMMON.PIPELINE_LOGS (LEVEL, COMPONENT_NAME, MESSAGE, STACK_TRACE) VALUES (?, ?, ?, ?)",
                params=['ERROR', COMPONENT_NAME, f"CRITICAL FAILURE: {error_msg}", stack_trace]
            ).collect()
        except:
            pass
        return f"CRITICAL ERROR: {error_msg}"