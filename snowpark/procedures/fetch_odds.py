"""
SnowGoal - Fetch Betting Odds
Fetches odds for all 5 European leagues from The Odds API
"""

import snowflake.snowpark as snowpark
import requests
import json
import _snowflake

BASE_URL = "https://api.the-odds-api.com/v4/sports"

# League mapping: The Odds API sport keys -> our competition codes
LEAGUES = {
    'soccer_epl': 'PL',
    'soccer_spain_la_liga': 'PD',
    'soccer_germany_bundesliga': 'BL1',
    'soccer_italy_serie_a': 'SA',
    'soccer_france_ligue_one': 'FL1'
}

def escape_sql(s):
    if s is None:
        return ""
    return str(s).replace("\\", "\\\\").replace("'", "''")

def fetch_odds_for_league(sport_key, api_key):
    url = f"{BASE_URL}/{sport_key}/odds"
    params = {
        'apiKey': api_key,
        'regions': 'eu',
        'markets': 'h2h',
        'oddsFormat': 'decimal'
    }
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()

def main(session: snowpark.Session) -> str:
    api_key = _snowflake.get_generic_secret_string('odds_api_key')

    total_records = 0

    for sport_key, competition_code in LEAGUES.items():
        try:
            games = fetch_odds_for_league(sport_key, api_key)

            for game in games:
                # Insert raw game data with all bookmakers
                json_str = escape_sql(json.dumps(game))
                session.sql(f"""
                    INSERT INTO RAW.RAW_ODDS (COMPETITION_CODE, RAW_DATA)
                    SELECT '{competition_code}', PARSE_JSON('{json_str}')
                """).collect()
                total_records += 1

        except Exception as e:
            # Continue with other leagues if one fails
            pass

    return f"SUCCESS: {len(LEAGUES)} leagues | Odds records: {total_records}"
