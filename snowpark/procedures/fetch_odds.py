"""
SnowGoal - Fetch Betting Odds
Version: 2.0 - Production Ready
Features: Rate Limiting, Batch Insert, Centralized Logging & Intelligent Status
"""

import snowflake.snowpark as snowpark
import pandas as pd
import requests
import json
import time
import _snowflake
import traceback

BASE_URL = "https://api.the-odds-api.com/v4/sports"

# DICTIONNAIRE OFFICIEL ET VÉRIFIÉ (The Odds API)
LEAGUES = {
    'soccer_epl': 'PL',
    'soccer_spain_la_liga': 'PD',
    'soccer_germany_bundesliga': 'BL1',
    'soccer_italy_serie_a': 'SA',
    'soccer_france_ligue_one': 'FL1',
    'soccer_uefa_champs_league': 'CL',
    'soccer_uefa_european_championship': 'EC',
    'soccer_portugal_primeira_liga': 'PPL',
    'soccer_netherlands_eredivisie': 'DED',
    'soccer_efl_champ': 'ELC',      # Corrigé : soccer_efl_champ
    'soccer_brazil_campeonato': 'BSA'
}

# Respect des limites de l'API Free Tier (3 secondes entre chaque appel)
RATE_LIMIT_DELAY = 3 

def fetch_odds_for_league(sport_key, api_key):
    """Effectue l'appel API pour une ligue spécifique"""
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
    COMPONENT_NAME = 'FETCH_ODDS'
    
    try:
        # Récupération sécurisée de la clé API depuis Snowflake Secrets
        api_key = _snowflake.get_generic_secret_string('odds_api_key')
        
        all_rows = []
        errors = []
        
        # 1. BOUCLE DE RÉCUPÉRATION (Avec Rate Limiting)
        for i, (sport_key, competition_code) in enumerate(LEAGUES.items()):
            try:
                if i > 0:
                    time.sleep(RATE_LIMIT_DELAY)
                
                games = fetch_odds_for_league(sport_key, api_key)
                
                for game in games:
                    all_rows.append({
                        "COMPETITION_CODE": competition_code,
                        "RAW_DATA": json.dumps(game)
                    })
                    
            except Exception as e:
                # On log l'erreur spécifique à la ligue mais on continue le traitement
                errors.append(f"{competition_code}: {str(e)}")

        # 2. BATCH INSERT (Optimisation des performances via Snowpark)
        inserted_count = 0
        if all_rows:
            inserted_count = len(all_rows)
            df = pd.DataFrame(all_rows)
            
            # Utilisation d'une table temporaire pour un chargement rapide
            temp_table = "TEMP_RAW_ODDS"
            session.write_pandas(df, temp_table, auto_create_table=True, overwrite=True, table_type="temp", quote_identifiers=False)
            
            # Insertion finale avec conversion VARIANT
            session.sql(f"""
                INSERT INTO RAW.RAW_ODDS (COMPETITION_CODE, RAW_DATA)
                SELECT COMPETITION_CODE, PARSE_JSON(RAW_DATA)
                FROM {temp_table}
            """).collect()

        # 3. LOGIQUE DE STATUT (Success / Partial Success / Failed)
        num_leagues = len(LEAGUES)
        num_errors = len(errors)
        
        if num_errors == 0:
            status = "SUCCESS"
            log_level = 'INFO'
        elif num_errors < num_leagues:
            status = "PARTIAL SUCCESS"
            log_level = 'WARNING'
        else:
            status = "FAILED"
            log_level = 'ERROR'

        summary = f"{status}: {num_leagues - num_errors}/{num_leagues} leagues loaded | Odds: {inserted_count}"
        
        if errors:
            summary += f" | {num_errors} error(s) detected"

        # 4. LOGGING CENTRALISÉ (Utilisation de Parameter Binding pour la sécurité)
        session.sql(
            "INSERT INTO SNOWGOAL_DB.COMMON.PIPELINE_LOGS (LEVEL, COMPONENT_NAME, MESSAGE, STACK_TRACE) VALUES (?, ?, ?, ?)",
            params=[log_level, COMPONENT_NAME, summary, "; ".join(errors) if errors else None]
        ).collect()

        return summary

    except Exception as e:
        # Erreur critique (ex: échec de connexion, droits insuffisants)
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