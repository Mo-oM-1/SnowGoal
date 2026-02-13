"""
Lambda function to fetch betting odds from The Odds API
and write to S3 for Snowpipe ingestion
"""

import json
import boto3
import requests
from datetime import datetime
import os

# Configuration
ODDS_API_KEY = os.environ.get('ODDS_API_KEY')
S3_BUCKET = os.environ.get('S3_BUCKET', 'ai-factory-bckt')
S3_PREFIX = os.environ.get('S3_PREFIX', 'snowgoal/odds/')

# League mapping: The Odds API sport keys for our 5 leagues
LEAGUES = {
    'soccer_epl': 'PL',           # Premier League
    'soccer_spain_la_liga': 'PD', # La Liga
    'soccer_germany_bundesliga': 'BL1',  # Bundesliga
    'soccer_italy_serie_a': 'SA', # Serie A
    'soccer_france_ligue_one': 'FL1'     # Ligue 1
}

def fetch_odds_for_league(sport_key: str, competition_code: str) -> list:
    """Fetch odds for a single league"""

    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
    params = {
        'apiKey': ODDS_API_KEY,
        'regions': 'eu',
        'markets': 'h2h',  # Head to head (1X2)
        'oddsFormat': 'decimal'
    }

    response = requests.get(url, params=params)

    if response.status_code != 200:
        print(f"Error fetching {sport_key}: {response.status_code}")
        return []

    games = response.json()
    processed = []

    for game in games:
        # Extract odds from first bookmaker (or average later)
        for bookmaker in game.get('bookmakers', []):
            for market in bookmaker.get('markets', []):
                if market['key'] == 'h2h':
                    outcomes = {o['name']: o['price'] for o in market['outcomes']}

                    processed.append({
                        'game_id': game['id'],
                        'sport_key': sport_key,
                        'competition_code': competition_code,
                        'commence_time': game['commence_time'],
                        'home_team': game['home_team'],
                        'away_team': game['away_team'],
                        'bookmaker': bookmaker['key'],
                        'bookmaker_title': bookmaker['title'],
                        'home_odds': outcomes.get(game['home_team']),
                        'away_odds': outcomes.get(game['away_team']),
                        'draw_odds': outcomes.get('Draw'),
                        'last_update': bookmaker['last_update'],
                        'fetched_at': datetime.utcnow().isoformat()
                    })

    return processed

def write_to_s3(data: list, s3_client) -> str:
    """Write data to S3 as JSON"""

    if not data:
        return None

    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    filename = f"{S3_PREFIX}odds_{timestamp}.json"

    # Write as newline-delimited JSON (one record per line)
    json_lines = '\n'.join(json.dumps(record) for record in data)

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=filename,
        Body=json_lines,
        ContentType='application/json'
    )

    return filename

def lambda_handler(event, context):
    """Main Lambda handler"""

    s3_client = boto3.client('s3')
    all_odds = []

    for sport_key, competition_code in LEAGUES.items():
        print(f"Fetching odds for {sport_key} ({competition_code})")
        odds = fetch_odds_for_league(sport_key, competition_code)
        all_odds.extend(odds)
        print(f"  Found {len(odds)} odds records")

    if all_odds:
        filename = write_to_s3(all_odds, s3_client)
        message = f"Successfully wrote {len(all_odds)} records to s3://{S3_BUCKET}/{filename}"
    else:
        message = "No odds data found"

    print(message)

    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': message,
            'records': len(all_odds)
        })
    }

# For local testing
if __name__ == '__main__':
    # Set environment variables for local test
    # os.environ['ODDS_API_KEY'] = 'your-key'
    result = lambda_handler({}, None)
    print(result)
