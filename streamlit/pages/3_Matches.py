"""
SnowGoal - Matches Page
"""

import streamlit as st
import sys
sys.path.append('..')
from connection import run_query

st.set_page_config(page_title="Matches | SnowGoal", page_icon="📅", layout="wide")

st.title("📅 Matches")

LEAGUE_NAMES = {
    'PL': 'Premier League',
    'PD': 'La Liga',
    'BL1': 'Bundesliga',
    'SA': 'Serie A',
    'FL1': 'Ligue 1',
    'CL': 'Champions League',
    'WC': 'World Cup',
    'EC': 'European Championship',
    'PPL': 'Primeira Liga',
    'DED': 'Eredivisie',
    'ELC': 'Championship',
    'BSA': 'Brasileirão'
}

try:
    # League filter
    comp_codes = ['All', 'PL', 'PD', 'BL1', 'SA', 'FL1', 'CL', 'WC', 'EC', 'PPL', 'DED', 'ELC', 'BSA']
    comp_display = ['All Leagues'] + [LEAGUE_NAMES.get(c, c) for c in comp_codes[1:]]
    selected_idx = st.selectbox("Filter by League", range(len(comp_display)), format_func=lambda x: comp_display[x])
    selected_comp = comp_codes[selected_idx]

    where_clause = f"AND COMPETITION_CODE = '{selected_comp}'" if selected_comp != 'All' else ""

    tab1, tab2 = st.tabs(["Recent Results", "Upcoming"])

    with tab1:
        st.subheader("Recent Results")

        recent_df = run_query(f"""
            SELECT
                MATCH_DATE,
                COMPETITION_CODE,
                MATCHDAY,
                HOME_TEAM_NAME,
                SCORE_DISPLAY,
                AWAY_TEAM_NAME,
                RESULT_DISPLAY
            FROM GOLD.RECENT_MATCHES
            WHERE STATUS = 'FINISHED' {where_clause}
            ORDER BY MATCH_DATE DESC
            LIMIT 30
        """)

        if not recent_df.empty:
            recent_df['LEAGUE'] = recent_df['COMPETITION_CODE'].map(LEAGUE_NAMES)
            st.dataframe(recent_df[['MATCH_DATE', 'LEAGUE', 'MATCHDAY', 'HOME_TEAM_NAME', 'SCORE_DISPLAY', 'AWAY_TEAM_NAME']], use_container_width=True)
        else:
            st.info("No recent matches found.")

    with tab2:
        st.subheader("Upcoming Fixtures")

        upcoming_df = run_query(f"""
            SELECT
                MATCH_DATETIME_DISPLAY,
                COMPETITION_CODE,
                MATCHDAY,
                HOME_TEAM_NAME,
                AWAY_TEAM_NAME,
                DAYS_UNTIL
            FROM GOLD.UPCOMING_FIXTURES
            WHERE 1=1 {where_clause}
            ORDER BY MATCH_DATE
            LIMIT 30
        """)

        if not upcoming_df.empty:
            upcoming_df['LEAGUE'] = upcoming_df['COMPETITION_CODE'].map(LEAGUE_NAMES)
            st.dataframe(upcoming_df[['MATCH_DATETIME_DISPLAY', 'LEAGUE', 'MATCHDAY', 'HOME_TEAM_NAME', 'AWAY_TEAM_NAME', 'DAYS_UNTIL']], use_container_width=True)
        else:
            st.info("No upcoming fixtures found.")

except Exception as e:
    st.error(f"Could not connect to Snowflake: {e}")
    st.info("Check your connection settings.")
