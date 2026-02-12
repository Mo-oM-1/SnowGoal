"""
SnowGoal - Matches Page
"""

import streamlit as st
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="Matches | SnowGoal", page_icon="📅", layout="wide")

st.title("📅 Matches")

LEAGUE_NAMES = {
    'PL': 'Premier League',
    'PD': 'La Liga',
    'BL1': 'Bundesliga',
    'SA': 'Serie A',
    'FL1': 'Ligue 1'
}

try:
    session = get_active_session()

    tab1, tab2 = st.tabs(["🔴 Recent Results", "📆 Upcoming"])

    with tab1:
        st.subheader("Recent Results")

        recent_df = session.sql("""
            SELECT
                MATCH_DATE,
                COMPETITION_CODE,
                MATCHDAY,
                HOME_TEAM_NAME,
                SCORE_DISPLAY,
                AWAY_TEAM_NAME,
                RESULT_DISPLAY
            FROM GOLD.DT_RECENT_MATCHES
            WHERE STATUS = 'FINISHED'
            ORDER BY MATCH_DATE DESC
            LIMIT 30
        """).to_pandas()

        if not recent_df.empty:
            st.dataframe(recent_df, use_container_width=True)
        else:
            st.info("No recent matches found.")

    with tab2:
        st.subheader("Upcoming Fixtures")

        upcoming_df = session.sql("""
            SELECT
                MATCH_DATETIME_DISPLAY,
                COMPETITION_CODE,
                MATCHDAY,
                HOME_TEAM_NAME,
                AWAY_TEAM_NAME,
                DAYS_UNTIL
            FROM GOLD.DT_UPCOMING_FIXTURES
            ORDER BY MATCH_DATE
            LIMIT 30
        """).to_pandas()

        if not upcoming_df.empty:
            st.dataframe(upcoming_df, use_container_width=True)
        else:
            st.info("No upcoming fixtures found.")

except Exception as e:
    st.error(f"Could not connect to Snowflake: {e}")
    st.info("This dashboard requires Streamlit in Snowflake.")
