"""
SnowGoal - League Standings Page
"""

import streamlit as st
import sys
sys.path.append('..')
from connection import run_query

st.set_page_config(page_title="Standings | SnowGoal", page_icon="🏆", layout="wide")

st.title("🏆 League Standings")

# League names mapping
LEAGUE_NAMES = {
    'PL': 'Premier League',
    'PD': 'La Liga',
    'BL1': 'Bundesliga',
    'SA': 'Serie A',
    'FL1': 'Ligue 1'
}

try:
    # Get available competitions
    competitions = run_query("""
        SELECT DISTINCT COMPETITION_CODE
        FROM GOLD.LEAGUE_STANDINGS
        ORDER BY COMPETITION_CODE
    """)

    comp_codes = list(competitions['COMPETITION_CODE'])
    comp_options = {LEAGUE_NAMES.get(code, code): code for code in comp_codes}

    if comp_options:
        selected_comp = st.selectbox(
            "Select League",
            options=list(comp_options.keys()),
            index=0
        )
        comp_code = comp_options[selected_comp]

        # Get standings
        standings_df = run_query(f"""
            SELECT
                POSITION,
                TEAM_NAME,
                TEAM_TLA,
                PLAYED,
                WON,
                DRAW,
                LOST,
                GOALS_FOR,
                GOALS_AGAINST,
                GOAL_DIFF,
                POINTS,
                FORM,
                POINTS_PER_GAME,
                WIN_PERCENTAGE
            FROM GOLD.LEAGUE_STANDINGS
            WHERE COMPETITION_CODE = '{comp_code}'
            ORDER BY POSITION
        """)

        if not standings_df.empty:
            # KPIs
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                leader = standings_df.iloc[0]
                st.metric("🥇 Leader", leader['TEAM_NAME'], f"{int(leader['POINTS'])} pts")
            with col2:
                total_goals = int(standings_df['GOALS_FOR'].sum())
                st.metric("⚽ Total Goals", total_goals)
            with col3:
                avg_ppg = standings_df['POINTS_PER_GAME'].mean()
                st.metric("📊 Avg PPG", f"{avg_ppg:.2f}")
            with col4:
                matchday = int(standings_df['PLAYED'].max())
                st.metric("📅 Matchday", matchday)

            st.divider()

            # Standings table
            st.dataframe(standings_df, use_container_width=True)

            # Points chart
            st.subheader("📊 Points Distribution")
            st.bar_chart(standings_df.set_index('TEAM_TLA')['POINTS'])

        else:
            st.warning("No standings data available.")
    else:
        st.info("No competition data found. Run the data pipeline first.")

except Exception as e:
    st.error(f"Could not connect to Snowflake: {e}")
    st.info("Check your connection settings.")
