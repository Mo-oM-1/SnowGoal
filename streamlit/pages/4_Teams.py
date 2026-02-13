"""
SnowGoal - Teams Page
"""

import streamlit as st
import sys
sys.path.append('..')
from connection import run_query

st.set_page_config(page_title="Teams | SnowGoal", page_icon="🏟️", layout="wide")

st.title("🏟️ Teams")

LEAGUE_NAMES = {
    'PL': 'Premier League',
    'PD': 'La Liga',
    'BL1': 'Bundesliga',
    'SA': 'Serie A',
    'FL1': 'Ligue 1'
}

try:
    teams_df = run_query("""
        SELECT
            TEAM_ID,
            TEAM_NAME,
            TEAM_TLA,
            COMPETITION_CODE,
            VENUE,
            FOUNDED,
            CLUB_COLORS,
            COACH_NAME
        FROM SILVER.TEAMS
        ORDER BY TEAM_NAME
    """)

    if not teams_df.empty:
        # Competition filter with real names
        comp_codes = list(teams_df['COMPETITION_CODE'].unique())
        comp_options = {LEAGUE_NAMES.get(c, c): c for c in comp_codes}
        comp_display = ['All'] + list(comp_options.keys())
        selected_display = st.selectbox("Filter by League", comp_display)

        if selected_display != 'All':
            selected_comp = comp_options[selected_display]
            teams_df = teams_df[teams_df['COMPETITION_CODE'] == selected_comp]

        # Team selector
        team_names = teams_df['TEAM_NAME'].tolist()
        selected_team = st.selectbox("Select Team", team_names)

        team = teams_df[teams_df['TEAM_NAME'] == selected_team].iloc[0]
        team_id = int(team['TEAM_ID'])

        st.divider()

        # Team info
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"## {team['TEAM_NAME']} ({team['TEAM_TLA']})")
            st.markdown(f"**League:** {LEAGUE_NAMES.get(team['COMPETITION_CODE'], team['COMPETITION_CODE'])}")
            if team['FOUNDED']:
                st.markdown(f"**Founded:** {int(team['FOUNDED'])}")
            if team['CLUB_COLORS']:
                st.markdown(f"**Colors:** {team['CLUB_COLORS']}")

        with col2:
            st.markdown("### 📍 Venue")
            st.markdown(f"{team['VENUE'] or 'N/A'}")
            st.markdown("### 👔 Coach")
            st.markdown(f"{team['COACH_NAME'] or 'N/A'}")

        st.divider()

        # Team stats
        st.subheader("📊 Season Statistics")

        stats_df = run_query(f"""
            SELECT *
            FROM GOLD.DT_TEAM_STATS
            WHERE TEAM_ID = {team_id}
        """)

        if not stats_df.empty:
            stats = stats_df.iloc[0]

            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("🏠 Home Wins", int(stats['HOME_WINS']))
            with col2:
                st.metric("✈️ Away Wins", int(stats['AWAY_WINS']))
            with col3:
                st.metric("⚽ Goals Scored", int(stats['TOTAL_GOALS_FOR']))
            with col4:
                st.metric("🥅 Goals Conceded", int(stats['TOTAL_GOALS_AGAINST']))
        else:
            st.info("No statistics available for this team.")

    else:
        st.info("No team data found. Run the pipeline first.")

except Exception as e:
    st.error(f"Could not connect to Snowflake: {e}")
    st.info("Check your connection settings.")
