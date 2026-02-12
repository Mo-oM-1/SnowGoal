"""
SnowGoal - Top Scorers Page
"""

import streamlit as st
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="Top Scorers | SnowGoal", page_icon="🎯", layout="wide")

st.title("🎯 Top Scorers")

LEAGUE_NAMES = {
    'PL': 'Premier League',
    'PD': 'La Liga',
    'BL1': 'Bundesliga',
    'SA': 'Serie A',
    'FL1': 'Ligue 1'
}

try:
    session = get_active_session()

    competitions = session.sql("""
        SELECT DISTINCT COMPETITION_CODE
        FROM GOLD.DT_TOP_SCORERS
        ORDER BY COMPETITION_CODE
    """).collect()

    comp_codes = [row['COMPETITION_CODE'] for row in competitions]
    comp_options = {LEAGUE_NAMES.get(code, code): code for code in comp_codes}

    if comp_options:
        selected_comp = st.selectbox("Select League", list(comp_options.keys()))
        comp_code = comp_options[selected_comp]

        scorers_df = session.sql(f"""
            SELECT
                GOALS_RANK AS RANK,
                PLAYER_NAME,
                TEAM_NAME,
                NATIONALITY,
                AGE,
                GOALS,
                ASSISTS,
                PENALTIES,
                PLAYED_MATCHES,
                GOAL_CONTRIBUTIONS,
                GOALS_PER_MATCH
            FROM GOLD.DT_TOP_SCORERS
            WHERE COMPETITION_CODE = '{comp_code}'
            ORDER BY GOALS DESC
            LIMIT 20
        """).to_pandas()

        if not scorers_df.empty:
            # Top 3 podium
            st.subheader("🏆 Top 3")
            cols = st.columns(3)
            medals = ["🥇", "🥈", "🥉"]

            for i, col in enumerate(cols):
                if i < len(scorers_df):
                    player = scorers_df.iloc[i]
                    with col:
                        st.markdown(f"### {medals[i]} {player['PLAYER_NAME']}")
                        st.markdown(f"**{player['TEAM_NAME']}**")
                        st.markdown(f"⚽ **{int(player['GOALS'])} goals** | 🅰️ {int(player['ASSISTS'] or 0)} assists")

            st.divider()

            # Full table
            st.subheader("📋 Full Rankings")
            st.dataframe(scorers_df, use_container_width=True)

        else:
            st.warning("No scorer data available.")
    else:
        st.info("No data found. Run the pipeline first.")

except Exception as e:
    st.error(f"Could not connect to Snowflake: {e}")
    st.info("This dashboard requires Streamlit in Snowflake.")
