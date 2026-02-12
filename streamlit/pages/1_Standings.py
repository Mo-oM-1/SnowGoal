"""
SnowGoal - League Standings Page
"""

import streamlit as st
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="Standings | SnowGoal", page_icon="🏆", layout="wide")

st.title("🏆 League Standings")

# Get Snowflake session
try:
    session = get_active_session()

    # Competition selector
    competitions = session.sql("""
        SELECT DISTINCT COMPETITION_CODE, COMPETITION_NAME
        FROM GOLD.DT_LEAGUE_STANDINGS
        ORDER BY COMPETITION_NAME
    """).collect()

    comp_options = {row['COMPETITION_NAME']: row['COMPETITION_CODE'] for row in competitions}

    if comp_options:
        selected_comp = st.selectbox(
            "Select League",
            options=list(comp_options.keys()),
            index=0
        )
        comp_code = comp_options[selected_comp]

        # Get standings
        standings_df = session.sql(f"""
            SELECT
                POSITION,
                TEAM_CREST,
                TEAM_NAME,
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
            FROM GOLD.DT_LEAGUE_STANDINGS
            WHERE COMPETITION_CODE = '{comp_code}'
            ORDER BY POSITION
        """).to_pandas()

        if not standings_df.empty:
            # KPIs
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                leader = standings_df.iloc[0]
                st.metric("🥇 Leader", leader['TEAM_NAME'], f"{leader['POINTS']} pts")
            with col2:
                total_goals = standings_df['GOALS_FOR'].sum()
                st.metric("⚽ Total Goals", total_goals)
            with col3:
                avg_ppg = standings_df['POINTS_PER_GAME'].mean()
                st.metric("📊 Avg PPG", f"{avg_ppg:.2f}")
            with col4:
                matchday = standings_df['PLAYED'].max()
                st.metric("📅 Matchday", matchday)

            st.divider()

            # Standings table
            st.dataframe(
                standings_df,
                column_config={
                    "POSITION": st.column_config.NumberColumn("Pos", width="small"),
                    "TEAM_CREST": st.column_config.ImageColumn("", width="small"),
                    "TEAM_NAME": st.column_config.TextColumn("Team", width="medium"),
                    "PLAYED": st.column_config.NumberColumn("P", width="small"),
                    "WON": st.column_config.NumberColumn("W", width="small"),
                    "DRAW": st.column_config.NumberColumn("D", width="small"),
                    "LOST": st.column_config.NumberColumn("L", width="small"),
                    "GOALS_FOR": st.column_config.NumberColumn("GF", width="small"),
                    "GOALS_AGAINST": st.column_config.NumberColumn("GA", width="small"),
                    "GOAL_DIFF": st.column_config.NumberColumn("GD", width="small"),
                    "POINTS": st.column_config.NumberColumn("Pts", width="small"),
                    "FORM": st.column_config.TextColumn("Form", width="small"),
                    "POINTS_PER_GAME": st.column_config.NumberColumn("PPG", format="%.2f", width="small"),
                    "WIN_PERCENTAGE": st.column_config.ProgressColumn("Win %", min_value=0, max_value=100, width="small")
                },
                hide_index=True,
                use_container_width=True
            )

            # Points distribution chart
            st.subheader("📊 Points Distribution")
            st.bar_chart(standings_df.set_index('TEAM_NAME')['POINTS'])

        else:
            st.warning("No standings data available for this league.")
    else:
        st.info("No competition data found. Run the data pipeline first.")

except Exception as e:
    st.error(f"Could not connect to Snowflake: {e}")
    st.info("This dashboard requires Streamlit in Snowflake (SiS) to access data.")
