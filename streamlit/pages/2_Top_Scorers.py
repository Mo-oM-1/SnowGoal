"""
SnowGoal - Top Scorers Page
"""

import streamlit as st
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="Top Scorers | SnowGoal", page_icon="🎯", layout="wide")

st.title("🎯 Top Scorers")

try:
    session = get_active_session()

    # Competition selector
    competitions = session.sql("""
        SELECT DISTINCT COMPETITION_CODE
        FROM GOLD.DT_TOP_SCORERS
        ORDER BY COMPETITION_CODE
    """).collect()

    comp_codes = [row['COMPETITION_CODE'] for row in competitions]

    if comp_codes:
        selected_comp = st.selectbox("Select League", comp_codes)

        # Get scorers
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
                GOALS_PER_MATCH,
                CONTRIBUTIONS_PER_MATCH
            FROM GOLD.DT_TOP_SCORERS
            WHERE COMPETITION_CODE = '{selected_comp}'
            ORDER BY GOALS DESC
            LIMIT 25
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
                        st.markdown(f"""
                        <div style="text-align: center; padding: 1rem; background: #f8f9fa; border-radius: 10px;">
                            <h1>{medals[i]}</h1>
                            <h3>{player['PLAYER_NAME']}</h3>
                            <p>{player['TEAM_NAME']}</p>
                            <h2>{int(player['GOALS'])} goals</h2>
                            <p>{int(player['ASSISTS'] or 0)} assists</p>
                        </div>
                        """, unsafe_allow_html=True)

            st.divider()

            # Full table
            st.subheader("📋 Full Rankings")
            st.dataframe(
                scorers_df,
                column_config={
                    "RANK": st.column_config.NumberColumn("#", width="small"),
                    "PLAYER_NAME": st.column_config.TextColumn("Player", width="medium"),
                    "TEAM_NAME": st.column_config.TextColumn("Team", width="medium"),
                    "NATIONALITY": st.column_config.TextColumn("🌍", width="small"),
                    "AGE": st.column_config.NumberColumn("Age", width="small"),
                    "GOALS": st.column_config.NumberColumn("⚽", width="small"),
                    "ASSISTS": st.column_config.NumberColumn("🅰️", width="small"),
                    "PENALTIES": st.column_config.NumberColumn("🎯", width="small"),
                    "PLAYED_MATCHES": st.column_config.NumberColumn("Apps", width="small"),
                    "GOAL_CONTRIBUTIONS": st.column_config.NumberColumn("G+A", width="small"),
                    "GOALS_PER_MATCH": st.column_config.NumberColumn("G/M", format="%.2f", width="small"),
                    "CONTRIBUTIONS_PER_MATCH": st.column_config.NumberColumn("(G+A)/M", format="%.2f", width="small")
                },
                hide_index=True,
                use_container_width=True
            )

            # Goals vs Assists scatter
            st.subheader("📈 Goals vs Assists")
            import plotly.express as px

            fig = px.scatter(
                scorers_df,
                x="GOALS",
                y="ASSISTS",
                size="PLAYED_MATCHES",
                color="TEAM_NAME",
                hover_name="PLAYER_NAME",
                title="Player Performance"
            )
            st.plotly_chart(fig, use_container_width=True)

        else:
            st.warning("No scorer data available.")
    else:
        st.info("No data found. Run the pipeline first.")

except Exception as e:
    st.error(f"Could not connect to Snowflake: {e}")
    st.info("This dashboard requires Streamlit in Snowflake.")
