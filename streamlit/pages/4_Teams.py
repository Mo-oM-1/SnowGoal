"""
SnowGoal - Teams Page
"""

import streamlit as st
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="Teams | SnowGoal", page_icon="🏟️", layout="wide")

st.title("🏟️ Teams")

try:
    session = get_active_session()

    # Get teams list
    teams_df = session.sql("""
        SELECT
            t.TEAM_ID,
            t.TEAM_NAME,
            t.TEAM_TLA,
            t.TEAM_CREST,
            t.COMPETITION_CODE,
            t.VENUE,
            t.FOUNDED,
            t.CLUB_COLORS,
            t.COACH_NAME,
            t.WEBSITE
        FROM SILVER.TEAMS t
        ORDER BY t.TEAM_NAME
    """).to_pandas()

    if not teams_df.empty:
        # Competition filter
        comps = ['All'] + list(teams_df['COMPETITION_CODE'].unique())
        selected_comp = st.selectbox("Filter by League", comps)

        if selected_comp != 'All':
            teams_df = teams_df[teams_df['COMPETITION_CODE'] == selected_comp]

        # Team selector
        team_names = teams_df['TEAM_NAME'].tolist()
        selected_team = st.selectbox("Select Team", team_names)

        team = teams_df[teams_df['TEAM_NAME'] == selected_team].iloc[0]
        team_id = team['TEAM_ID']

        st.divider()

        # Team header
        col1, col2 = st.columns([1, 3])
        with col1:
            if team['TEAM_CREST']:
                st.image(team['TEAM_CREST'], width=150)
        with col2:
            st.markdown(f"## {team['TEAM_NAME']}")
            st.markdown(f"**TLA:** {team['TEAM_TLA']} | **League:** {team['COMPETITION_CODE']}")
            if team['FOUNDED']:
                st.markdown(f"**Founded:** {int(team['FOUNDED'])}")
            if team['CLUB_COLORS']:
                st.markdown(f"**Colors:** {team['CLUB_COLORS']}")

        st.divider()

        # Team details
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("### 📍 Venue")
            st.markdown(f"**Stadium:** {team['VENUE'] or 'N/A'}")

            st.markdown("### 👔 Coach")
            st.markdown(f"**Manager:** {team['COACH_NAME'] or 'N/A'}")

        with col2:
            st.markdown("### 🔗 Links")
            if team['WEBSITE']:
                st.markdown(f"[Official Website]({team['WEBSITE']})")

        st.divider()

        # Team stats from DT_TEAM_STATS
        st.markdown("### 📊 Season Statistics")

        stats_df = session.sql(f"""
            SELECT *
            FROM GOLD.DT_TEAM_STATS
            WHERE TEAM_ID = {team_id}
        """).to_pandas()

        if not stats_df.empty:
            stats = stats_df.iloc[0]

            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("🏠 Home Wins", int(stats['HOME_WINS']))
                st.metric("Home PPG", f"{stats['HOME_PPG']:.2f}")
            with col2:
                st.metric("✈️ Away Wins", int(stats['AWAY_WINS']))
                st.metric("Away PPG", f"{stats['AWAY_PPG']:.2f}")
            with col3:
                st.metric("⚽ Goals Scored", int(stats['TOTAL_GOALS_FOR']))
            with col4:
                st.metric("🥅 Goals Conceded", int(stats['TOTAL_GOALS_AGAINST']))

            # Home vs Away performance
            st.markdown("#### Home vs Away Performance")
            import plotly.graph_objects as go

            fig = go.Figure(data=[
                go.Bar(name='Home', x=['Wins', 'Draws', 'Losses', 'Goals For', 'Goals Against'],
                       y=[stats['HOME_WINS'], stats['HOME_DRAWS'], stats['HOME_LOSSES'],
                          stats['HOME_GOALS_FOR'], stats['HOME_GOALS_AGAINST']]),
                go.Bar(name='Away', x=['Wins', 'Draws', 'Losses', 'Goals For', 'Goals Against'],
                       y=[stats['AWAY_WINS'], stats['AWAY_DRAWS'], stats['AWAY_LOSSES'],
                          stats['AWAY_GOALS_FOR'], stats['AWAY_GOALS_AGAINST']])
            ])
            fig.update_layout(barmode='group')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No statistics available for this team yet.")

    else:
        st.info("No team data found. Run the pipeline first.")

except Exception as e:
    st.error(f"Could not connect to Snowflake: {e}")
    st.info("This dashboard requires Streamlit in Snowflake.")
