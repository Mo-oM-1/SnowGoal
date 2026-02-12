"""
SnowGoal - Matches Page (Results & Fixtures)
"""

import streamlit as st
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="Matches | SnowGoal", page_icon="📅", layout="wide")

st.title("📅 Matches")

try:
    session = get_active_session()

    tab1, tab2 = st.tabs(["🔴 Recent Results", "📆 Upcoming Fixtures"])

    with tab1:
        st.subheader("Recent Results (Last 30 Days)")

        # Get recent matches
        recent_df = session.sql("""
            SELECT
                MATCH_DATE,
                COMPETITION_CODE,
                MATCHDAY,
                HOME_TEAM_NAME,
                HOME_TEAM_TLA,
                SCORE_DISPLAY,
                AWAY_TEAM_TLA,
                AWAY_TEAM_NAME,
                RESULT_DISPLAY,
                REFEREE_NAME
            FROM GOLD.DT_RECENT_MATCHES
            WHERE STATUS = 'FINISHED'
            ORDER BY MATCH_DATE DESC
            LIMIT 50
        """).to_pandas()

        if not recent_df.empty:
            # Filter by competition
            comps = ['All'] + list(recent_df['COMPETITION_CODE'].unique())
            selected = st.selectbox("Filter by League", comps, key="recent_comp")

            if selected != 'All':
                recent_df = recent_df[recent_df['COMPETITION_CODE'] == selected]

            # Display results
            for _, match in recent_df.iterrows():
                col1, col2, col3 = st.columns([3, 2, 3])
                with col1:
                    st.markdown(f"**{match['HOME_TEAM_NAME']}**")
                with col2:
                    st.markdown(f"<h3 style='text-align:center'>{match['SCORE_DISPLAY']}</h3>", unsafe_allow_html=True)
                with col3:
                    st.markdown(f"**{match['AWAY_TEAM_NAME']}**")
                st.caption(f"📅 {match['MATCH_DATE']} | 🏆 {match['COMPETITION_CODE']} | Matchday {match['MATCHDAY']}")
                st.divider()
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
                HOME_TEAM_TLA,
                AWAY_TEAM_TLA,
                AWAY_TEAM_NAME,
                DAYS_UNTIL,
                HOURS_UNTIL
            FROM GOLD.DT_UPCOMING_FIXTURES
            ORDER BY MATCH_DATE
            LIMIT 30
        """).to_pandas()

        if not upcoming_df.empty:
            # Filter
            comps = ['All'] + list(upcoming_df['COMPETITION_CODE'].unique())
            selected = st.selectbox("Filter by League", comps, key="upcoming_comp")

            if selected != 'All':
                upcoming_df = upcoming_df[upcoming_df['COMPETITION_CODE'] == selected]

            # Display fixtures
            for _, match in upcoming_df.iterrows():
                col1, col2, col3 = st.columns([3, 2, 3])
                with col1:
                    st.markdown(f"**{match['HOME_TEAM_NAME']}**")
                with col2:
                    st.markdown(f"<p style='text-align:center'>vs</p>", unsafe_allow_html=True)
                with col3:
                    st.markdown(f"**{match['AWAY_TEAM_NAME']}**")

                days = int(match['DAYS_UNTIL'])
                if days == 0:
                    time_str = f"⏰ In {int(match['HOURS_UNTIL'])} hours"
                elif days == 1:
                    time_str = "📅 Tomorrow"
                else:
                    time_str = f"📅 In {days} days"

                st.caption(f"{time_str} | {match['MATCH_DATETIME_DISPLAY']} | 🏆 {match['COMPETITION_CODE']}")
                st.divider()
        else:
            st.info("No upcoming fixtures found.")

except Exception as e:
    st.error(f"Could not connect to Snowflake: {e}")
    st.info("This dashboard requires Streamlit in Snowflake.")
