"""
SnowGoal - Match Analytics Page
"""

import streamlit as st
import sys
import pandas as pd
sys.path.append('..')
from connection import run_query

st.set_page_config(page_title="Analytics | SnowGoal", page_icon="📊", layout="wide")

st.title("📊 Match Analytics")

# Tabs for different analyses
tab1, tab2, tab3 = st.tabs(["⏰ Time Patterns", "👔 Referee Stats", "🌍 Geographic Analysis"])

# ============================================
# TAB 1: Time Patterns
# ============================================
with tab1:
    st.header("⏰ Match Time Patterns")

    try:
        # Weekend vs Midweek analysis
        st.subheader("📅 Weekend vs Midweek Performance")

        day_stats = run_query("""
            SELECT
                CASE
                    WHEN DAY_OF_WEEK IN ('Saturday', 'Sunday') THEN 'Weekend'
                    ELSE 'Midweek'
                END AS PERIOD,
                COUNT(*) AS MATCHES,
                ROUND(AVG(HOME_SCORE + AWAY_SCORE), 2) AS AVG_GOALS,
                ROUND(100.0 * SUM(CASE WHEN WINNER = 'HOME_TEAM' THEN 1 ELSE 0 END) / COUNT(*), 1) AS HOME_WIN_PCT
            FROM SILVER.MATCHES
            WHERE STATUS = 'FINISHED' AND DAY_OF_WEEK IS NOT NULL
            GROUP BY PERIOD
        """)

        if not day_stats.empty:
            col1, col2 = st.columns(2)
            for i, row in day_stats.iterrows():
                with col1 if i == 0 else col2:
                    st.metric(
                        label=f"{row['PERIOD']} Matches",
                        value=f"{int(row['MATCHES'])} matches",
                        delta=f"{row['AVG_GOALS']} avg goals"
                    )
                    st.caption(f"Home win rate: {row['HOME_WIN_PCT']}%")

        st.divider()

        # Goals by hour
        st.subheader("⚽ Goals by Match Hour")

        hour_stats = run_query("""
            SELECT
                MATCH_HOUR,
                COUNT(*) AS MATCHES,
                ROUND(AVG(HOME_SCORE + AWAY_SCORE), 2) AS AVG_GOALS
            FROM SILVER.MATCHES
            WHERE STATUS = 'FINISHED'
              AND MATCH_HOUR IS NOT NULL
            GROUP BY MATCH_HOUR
            ORDER BY MATCH_HOUR
        """)

        if not hour_stats.empty:
            st.bar_chart(hour_stats.set_index('MATCH_HOUR')['AVG_GOALS'])
            st.dataframe(hour_stats, use_container_width=True)

        st.divider()

        # Day of week distribution
        st.subheader("📆 Matches by Day of Week")

        dow_stats = run_query("""
            SELECT
                DAY_OF_WEEK,
                COUNT(*) AS MATCHES,
                ROUND(AVG(HOME_SCORE + AWAY_SCORE), 2) AS AVG_GOALS
            FROM SILVER.MATCHES
            WHERE STATUS = 'FINISHED' AND DAY_OF_WEEK IS NOT NULL
            GROUP BY DAY_OF_WEEK
            ORDER BY
                CASE DAY_OF_WEEK
                    WHEN 'Monday' THEN 1
                    WHEN 'Tuesday' THEN 2
                    WHEN 'Wednesday' THEN 3
                    WHEN 'Thursday' THEN 4
                    WHEN 'Friday' THEN 5
                    WHEN 'Saturday' THEN 6
                    WHEN 'Sunday' THEN 7
                END
        """)

        if not dow_stats.empty:
            st.bar_chart(dow_stats.set_index('DAY_OF_WEEK')['MATCHES'])
            st.dataframe(dow_stats, use_container_width=True)

    except Exception as e:
        st.error(f"Error loading time patterns: {e}")

# ============================================
# TAB 2: Referee Stats
# ============================================
with tab2:
    st.header("👔 Referee Statistics")

    try:
        # Top referees by matches
        st.subheader("🏅 Most Active Referees")

        top_refs = run_query("""
            SELECT
                REFEREE_NAME,
                REFEREE_NATIONALITY,
                MATCHES_REFEREED,
                COMPETITIONS,
                AVG_GOALS_PER_MATCH,
                EXTRA_TIME_MATCHES,
                EXTRA_TIME_PCT
            FROM GOLD.REFEREE_STATS
            ORDER BY MATCHES_REFEREED DESC
            LIMIT 20
        """)

        if not top_refs.empty:
            st.dataframe(top_refs, use_container_width=True)

        st.divider()

        # Referee nationality distribution
        st.subheader("🌍 Referees by Nationality")

        ref_nationality = run_query("""
            SELECT
                REFEREE_NATIONALITY,
                COUNT(*) AS REFEREES,
                SUM(MATCHES_REFEREED) AS TOTAL_MATCHES
            FROM GOLD.REFEREE_STATS
            WHERE REFEREE_NATIONALITY IS NOT NULL
            GROUP BY REFEREE_NATIONALITY
            ORDER BY TOTAL_MATCHES DESC
            LIMIT 15
        """)

        if not ref_nationality.empty:
            st.bar_chart(ref_nationality.set_index('REFEREE_NATIONALITY')['TOTAL_MATCHES'])
            st.dataframe(ref_nationality, use_container_width=True)

        st.divider()

        # Extra time specialists
        st.subheader("⏱️ Extra Time Specialists")

        extra_time_refs = run_query("""
            SELECT
                REFEREE_NAME,
                REFEREE_NATIONALITY,
                MATCHES_REFEREED,
                EXTRA_TIME_MATCHES,
                EXTRA_TIME_PCT
            FROM GOLD.REFEREE_STATS
            WHERE MATCHES_REFEREED >= 10
            ORDER BY EXTRA_TIME_PCT DESC
            LIMIT 10
        """)

        if not extra_time_refs.empty:
            st.dataframe(extra_time_refs, use_container_width=True)

    except Exception as e:
        st.error(f"Error loading referee stats: {e}")

# ============================================
# TAB 3: Geographic Analysis
# ============================================
with tab3:
    st.header("🌍 Geographic Analysis")

    try:
        # Stats by country/area
        st.subheader("📍 Matches by Country")

        geo_stats = run_query("""
            SELECT
                AREA_NAME,
                AREA_CODE,
                COMPETITIONS,
                TOTAL_MATCHES,
                AVG_GOALS_PER_MATCH,
                HOME_WIN_PCT
            FROM GOLD.GEOGRAPHIC_STATS
            ORDER BY TOTAL_MATCHES DESC
        """)

        if not geo_stats.empty:
            st.dataframe(geo_stats, use_container_width=True)

        st.divider()

        # Home advantage by country
        st.subheader("🏠 Home Advantage by Country")

        home_adv = run_query("""
            SELECT
                AREA_NAME,
                TOTAL_MATCHES,
                HOME_WIN_PCT
            FROM GOLD.GEOGRAPHIC_STATS
            WHERE TOTAL_MATCHES >= 50
            ORDER BY HOME_WIN_PCT DESC
            LIMIT 10
        """)

        if not home_adv.empty:
            st.bar_chart(home_adv.set_index('AREA_NAME')['HOME_WIN_PCT'])
            st.caption("Countries with strongest home advantage")

    except Exception as e:
        st.error(f"Error loading geographic stats: {e}")
