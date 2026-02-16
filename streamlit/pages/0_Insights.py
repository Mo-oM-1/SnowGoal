"""
SnowGoal - Key Insights & Metrics
Business metrics and data-driven insights from football analytics
"""

import streamlit as st
import sys
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
sys.path.append('..')
from connection import run_query

st.set_page_config(page_title="Insights | SnowGoal", page_icon="💡", layout="wide")

st.title("💡 Key Insights & Metrics")
st.info("📅 **Season 2025-2026** | Data refreshes automatically 3x daily (7h, 17h, 00h)")

# ============================================
# Global Metrics
# ============================================
st.header("📊 Global Data Metrics")

try:
    # Get overall statistics
    global_stats = run_query("""
        SELECT
            (SELECT COUNT(*) FROM SILVER.MATCHES) AS total_matches,
            (SELECT COUNT(DISTINCT PLAYER_ID) FROM SILVER.SCORERS) AS total_players,
            (SELECT COUNT(DISTINCT TEAM_ID) FROM SILVER.TEAMS) AS total_teams,
            (SELECT COUNT(DISTINCT COMPETITION_CODE) FROM SILVER.COMPETITIONS) AS total_competitions,
            (SELECT COUNT(*) FROM SILVER.MATCHES WHERE STATUS = 'FINISHED') AS finished_matches,
            (SELECT SUM(HOME_SCORE + AWAY_SCORE) FROM SILVER.MATCHES WHERE STATUS = 'FINISHED') AS total_goals
    """)

    if not global_stats.empty:
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("📅 Total Matches", f"{global_stats['TOTAL_MATCHES'].iloc[0]:,}")
            st.metric("⚽ Total Goals Scored", f"{int(global_stats['TOTAL_GOALS'].iloc[0]):,}")

        with col2:
            st.metric("👥 Players Tracked", f"{global_stats['TOTAL_PLAYERS'].iloc[0]:,}")
            st.metric("🏆 Competitions", f"{global_stats['TOTAL_COMPETITIONS'].iloc[0]}")

        with col3:
            st.metric("🔵 Teams", f"{global_stats['TOTAL_TEAMS'].iloc[0]:,}")
            st.metric("✅ Finished Matches", f"{global_stats['FINISHED_MATCHES'].iloc[0]:,}")

    st.divider()

    # ============================================
    # Key Insights
    # ============================================
    st.header("🔍 Data-Driven Insights")

    # Insight 1: High-scoring matches
    st.subheader("Match Excitement Analysis")

    high_scoring = run_query("""
        SELECT
            COUNT(*) as total_matches,
            SUM(CASE WHEN HOME_SCORE + AWAY_SCORE >= 4 THEN 1 ELSE 0 END) as high_scoring,
            ROUND(100.0 * high_scoring / total_matches, 1) as high_scoring_pct,
            ROUND(AVG(ABS(HOME_SCORE - AWAY_SCORE)), 2) as avg_goal_difference
        FROM SILVER.MATCHES
        WHERE STATUS = 'FINISHED'
    """)

    if not high_scoring.empty:
        hs_pct = high_scoring['HIGH_SCORING_PCT'].iloc[0]
        avg_diff = high_scoring['AVG_GOAL_DIFFERENCE'].iloc[0]
        st.success(f"⚡ **Insight:** **{hs_pct}%** of matches had 4+ goals (high-scoring). Average goal difference: **{avg_diff} goals**")

    # Insight 2: Comeback Analysis
    st.subheader("Comeback Frequency")

    comebacks = run_query("""
        SELECT
            COUNT(*) as total_matches,
            SUM(CASE WHEN (HOME_SCORE_HT < AWAY_SCORE_HT AND WINNER = 'HOME_TEAM')
                       OR (HOME_SCORE_HT > AWAY_SCORE_HT AND WINNER = 'AWAY_TEAM')
                THEN 1 ELSE 0 END) as comebacks,
            ROUND(100.0 * comebacks / total_matches, 1) as comeback_pct
        FROM SILVER.MATCHES
        WHERE STATUS = 'FINISHED'
          AND HOME_SCORE_HT IS NOT NULL
          AND WINNER IN ('HOME_TEAM', 'AWAY_TEAM')
    """)

    if not comebacks.empty:
        cb_pct = comebacks['COMEBACK_PCT'].iloc[0]
        cb_count = comebacks['COMEBACKS'].iloc[0]
        st.success(f"🎢 **Insight:** **{cb_pct}%** of matches featured a comeback ({int(cb_count):,} matches - losing at HT, winning at FT)")

    # Insight 3: When are goals scored?
    st.subheader("Goal Distribution by Half")

    half_goals = run_query("""
        SELECT
            SUM(HOME_SCORE_HT + AWAY_SCORE_HT) as first_half_goals,
            SUM((HOME_SCORE - HOME_SCORE_HT) + (AWAY_SCORE - AWAY_SCORE_HT)) as second_half_goals,
            ROUND(100.0 * second_half_goals / (first_half_goals + second_half_goals), 1) as second_half_pct
        FROM SILVER.MATCHES
        WHERE STATUS = 'FINISHED'
          AND HOME_SCORE_HT IS NOT NULL
          AND HOME_SCORE IS NOT NULL
    """)

    if not half_goals.empty:
        sh_pct = half_goals['SECOND_HALF_PCT'].iloc[0]
        sh_goals = int(half_goals['SECOND_HALF_GOALS'].iloc[0])
        fh_goals = int(half_goals['FIRST_HALF_GOALS'].iloc[0])
        st.success(f"⏱️ **Insight:** **{sh_pct}%** of goals are scored in the 2nd half ({sh_goals:,} vs {fh_goals:,} in 1st half)")

    # Insight 2: Home advantage by day
    st.subheader("Home Advantage Analysis")

    home_advantage = run_query("""
        SELECT
            DAY_OF_WEEK,
            COUNT(*) AS matches,
            ROUND(100.0 * SUM(CASE WHEN WINNER = 'HOME_TEAM' THEN 1 ELSE 0 END) / COUNT(*), 1) AS home_win_pct
        FROM SILVER.MATCHES
        WHERE STATUS = 'FINISHED'
          AND DAY_OF_WEEK IS NOT NULL
          AND WINNER IS NOT NULL
        GROUP BY DAY_OF_WEEK
        ORDER BY home_win_pct DESC
        LIMIT 1
    """)

    if not home_advantage.empty:
        best_day = home_advantage['DAY_OF_WEEK'].iloc[0]
        home_pct = home_advantage['HOME_WIN_PCT'].iloc[0]
        st.success(f"🏆 **Insight:** Home teams win most on **{best_day}** with **{home_pct}%** win rate")

    # Insight 3: Most prolific competition
    st.subheader("Most Goals by Competition")

    LEAGUE_NAMES = {
        'PL': 'Premier League',
        'PD': 'La Liga',
        'BL1': 'Bundesliga',
        'SA': 'Serie A',
        'FL1': 'Ligue 1',
        'CL': 'Champions League',
        'EC': 'European Championship',
        'PPL': 'Primeira Liga',
        'DED': 'Eredivisie',
        'ELC': 'Championship',
        'BSA': 'Brasileirão'
    }

    goals_by_comp = run_query("""
        SELECT
            COMPETITION_CODE,
            COUNT(*) AS matches,
            ROUND(AVG(HOME_SCORE + AWAY_SCORE), 2) AS avg_goals_per_match
        FROM SILVER.MATCHES
        WHERE STATUS = 'FINISHED'
        GROUP BY COMPETITION_CODE
        ORDER BY avg_goals_per_match DESC
        LIMIT 3
    """)

    if not goals_by_comp.empty:
        top_comp = goals_by_comp.iloc[0]
        comp_name = LEAGUE_NAMES.get(top_comp['COMPETITION_CODE'], top_comp['COMPETITION_CODE'])
        st.success(f"🥇 **Insight:** **{comp_name}** has the highest scoring matches: **{top_comp['AVG_GOALS_PER_MATCH']} goals/match**")

    # Insight 4: Extra Time Frequency
    st.subheader("Extra Time Analysis")

    extra_time_stats = run_query("""
        SELECT
            COUNT(*) AS total_matches,
            SUM(CASE WHEN MATCH_DURATION != 'REGULAR' THEN 1 ELSE 0 END) AS extra_time_matches,
            ROUND(100.0 * SUM(CASE WHEN MATCH_DURATION != 'REGULAR' THEN 1 ELSE 0 END) / COUNT(*), 1) AS extra_time_pct
        FROM SILVER.MATCHES
        WHERE STATUS = 'FINISHED'
          AND MATCH_DURATION IS NOT NULL
    """)

    if not extra_time_stats.empty:
        et_pct = extra_time_stats['EXTRA_TIME_PCT'].iloc[0]
        et_matches = extra_time_stats['EXTRA_TIME_MATCHES'].iloc[0]
        st.info(f"⚡ **Insight:** **{et_pct}%** of matches ({et_matches:,} matches) went to extra time or penalties")

    st.divider()

    # ============================================
    # Top Performers
    # ============================================
    st.header("🏆 Top Performers")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Top 5 Scorers (All Competitions)")
        top_scorers = run_query("""
            SELECT
                PLAYER_NAME,
                TEAM_SHORT,
                SUM(GOALS) AS total_goals,
                SUM(ASSISTS) AS total_assists
            FROM SILVER.SCORERS
            GROUP BY PLAYER_NAME, TEAM_SHORT
            ORDER BY total_goals DESC
            LIMIT 5
        """)

        if not top_scorers.empty:
            st.dataframe(top_scorers, use_container_width=True, hide_index=True)

    with col2:
        st.subheader("Most Active Referees")
        top_refs = run_query("""
            SELECT
                REFEREE_NAME,
                MATCHES_REFEREED,
                COMPETITIONS
            FROM GOLD.REFEREE_STATS
            ORDER BY MATCHES_REFEREED DESC
            LIMIT 5
        """)

        if not top_refs.empty:
            st.dataframe(top_refs, use_container_width=True, hide_index=True)

    st.divider()

    # ============================================
    # Geographic Distribution
    # ============================================
    st.header("🌍 Geographic Distribution")

    geo_dist = run_query("""
        SELECT
            AREA_NAME,
            TOTAL_MATCHES,
            AVG_GOALS_PER_MATCH,
            HOME_WIN_PCT
        FROM GOLD.GEOGRAPHIC_STATS
        ORDER BY TOTAL_MATCHES DESC
        LIMIT 10
    """)

    if not geo_dist.empty:
        col1, col2 = st.columns(2)

        with col1:
            fig = px.bar(geo_dist, x='AREA_NAME', y='TOTAL_MATCHES',
                        title='Top 10 Countries by Total Matches',
                        color='TOTAL_MATCHES',
                        color_continuous_scale='Blues')
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = px.scatter(geo_dist, x='AVG_GOALS_PER_MATCH', y='HOME_WIN_PCT',
                           size='TOTAL_MATCHES',
                           hover_data=['AREA_NAME'],
                           title='Goals vs Home Advantage by Country',
                           labels={'AVG_GOALS_PER_MATCH': 'Avg Goals/Match',
                                  'HOME_WIN_PCT': 'Home Win %'})
            st.plotly_chart(fig, use_container_width=True)

except Exception as e:
    st.error(f"Error loading insights: {e}")

st.divider()

# Footer
st.caption("💡 SnowGoal Insights | Season 2025-2026 | 11 competitions across Europe and Brazil")
