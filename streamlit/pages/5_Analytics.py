"""
SnowGoal - Advanced Analytics Page
Enhanced with match patterns, referee stats, and geographic analysis
"""

import streamlit as st
import sys
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
sys.path.append('..')
from connection import run_query

st.set_page_config(page_title="Analytics | SnowGoal", page_icon="📊", layout="wide")

st.title("📊 Advanced Analytics")

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

# Get available competitions
try:
    competitions = run_query("""
        SELECT DISTINCT COMPETITION_CODE
        FROM GOLD.MATCH_PATTERNS
        ORDER BY COMPETITION_CODE
    """)

    comp_codes = list(competitions['COMPETITION_CODE'])
    comp_options = {LEAGUE_NAMES.get(code, code): code for code in comp_codes}

    if comp_options:
        selected_comp = st.selectbox("Select League", list(comp_options.keys()))
        comp_code = comp_options[selected_comp]
    else:
        st.warning("No data available. Run the pipeline first.")
        st.stop()
except Exception as e:
    st.error(f"Could not load competitions: {e}")
    st.stop()

# Tabs for different analyses
tab1, tab2, tab3 = st.tabs(["Time Patterns", "Referee Stats", "Geographic Analysis"])

# ============================================
# TAB 1: Time Patterns
# ============================================
with tab1:
    st.subheader("⏰ Match Timing Patterns")

    try:
        # Weekend vs Midweek analysis
        st.markdown("### Weekend vs Midweek")
        weekend_data = run_query(f"""
            SELECT
                CASE
                    WHEN DAY_OF_WEEK IN ('Sat', 'Sun') THEN 'Weekend'
                    ELSE 'Midweek'
                END AS PERIOD,
                SUM(TOTAL_MATCHES) AS MATCHES,
                ROUND(AVG(AVG_TOTAL_GOALS), 2) AS AVG_GOALS
            FROM GOLD.MATCH_PATTERNS
            WHERE COMPETITION_CODE = '{comp_code}'
            GROUP BY PERIOD
        """)

        if not weekend_data.empty:
            col1, col2 = st.columns(2)
            with col1:
                fig = px.bar(weekend_data, x='PERIOD', y='MATCHES',
                            title='Matches by Period',
                            color='PERIOD',
                            color_discrete_map={'Weekend': '#1f77b4', 'Midweek': '#ff7f0e'})
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                fig = px.bar(weekend_data, x='PERIOD', y='AVG_GOALS',
                            title='Average Goals by Period',
                            color='PERIOD',
                            color_discrete_map={'Weekend': '#1f77b4', 'Midweek': '#ff7f0e'})
                st.plotly_chart(fig, use_container_width=True)

        st.divider()

        # Goals by hour
        st.markdown("### Goals by Match Hour")
        hourly_data = run_query(f"""
            SELECT
                MATCH_HOUR,
                SUM(TOTAL_MATCHES) AS MATCHES,
                ROUND(AVG(AVG_TOTAL_GOALS), 2) AS AVG_GOALS
            FROM GOLD.MATCH_PATTERNS
            WHERE COMPETITION_CODE = '{comp_code}'
            GROUP BY MATCH_HOUR
            ORDER BY MATCH_HOUR
        """)

        if not hourly_data.empty:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=hourly_data['MATCH_HOUR'],
                y=hourly_data['AVG_GOALS'],
                mode='lines+markers',
                name='Avg Goals',
                line=dict(color='#2ecc71', width=3),
                marker=dict(size=10)
            ))
            fig.update_layout(
                title='Average Goals by Match Hour',
                xaxis_title='Hour (UTC)',
                yaxis_title='Average Goals',
                hovermode='x'
            )
            st.plotly_chart(fig, use_container_width=True)

        st.divider()

        # Day of week breakdown
        st.markdown("### Day of Week Analysis")
        dow_data = run_query(f"""
            SELECT
                DAY_OF_WEEK,
                SUM(TOTAL_MATCHES) AS MATCHES,
                ROUND(AVG(AVG_TOTAL_GOALS), 2) AS AVG_GOALS,
                ROUND(100.0 * SUM(HOME_WINS) / SUM(TOTAL_MATCHES), 1) AS HOME_WIN_PCT
            FROM GOLD.MATCH_PATTERNS
            WHERE COMPETITION_CODE = '{comp_code}'
            GROUP BY DAY_OF_WEEK
            ORDER BY
                CASE DAY_OF_WEEK
                    WHEN 'Mon' THEN 1
                    WHEN 'Tue' THEN 2
                    WHEN 'Wed' THEN 3
                    WHEN 'Thu' THEN 4
                    WHEN 'Fri' THEN 5
                    WHEN 'Sat' THEN 6
                    WHEN 'Sun' THEN 7
                END
        """)

        if not dow_data.empty:
            col1, col2 = st.columns(2)
            with col1:
                fig = px.bar(dow_data, x='DAY_OF_WEEK', y='MATCHES',
                            title='Matches by Day of Week',
                            color='MATCHES',
                            color_continuous_scale='Blues')
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                fig = px.line(dow_data, x='DAY_OF_WEEK', y='HOME_WIN_PCT',
                             title='Home Win % by Day',
                             markers=True)
                fig.update_traces(line_color='#e74c3c', marker=dict(size=12))
                st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"Error loading time patterns: {e}")

# ============================================
# TAB 2: Referee Stats
# ============================================
with tab2:
    st.subheader("👨‍⚖️ Referee Statistics")

    try:
        # Top referees
        st.markdown("### 🏆 Most Active Referees")
        top_refs = run_query(f"""
            SELECT
                REFEREE_NAME,
                REFEREE_NATIONALITY,
                MATCHES_REFEREED,
                AVG_GOALS_PER_MATCH,
                EXTRA_TIME_PCT
            FROM GOLD.REFEREE_STATS
            WHERE REFEREE_NAME IN (
                SELECT REFEREE_NAME
                FROM SILVER.MATCHES
                WHERE COMPETITION_CODE = '{comp_code}'
                AND REFEREE_NAME IS NOT NULL
            )
            ORDER BY MATCHES_REFEREED DESC
            LIMIT 15
        """)

        if not top_refs.empty:
            st.dataframe(top_refs, use_container_width=True)

            col1, col2 = st.columns(2)
            with col1:
                fig = px.bar(top_refs.head(10), x='REFEREE_NAME', y='MATCHES_REFEREED',
                            title='Top 10 Referees by Matches',
                            color='MATCHES_REFEREED',
                            color_continuous_scale='Viridis')
                fig.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                fig = px.scatter(top_refs, x='MATCHES_REFEREED', y='AVG_GOALS_PER_MATCH',
                               size='EXTRA_TIME_PCT',
                               hover_data=['REFEREE_NAME', 'REFEREE_NATIONALITY'],
                               title='Goals per Match vs Experience',
                               labels={'AVG_GOALS_PER_MATCH': 'Avg Goals/Match',
                                      'MATCHES_REFEREED': 'Matches Refereed'})
                st.plotly_chart(fig, use_container_width=True)

        st.divider()

        # Referees by nationality
        st.markdown("### 🌍 Referees by Nationality")
        refs_by_nation = run_query(f"""
            SELECT
                REFEREE_NATIONALITY,
                COUNT(DISTINCT REFEREE_NAME) AS NUM_REFEREES,
                SUM(MATCHES_REFEREED) AS TOTAL_MATCHES
            FROM GOLD.REFEREE_STATS
            WHERE REFEREE_NAME IN (
                SELECT REFEREE_NAME
                FROM SILVER.MATCHES
                WHERE COMPETITION_CODE = '{comp_code}'
                AND REFEREE_NAME IS NOT NULL
            )
            GROUP BY REFEREE_NATIONALITY
            ORDER BY TOTAL_MATCHES DESC
            LIMIT 10
        """)

        if not refs_by_nation.empty:
            fig = px.treemap(refs_by_nation,
                           path=['REFEREE_NATIONALITY'],
                           values='TOTAL_MATCHES',
                           title='Referee Distribution by Nationality',
                           color='NUM_REFEREES',
                           color_continuous_scale='RdYlGn')
            st.plotly_chart(fig, use_container_width=True)

        st.divider()

        # Extra time specialists
        st.markdown("### ⏱️ Extra Time Specialists")
        extra_time_refs = run_query(f"""
            SELECT
                REFEREE_NAME,
                REFEREE_NATIONALITY,
                MATCHES_REFEREED,
                EXTRA_TIME_MATCHES,
                EXTRA_TIME_PCT
            FROM GOLD.REFEREE_STATS
            WHERE REFEREE_NAME IN (
                SELECT REFEREE_NAME
                FROM SILVER.MATCHES
                WHERE COMPETITION_CODE = '{comp_code}'
                AND REFEREE_NAME IS NOT NULL
            )
            AND MATCHES_REFEREED >= 10
            ORDER BY EXTRA_TIME_PCT DESC
            LIMIT 10
        """)

        if not extra_time_refs.empty:
            fig = px.bar(extra_time_refs, x='REFEREE_NAME', y='EXTRA_TIME_PCT',
                        title='Top 10 Referees by Extra Time %',
                        color='EXTRA_TIME_PCT',
                        color_continuous_scale='Reds',
                        hover_data=['MATCHES_REFEREED', 'EXTRA_TIME_MATCHES'])
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"Error loading referee stats: {e}")

# ============================================
# TAB 3: Geographic Analysis
# ============================================
with tab3:
    st.subheader("🌍 Geographic Analysis")

    try:
        # Matches by country
        st.markdown("### 🗺️ Matches by Country/Area")
        geo_data = run_query("""
            SELECT
                AREA_NAME,
                AREA_CODE,
                COMPETITIONS,
                TOTAL_MATCHES,
                AVG_GOALS_PER_MATCH,
                HOME_WIN_PCT
            FROM GOLD.GEOGRAPHIC_STATS
            ORDER BY TOTAL_MATCHES DESC
            LIMIT 15
        """)

        if not geo_data.empty:
            st.dataframe(geo_data, use_container_width=True)

            col1, col2 = st.columns(2)
            with col1:
                fig = px.bar(geo_data, x='AREA_NAME', y='TOTAL_MATCHES',
                            title='Top Countries by Total Matches',
                            color='TOTAL_MATCHES',
                            color_continuous_scale='Blues')
                fig.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                fig = px.scatter(geo_data, x='AVG_GOALS_PER_MATCH', y='HOME_WIN_PCT',
                               size='TOTAL_MATCHES',
                               hover_data=['AREA_NAME'],
                               title='Goals vs Home Advantage',
                               labels={'AVG_GOALS_PER_MATCH': 'Avg Goals/Match',
                                      'HOME_WIN_PCT': 'Home Win %'})
                st.plotly_chart(fig, use_container_width=True)

        st.divider()

        # Home advantage analysis
        st.markdown("### 🏠 Home Advantage Analysis")
        home_adv = run_query("""
            SELECT
                AREA_NAME,
                TOTAL_MATCHES,
                HOME_WINS,
                AWAY_WINS,
                DRAWS,
                HOME_WIN_PCT
            FROM GOLD.GEOGRAPHIC_STATS
            WHERE TOTAL_MATCHES >= 50
            ORDER BY HOME_WIN_PCT DESC
            LIMIT 10
        """)

        if not home_adv.empty:
            fig = px.bar(home_adv, x='AREA_NAME', y='HOME_WIN_PCT',
                        title='Strongest Home Advantage (min 50 matches)',
                        color='HOME_WIN_PCT',
                        color_continuous_scale='Greens',
                        hover_data=['TOTAL_MATCHES', 'HOME_WINS', 'AWAY_WINS'])
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

            # Pie chart for win distribution
            avg_area = home_adv.iloc[0]['AREA_NAME']
            values = [
                home_adv.iloc[0]['HOME_WINS'],
                home_adv.iloc[0]['AWAY_WINS'],
                home_adv.iloc[0]['DRAWS']
            ]
            labels = ['Home Wins', 'Away Wins', 'Draws']

            fig = go.Figure(data=[go.Pie(labels=labels, values=values,
                                        marker_colors=['#2ecc71', '#e74c3c', '#95a5a6'])])
            fig.update_layout(title=f'Match Outcomes - {avg_area}')
            st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"Error loading geographic stats: {e}")
