"""
SnowGoal - Betting Intelligence
Comprehensive betting odds analysis and value detection
"""

import streamlit as st
import sys
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
sys.path.append('..')
from connection import run_query

st.set_page_config(page_title="Betting Intelligence | SnowGoal", page_icon="🎲", layout="wide")

st.title("🎲 Betting Intelligence")
st.info("📊 **Live Odds Analysis** | Data from multiple bookmakers | Updated continuously")

# ============================================
# Sidebar Filters
# ============================================
with st.sidebar:
    st.header("🔧 Filters")

    # Competition filter
    competitions = run_query("SELECT DISTINCT COMPETITION_CODE, COMPETITION_CODE as display FROM GOLD.ODDS_ANALYSIS ORDER BY COMPETITION_CODE")
    if not competitions.empty:
        selected_comps = st.multiselect(
            "Competitions",
            options=competitions['COMPETITION_CODE'].tolist(),
            default=competitions['COMPETITION_CODE'].tolist()[:3]
        )
    else:
        selected_comps = []
        st.warning("No odds data available")

    # Days ahead filter
    days_ahead = st.slider("Show matches in next X days", 1, 14, 7)

# ============================================
# SECTION 1: Upcoming Matches with Odds
# ============================================
st.header("📅 Upcoming Matches & Odds")

if selected_comps:
    comp_filter = "', '".join(selected_comps)
    upcoming_matches = run_query(f"""
        SELECT
            o.COMPETITION_CODE,
            o.COMMENCE_TIME,
            o.HOME_TEAM,
            o.AWAY_TEAM,
            o.AVG_HOME_ODDS,
            o.AVG_DRAW_ODDS,
            o.AVG_AWAY_ODDS,
            o.IMPLIED_HOME_PROB,
            o.IMPLIED_DRAW_PROB,
            o.IMPLIED_AWAY_PROB,
            o.BOOKMAKER_MARGIN_PCT,
            o.NB_BOOKMAKERS,
            DATEDIFF('day', CURRENT_TIMESTAMP(), o.COMMENCE_TIME) as days_until
        FROM GOLD.ODDS_ANALYSIS o
        WHERE o.COMPETITION_CODE IN ('{comp_filter}')
          AND o.COMMENCE_TIME > CURRENT_TIMESTAMP()
          AND o.COMMENCE_TIME <= DATEADD('day', {days_ahead}, CURRENT_TIMESTAMP())
        ORDER BY o.COMMENCE_TIME
        LIMIT 100
    """)

    if not upcoming_matches.empty:
        st.subheader(f"🔮 {len(upcoming_matches)} Upcoming Matches")

        # Display matches in expandable cards
        for idx, match in upcoming_matches.iterrows():
            match_time = pd.to_datetime(match['COMMENCE_TIME'])
            time_str = match_time.strftime("%a %d %b, %H:%M")

            with st.expander(f"**{match['HOME_TEAM']}** vs **{match['AWAY_TEAM']}** • {match['COMPETITION_CODE']} • {time_str}"):
                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    st.metric("🏠 Home Win", f"{match['AVG_HOME_ODDS']:.2f}", f"{match['IMPLIED_HOME_PROB']:.1f}%")
                with col2:
                    st.metric("🤝 Draw", f"{match['AVG_DRAW_ODDS']:.2f}", f"{match['IMPLIED_DRAW_PROB']:.1f}%")
                with col3:
                    st.metric("✈️ Away Win", f"{match['AVG_AWAY_ODDS']:.2f}", f"{match['IMPLIED_AWAY_PROB']:.1f}%")
                with col4:
                    st.metric("📊 Bookmakers", int(match['NB_BOOKMAKERS']), f"Margin: {match['BOOKMAKER_MARGIN_PCT']:.1f}%")
    else:
        st.warning("No upcoming matches found with odds data")

st.divider()

# ============================================
# SECTION 2: Bookmaker Comparison
# ============================================
st.header("⚖️ Bookmaker Comparison")

if selected_comps:
    st.subheader("Find the Best Odds")

    # Get detailed odds by bookmaker for upcoming matches
    bookmaker_odds = run_query(f"""
        SELECT
            o.HOME_TEAM || ' vs ' || o.AWAY_TEAM as MATCH,
            o.BOOKMAKER_TITLE,
            o.HOME_ODDS,
            o.DRAW_ODDS,
            o.AWAY_ODDS,
            o.COMMENCE_TIME
        FROM SILVER.ODDS o
        WHERE o.COMPETITION_CODE IN ('{comp_filter}')
          AND o.COMMENCE_TIME > CURRENT_TIMESTAMP()
          AND o.COMMENCE_TIME <= DATEADD('day', {days_ahead}, CURRENT_TIMESTAMP())
        ORDER BY o.COMMENCE_TIME, o.MATCH, o.BOOKMAKER_TITLE
        LIMIT 500
    """)

    if not bookmaker_odds.empty:
        # Select a match to compare
        unique_matches = bookmaker_odds['MATCH'].unique()
        selected_match = st.selectbox("Select a match to compare bookmakers:", unique_matches)

        match_data = bookmaker_odds[bookmaker_odds['MATCH'] == selected_match]

        col1, col2, col3 = st.columns(3)

        with col1:
            st.markdown("### 🏠 Home Win")
            best_home = match_data.nlargest(5, 'HOME_ODDS')[['BOOKMAKER_TITLE', 'HOME_ODDS']]
            best_home_idx = best_home['HOME_ODDS'].idxmax()
            for idx, row in best_home.iterrows():
                emoji = "🥇" if idx == best_home_idx else "📊"
                st.write(f"{emoji} **{row['BOOKMAKER_TITLE']}**: {row['HOME_ODDS']:.2f}")

        with col2:
            st.markdown("### 🤝 Draw")
            best_draw = match_data.nlargest(5, 'DRAW_ODDS')[['BOOKMAKER_TITLE', 'DRAW_ODDS']]
            best_draw_idx = best_draw['DRAW_ODDS'].idxmax()
            for idx, row in best_draw.iterrows():
                emoji = "🥇" if idx == best_draw_idx else "📊"
                st.write(f"{emoji} **{row['BOOKMAKER_TITLE']}**: {row['DRAW_ODDS']:.2f}")

        with col3:
            st.markdown("### ✈️ Away Win")
            best_away = match_data.nlargest(5, 'AWAY_ODDS')[['BOOKMAKER_TITLE', 'AWAY_ODDS']]
            best_away_idx = best_away['AWAY_ODDS'].idxmax()
            for idx, row in best_away.iterrows():
                emoji = "🥇" if idx == best_away_idx else "📊"
                st.write(f"{emoji} **{row['BOOKMAKER_TITLE']}**: {row['AWAY_ODDS']:.2f}")

        # Bookmaker margin comparison
        st.subheader("📊 Bookmaker Margin Analysis")
        margin_data = match_data.copy()
        margin_data['MARGIN'] = (1/margin_data['HOME_ODDS'] + 1/margin_data['DRAW_ODDS'] + 1/margin_data['AWAY_ODDS'] - 1) * 100

        fig_margin = px.bar(
            margin_data.sort_values('MARGIN'),
            x='BOOKMAKER_TITLE',
            y='MARGIN',
            title="Bookmaker Margin (Lower is Better for Bettors)",
            labels={'MARGIN': 'Margin (%)', 'BOOKMAKER_TITLE': 'Bookmaker'},
            color='MARGIN',
            color_continuous_scale='RdYlGn_r'
        )
        st.plotly_chart(fig_margin, use_container_width=True)
    else:
        st.info("No bookmaker comparison data available for selected filters")

st.divider()

# ============================================
# SECTION 3: Value Bets Detector
# ============================================
st.header("🎯 Value Bets Detector")
st.markdown("*Identifies matches where bookmaker odds differ significantly from historical performance*")

if selected_comps:
    # Calculate historical win rates and compare with implied probabilities
    value_bets = run_query(f"""
        WITH historical_performance AS (
            SELECT
                m.HOME_TEAM_NAME,
                m.AWAY_TEAM_NAME,
                m.COMPETITION_CODE,
                COUNT(*) as total_matches,
                SUM(CASE WHEN m.WINNER = 'HOME_TEAM' THEN 1 ELSE 0 END) as home_wins,
                SUM(CASE WHEN m.WINNER = 'DRAW' THEN 1 ELSE 0 END) as draws,
                SUM(CASE WHEN m.WINNER = 'AWAY_TEAM' THEN 1 ELSE 0 END) as away_wins,
                ROUND(100.0 * home_wins / total_matches, 1) as home_win_pct,
                ROUND(100.0 * draws / total_matches, 1) as draw_pct,
                ROUND(100.0 * away_wins / total_matches, 1) as away_win_pct
            FROM SILVER.MATCHES m
            WHERE m.STATUS = 'FINISHED'
              AND m.SEASON_YEAR >= YEAR(CURRENT_DATE()) - 2
              AND m.COMPETITION_CODE IN ('{comp_filter}')
            GROUP BY m.HOME_TEAM_NAME, m.AWAY_TEAM_NAME, m.COMPETITION_CODE
            HAVING total_matches >= 3
        )
        SELECT
            o.COMPETITION_CODE,
            o.HOME_TEAM,
            o.AWAY_TEAM,
            o.COMMENCE_TIME,
            o.AVG_HOME_ODDS,
            o.AVG_DRAW_ODDS,
            o.AVG_AWAY_ODDS,
            o.IMPLIED_HOME_PROB,
            o.IMPLIED_DRAW_PROB,
            o.IMPLIED_AWAY_PROB,
            h.home_win_pct as HIST_HOME_PCT,
            h.draw_pct as HIST_DRAW_PCT,
            h.away_win_pct as HIST_AWAY_PCT,
            h.total_matches as HIST_MATCHES,
            ROUND(h.home_win_pct - o.IMPLIED_HOME_PROB, 1) as HOME_VALUE,
            ROUND(h.draw_pct - o.IMPLIED_DRAW_PROB, 1) as DRAW_VALUE,
            ROUND(h.away_win_pct - o.IMPLIED_AWAY_PROB, 1) as AWAY_VALUE
        FROM GOLD.ODDS_ANALYSIS o
        LEFT JOIN historical_performance h
            ON o.HOME_TEAM = h.HOME_TEAM_NAME
            AND o.AWAY_TEAM = h.AWAY_TEAM_NAME
            AND o.COMPETITION_CODE = h.COMPETITION_CODE
        WHERE o.COMMENCE_TIME > CURRENT_TIMESTAMP()
          AND o.COMMENCE_TIME <= DATEADD('day', {days_ahead}, CURRENT_TIMESTAMP())
          AND o.COMPETITION_CODE IN ('{comp_filter}')
          AND h.total_matches IS NOT NULL
        ORDER BY o.COMMENCE_TIME
        LIMIT 50
    """)

    if not value_bets.empty:
        # Find significant value bets (>10% difference)
        value_threshold = 10

        value_bets['MAX_VALUE'] = value_bets[['HOME_VALUE', 'DRAW_VALUE', 'AWAY_VALUE']].max(axis=1)
        value_bets['VALUE_TYPE'] = value_bets.apply(
            lambda x: 'Home Win' if x['HOME_VALUE'] == x['MAX_VALUE']
                     else ('Draw' if x['DRAW_VALUE'] == x['MAX_VALUE'] else 'Away Win'),
            axis=1
        )

        significant_values = value_bets[value_bets['MAX_VALUE'] >= value_threshold].sort_values('MAX_VALUE', ascending=False)

        if not significant_values.empty:
            st.success(f"🔍 Found {len(significant_values)} potential value bets (>{value_threshold}% difference)")

            for idx, bet in significant_values.head(10).iterrows():
                match_time = pd.to_datetime(bet['COMMENCE_TIME']).strftime("%a %d %b, %H:%M")

                if bet['VALUE_TYPE'] == 'Home Win':
                    odds = bet['AVG_HOME_ODDS']
                    hist_pct = bet['HIST_HOME_PCT']
                    impl_pct = bet['IMPLIED_HOME_PROB']
                    value = bet['HOME_VALUE']
                elif bet['VALUE_TYPE'] == 'Draw':
                    odds = bet['AVG_DRAW_ODDS']
                    hist_pct = bet['HIST_DRAW_PCT']
                    impl_pct = bet['IMPLIED_DRAW_PROB']
                    value = bet['DRAW_VALUE']
                else:
                    odds = bet['AVG_AWAY_ODDS']
                    hist_pct = bet['HIST_AWAY_PCT']
                    impl_pct = bet['IMPLIED_AWAY_PROB']
                    value = bet['AWAY_VALUE']

                with st.expander(f"💎 **{bet['HOME_TEAM']} vs {bet['AWAY_TEAM']}** • {match_time} • Value: +{value:.1f}%"):
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Suggested Bet", bet['VALUE_TYPE'], f"Odds: {odds:.2f}")
                    with col2:
                        st.metric("Historical Win Rate", f"{hist_pct:.1f}%", f"Based on {int(bet['HIST_MATCHES'])} matches")
                    with col3:
                        st.metric("Implied Probability", f"{impl_pct:.1f}%", f"Difference: +{value:.1f}%")

                    st.info(f"📊 **Analysis**: Historical data suggests {bet['VALUE_TYPE']} occurs {hist_pct:.1f}% of the time in H2H matches, but bookmakers only imply {impl_pct:.1f}% probability (difference: {value:.1f}%). This could represent value.")
        else:
            st.info(f"No significant value bets found (threshold: {value_threshold}% difference between historical and implied probability)")
    else:
        st.warning("Not enough historical data to calculate value bets")

st.divider()

# ============================================
# SECTION 4: Match Predictor
# ============================================
st.header("🔮 SnowGoal Match Predictor")
st.markdown("*Combines team form, standings, and historical H2H data with odds analysis*")

if selected_comps:
    predictions = run_query(f"""
        WITH team_form AS (
            SELECT
                m.HOME_TEAM_NAME as TEAM,
                m.COMPETITION_CODE,
                COUNT(*) as recent_matches,
                SUM(CASE WHEN m.WINNER = 'HOME_TEAM' THEN 3
                         WHEN m.WINNER = 'DRAW' THEN 1 ELSE 0 END) as home_points,
                ROUND(AVG(m.HOME_SCORE), 1) as avg_goals_scored,
                ROUND(AVG(m.AWAY_SCORE), 1) as avg_goals_conceded
            FROM SILVER.MATCHES m
            WHERE m.STATUS = 'FINISHED'
              AND m.MATCH_DATE >= DATEADD('day', -90, CURRENT_DATE())
              AND m.COMPETITION_CODE IN ('{comp_filter}')
            GROUP BY m.HOME_TEAM_NAME, m.COMPETITION_CODE

            UNION ALL

            SELECT
                m.AWAY_TEAM_NAME as TEAM,
                m.COMPETITION_CODE,
                COUNT(*) as recent_matches,
                SUM(CASE WHEN m.WINNER = 'AWAY_TEAM' THEN 3
                         WHEN m.WINNER = 'DRAW' THEN 1 ELSE 0 END) as away_points,
                ROUND(AVG(m.AWAY_SCORE), 1) as avg_goals_scored,
                ROUND(AVG(m.HOME_SCORE), 1) as avg_goals_conceded
            FROM SILVER.MATCHES m
            WHERE m.STATUS = 'FINISHED'
              AND m.MATCH_DATE >= DATEADD('day', -90, CURRENT_DATE())
              AND m.COMPETITION_CODE IN ('{comp_filter}')
            GROUP BY m.AWAY_TEAM_NAME, m.COMPETITION_CODE
        ),
        aggregated_form AS (
            SELECT
                TEAM,
                COMPETITION_CODE,
                SUM(recent_matches) as total_matches,
                ROUND(SUM(home_points + away_points) * 1.0 / SUM(recent_matches), 2) as ppg,
                ROUND(AVG(avg_goals_scored), 1) as avg_scored,
                ROUND(AVG(avg_goals_conceded), 1) as avg_conceded
            FROM team_form
            GROUP BY TEAM, COMPETITION_CODE
        )
        SELECT
            o.COMPETITION_CODE,
            o.HOME_TEAM,
            o.AWAY_TEAM,
            o.COMMENCE_TIME,
            o.AVG_HOME_ODDS,
            o.AVG_DRAW_ODDS,
            o.AVG_AWAY_ODDS,
            o.IMPLIED_HOME_PROB,
            o.IMPLIED_DRAW_PROB,
            o.IMPLIED_AWAY_PROB,
            h.ppg as home_form_ppg,
            h.avg_scored as home_avg_scored,
            h.avg_conceded as home_avg_conceded,
            a.ppg as away_form_ppg,
            a.avg_scored as away_avg_scored,
            a.avg_conceded as away_avg_conceded,
            ROUND((h.ppg - a.ppg) * 10, 1) as form_advantage
        FROM GOLD.ODDS_ANALYSIS o
        LEFT JOIN aggregated_form h ON o.HOME_TEAM = h.TEAM AND o.COMPETITION_CODE = h.COMPETITION_CODE
        LEFT JOIN aggregated_form a ON o.AWAY_TEAM = a.TEAM AND o.COMPETITION_CODE = a.COMPETITION_CODE
        WHERE o.COMMENCE_TIME > CURRENT_TIMESTAMP()
          AND o.COMMENCE_TIME <= DATEADD('day', {days_ahead}, CURRENT_TIMESTAMP())
          AND o.COMPETITION_CODE IN ('{comp_filter}')
          AND h.ppg IS NOT NULL
          AND a.ppg IS NOT NULL
        ORDER BY o.COMMENCE_TIME
        LIMIT 20
    """)

    if not predictions.empty:
        for idx, pred in predictions.iterrows():
            match_time = pd.to_datetime(pred['COMMENCE_TIME']).strftime("%a %d %b, %H:%M")

            # Calculate SnowGoal prediction
            form_diff = pred['FORM_ADVANTAGE']

            # Simple prediction logic
            if form_diff > 5:
                snowgoal_pred = "Home Win"
                confidence = min(70 + form_diff, 85)
            elif form_diff < -5:
                snowgoal_pred = "Away Win"
                confidence = min(70 + abs(form_diff), 85)
            else:
                snowgoal_pred = "Close Match / Draw Possible"
                confidence = 50 + abs(form_diff)

            # Compare with bookmaker favorite
            if pred['IMPLIED_HOME_PROB'] > pred['IMPLIED_AWAY_PROB'] and pred['IMPLIED_HOME_PROB'] > pred['IMPLIED_DRAW_PROB']:
                bookie_pred = "Home Win"
            elif pred['IMPLIED_AWAY_PROB'] > pred['IMPLIED_HOME_PROB'] and pred['IMPLIED_AWAY_PROB'] > pred['IMPLIED_DRAW_PROB']:
                bookie_pred = "Away Win"
            else:
                bookie_pred = "Draw"

            agreement = "✅" if snowgoal_pred.split()[0] == bookie_pred.split()[0] else "⚠️"

            with st.expander(f"{agreement} **{pred['HOME_TEAM']} vs {pred['AWAY_TEAM']}** • {pred['COMPETITION_CODE']} • {match_time}"):
                col1, col2 = st.columns(2)

                with col1:
                    st.markdown("### 🤖 SnowGoal Prediction")
                    st.metric("Prediction", snowgoal_pred, f"Confidence: {confidence:.0f}%")
                    st.markdown(f"""
                    **Form Analysis (Last 90 days):**
                    - 🏠 Home: {pred['HOME_FORM_PPG']:.2f} PPG, {pred['HOME_AVG_SCORED']:.1f} goals/match
                    - ✈️ Away: {pred['AWAY_FORM_PPG']:.2f} PPG, {pred['AWAY_AVG_SCORED']:.1f} goals/match
                    - 📊 Form Advantage: {form_diff:+.1f} (towards {"Home" if form_diff > 0 else "Away"})
                    """)

                with col2:
                    st.markdown("### 💰 Bookmaker Prediction")
                    st.metric("Favorite", bookie_pred)
                    st.markdown(f"""
                    **Implied Probabilities:**
                    - 🏠 Home: {pred['IMPLIED_HOME_PROB']:.1f}% (odds: {pred['AVG_HOME_ODDS']:.2f})
                    - 🤝 Draw: {pred['IMPLIED_DRAW_PROB']:.1f}% (odds: {pred['AVG_DRAW_ODDS']:.2f})
                    - ✈️ Away: {pred['IMPLIED_AWAY_PROB']:.1f}% (odds: {pred['AVG_AWAY_ODDS']:.2f})
                    """)
    else:
        st.info("Not enough form data to generate predictions")

st.divider()

# ============================================
# SECTION 5: ROI Simulator
# ============================================
st.header("💰 ROI Simulator")
st.markdown("*Simulate betting strategies on historical matches with known outcomes*")

if selected_comps:
    st.subheader("⚙️ Strategy Configuration")

    col1, col2, col3 = st.columns(3)
    with col1:
        strategy = st.selectbox("Strategy", [
            "Always Home",
            "Always Away",
            "Always Draw",
            "Favorite (Lowest Odds)",
            "Underdog (Highest Odds)"
        ])
    with col2:
        stake_per_bet = st.number_input("Stake per Bet (€)", min_value=1, max_value=1000, value=10)
    with col3:
        min_odds = st.slider("Min Odds Filter", 1.0, 5.0, 1.5, 0.1)

    # Simulate strategy (we need historical odds matched with results)
    # Note: This requires odds data that we can match with finished matches
    st.info("💡 **Coming Soon**: Historical odds backtesting. This feature requires historical odds data matched with actual match results. Currently, odds are only available for upcoming matches.")

    st.markdown("""
    **Planned Features:**
    - Backtest strategies on past matches with known outcomes
    - Calculate ROI, win rate, and profit/loss
    - Compare multiple strategies side-by-side
    - Risk analysis and variance calculations
    - Kelly Criterion optimal stake suggestions
    """)

st.divider()

# ============================================
# Analytics Summary
# ============================================
st.header("📊 Odds Data Summary")

if selected_comps:
    summary = run_query(f"""
        SELECT
            COUNT(DISTINCT GAME_ID) as total_games,
            COUNT(DISTINCT BOOKMAKER_KEY) as total_bookmakers,
            ROUND(AVG(BOOKMAKER_MARGIN_PCT), 2) as avg_margin,
            MIN(COMMENCE_TIME) as next_match,
            MAX(COMMENCE_TIME) as last_match
        FROM GOLD.ODDS_ANALYSIS
        WHERE COMPETITION_CODE IN ('{comp_filter}')
          AND COMMENCE_TIME > CURRENT_TIMESTAMP()
    """)

    if not summary.empty:
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("📅 Games with Odds", int(summary['TOTAL_GAMES'].iloc[0]))
        with col2:
            st.metric("🏦 Bookmakers", int(summary['TOTAL_BOOKMAKERS'].iloc[0]))
        with col3:
            st.metric("📊 Avg Margin", f"{summary['AVG_MARGIN'].iloc[0]:.2f}%")
        with col4:
            next_match = pd.to_datetime(summary['NEXT_MATCH'].iloc[0])
            hours_until = (next_match - datetime.now()).total_seconds() / 3600
            st.metric("⏰ Next Match", f"{hours_until:.1f}h")

st.divider()

# Footer
st.caption("🎲 SnowGoal Betting Intelligence | Season 2025-2026 | Odds from The Odds API | For informational purposes only")
st.caption("⚠️ **Disclaimer**: This tool is for educational and analytical purposes. Betting involves risk. Gamble responsibly.")
