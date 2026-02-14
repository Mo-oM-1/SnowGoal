"""
SnowGoal - Football Analytics Dashboard
Built 100% on Snowflake Native Features
"""

import streamlit as st
from connection import run_query

st.set_page_config(
    page_title="SnowGoal",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Header
st.markdown("# ⚽ SnowGoal")
st.markdown("**Football Analytics Pipeline - 100% Snowflake Native**")

st.divider()

# Feature highlights
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("### 🏆 Leagues Covered")
    st.markdown("""
    - 🏴󠁧󠁢󠁥󠁮󠁧󠁿 Premier League
    - 🇪🇸 La Liga
    - 🇩🇪 Bundesliga
    - 🇮🇹 Serie A
    - 🇫🇷 Ligue 1
    """)

with col2:
    st.markdown("### ⚡ Snowflake Features")
    st.markdown("""
    - 🔄 Streams & Tasks
    - 🐍 Snowpark Python
    - 🔐 External Access
    - 📊 Orchestrated DAG
    - 📈 Native Streamlit
    """)

with col3:
    st.markdown("### 📈 Analytics")
    st.markdown("""
    - 🏅 Live Standings
    - ⚽ Top Scorers
    - 🏟️ Team Stats
    - 📅 Match Results
    - 🔮 Upcoming Fixtures
    """)

st.divider()

# Live stats from Snowflake
st.markdown("### 📊 Live Data Status")

try:
    stats = run_query("""
        SELECT
            (SELECT COUNT(*) FROM SILVER.MATCHES) AS MATCHES,
            (SELECT COUNT(*) FROM SILVER.TEAMS) AS TEAMS,
            (SELECT COUNT(*) FROM SILVER.SCORERS) AS SCORERS,
            (SELECT COUNT(*) FROM SILVER.COMPETITIONS) AS COMPETITIONS
    """)

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("⚽ Matches", int(stats['MATCHES'].iloc[0]))
    with col2:
        st.metric("🏟️ Teams", int(stats['TEAMS'].iloc[0]))
    with col3:
        st.metric("🎯 Scorers", int(stats['SCORERS'].iloc[0]))
    with col4:
        st.metric("🏆 Leagues", int(stats['COMPETITIONS'].iloc[0]))

    st.success("✅ Connected to Snowflake")

except Exception as e:
    st.warning("⚠️ Could not load live stats")
    st.info(f"Error: {e}")

# Footer
st.divider()
st.caption("Data source: football-data.org | Refresh: 3x daily (7h, 17h, 00h)")

# Sidebar
with st.sidebar:
    st.markdown("### 🧭 Navigation")
    st.markdown("""
    Use the sidebar to navigate:
    - **Standings**: League tables
    - **Top Scorers**: Goal rankings
    - **Matches**: Results & fixtures
    - **Teams**: Club details
    """)
