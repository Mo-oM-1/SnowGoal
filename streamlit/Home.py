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
st.info("📅 **Season 2025-2026** | Data refreshes automatically 3x daily")

st.divider()

# Feature highlights
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("### 🏆 11 Competitions")
    st.markdown("""
    - Premier League, La Liga
    - Bundesliga, Serie A, Ligue 1
    - Champions League, Euro
    - Primeira Liga, Eredivisie
    - Championship, Brasileirão
    """)

with col2:
    st.markdown("### ⚡ Snowflake Features")
    st.markdown("""
    - Streams & Tasks
    - Snowpark Python
    - External Access
    - Orchestrated DAG
    - Native Streamlit
    """)

with col3:
    st.markdown("### 📈 Analytics")
    st.markdown("""
    - Live Standings
    - Top Scorers
    - Team Stats
    - Match Results
    - Upcoming Fixtures
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
            (SELECT COUNT(*) FROM SILVER.COMPETITIONS) AS COMPETITIONS,
            (SELECT MAX(_UPDATED_AT) FROM SILVER.MATCHES) AS LAST_UPDATE,
            (SELECT DATEDIFF('hour', MAX(_UPDATED_AT), CURRENT_TIMESTAMP()) FROM SILVER.MATCHES) AS HOURS_SINCE_UPDATE
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

    # Connection status with data freshness
    hours_ago = stats['HOURS_SINCE_UPDATE'].iloc[0]
    if hours_ago < 2:
        st.success(f"✅ Connected to Snowflake ❄️ | **Data is fresh!** Last updated **{hours_ago} hours ago**")
    elif hours_ago < 8:
        st.info(f"✅ Connected to Snowflake ❄️ | **Data is up-to-date.** Last updated **{hours_ago} hours ago**")
    else:
        st.warning(f"✅ Connected to Snowflake ❄️ | **Data needs refresh.** Last updated **{hours_ago} hours ago**")

except Exception as e:
    st.warning("⚠️ Could not load live stats")
    st.info(f"Error: {e}")

# Footer
st.divider()
st.caption("Season 2025-2026 | 11 competitions | Data source: football-data.org | Refresh: 3x daily (7h, 17h, 00h)")

# Sidebar
with st.sidebar:
    st.markdown("### 🧭 Navigation")
    st.markdown("""
    Use the sidebar to navigate :
    - **Insights** : Key metrics & discoveries
    - **Standings** : League tables
    - **Top Scorers** : Goal rankings
    - **Matches** : Results & fixtures
    - **Teams** : Club details
    - **Analytics** : Advanced patterns
    - **Doc** : Full documentation
    """)
