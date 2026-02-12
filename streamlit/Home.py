"""
SnowGoal - Football Analytics Dashboard
Built 100% on Snowflake Native Features
"""

import streamlit as st

st.set_page_config(
    page_title="SnowGoal",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        background: linear-gradient(90deg, #1e88e5, #43a047);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        padding: 1rem 0;
    }
    .feature-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        margin: 0.5rem 0;
    }
    .stat-box {
        background: #f8f9fa;
        padding: 1rem;
        border-radius: 8px;
        text-align: center;
        border-left: 4px solid #1e88e5;
    }
</style>
""", unsafe_allow_html=True)

# Header
st.markdown('<h1 class="main-header">⚽ SnowGoal</h1>', unsafe_allow_html=True)
st.markdown("""
<p style="text-align: center; font-size: 1.2rem; color: #666;">
    Football Analytics Pipeline - 100% Snowflake Native
</p>
""", unsafe_allow_html=True)

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
    - 📊 Dynamic Tables
    - 🔄 Streams & Tasks
    - 🐍 Snowpark Python
    - 🔐 External Access
    - 📈 Native Streamlit
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

# Architecture diagram
st.markdown("### 🏗️ Architecture")
st.code("""
┌─────────────────────────────────────────────────────────────────────┐
│                         SNOWGOAL PIPELINE                           │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   ┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐  │
│   │   API    │────▶│   RAW    │────▶│  SILVER  │────▶│   GOLD   │  │
│   │ Football │     │ (VARIANT)│     │  (MERGE) │     │(DYN.TBL) │  │
│   └──────────┘     └──────────┘     └──────────┘     └──────────┘  │
│        │                │                                    │      │
│        │                │                                    │      │
│   ┌────┴────┐     ┌────┴────┐                          ┌────┴────┐ │
│   │Snowpark │     │ Streams │                          │Streamlit│ │
│   │  Proc   │     │  (CDC)  │                          │Dashboard│ │
│   └─────────┘     └─────────┘                          └─────────┘ │
│                                                                     │
│   ┌─────────────────────────────────────────────────────────────┐  │
│   │                    TASKS (DAG)                               │  │
│   │   Fetch PL ─▶ Fetch PD ─▶ Fetch BL1 ─▶ Fetch SA ─▶ Merge   │  │
│   └─────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
""", language=None)

# Footer
st.divider()
st.markdown("""
<p style="text-align: center; color: #999; font-size: 0.9rem;">
    Data source: football-data.org | Refresh: Every 6 hours
</p>
""", unsafe_allow_html=True)

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

    st.divider()
    st.markdown("### 📊 Data Status")
    st.info("Connect to Snowflake to see live data")
