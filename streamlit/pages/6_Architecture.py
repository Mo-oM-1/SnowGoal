"""
SnowGoal - Architecture & Monitoring Page
"""

import streamlit as st
import sys
sys.path.append('..')
from connection import run_query
import pandas as pd

st.set_page_config(page_title="Architecture | SnowGoal", page_icon="🏗️", layout="wide")

st.title("🏗️ Pipeline Architecture & Monitoring")

# Architecture Diagram
st.header("📊 Data Pipeline Flow")

st.markdown("""
```
football-data.org API
        |
        ↓ (Snowpark Python Procedure)
RAW Layer (VARIANT JSON)
        |
        ↓ (Streams CDC)
STAGING Views (LATERAL FLATTEN)
        |
        ↓ (MERGE incremental)
SILVER Tables (Clean Data)
        |
        ↓ (INSERT OVERWRITE via Tasks)
GOLD Tables (Aggregations)
        |
        ↓
Streamlit Dashboard
```
""")

st.divider()

# DAG Orchestration
st.header("🔄 Task Orchestration (DAG)")

try:
    tasks_df = run_query("""
        SELECT
            NAME,
            STATE,
            SCHEDULE,
            WAREHOUSE,
            PREDECESSORS
        FROM TABLE(INFORMATION_SCHEMA.TASK_DEPENDENTS(
            TASK_NAME => 'SNOWGOAL_DB.COMMON.TASK_FETCH_ALL_LEAGUES',
            RECURSIVE => TRUE
        ))
        ORDER BY NAME
    """)

    if not tasks_df.empty:
        st.dataframe(tasks_df, use_container_width=True)

        # Task status summary
        col1, col2 = st.columns(2)
        with col1:
            total_tasks = len(tasks_df)
            started_tasks = len(tasks_df[tasks_df['STATE'] == 'started'])
            st.metric("📋 Total Tasks", total_tasks)
        with col2:
            st.metric("✅ Active Tasks", started_tasks, delta=f"{started_tasks}/{total_tasks}")
    else:
        st.info("No tasks found.")

except Exception as e:
    st.warning(f"Could not load task information: {e}")

st.divider()

# Data Lineage
st.header("📈 Data Lineage (Row Counts)")

try:
    lineage_df = run_query("""
        SELECT
            'RAW' AS LAYER,
            'RAW_MATCHES' AS TABLE_NAME,
            (SELECT COUNT(*) FROM RAW.RAW_MATCHES) AS ROW_COUNT
        UNION ALL
        SELECT 'RAW', 'RAW_STANDINGS', (SELECT COUNT(*) FROM RAW.RAW_STANDINGS)
        UNION ALL
        SELECT 'RAW', 'RAW_SCORERS', (SELECT COUNT(*) FROM RAW.RAW_SCORERS)
        UNION ALL
        SELECT 'RAW', 'RAW_TEAMS', (SELECT COUNT(*) FROM RAW.RAW_TEAMS)
        UNION ALL
        SELECT 'SILVER', 'MATCHES', (SELECT COUNT(*) FROM SILVER.MATCHES)
        UNION ALL
        SELECT 'SILVER', 'STANDINGS', (SELECT COUNT(*) FROM SILVER.STANDINGS)
        UNION ALL
        SELECT 'SILVER', 'SCORERS', (SELECT COUNT(*) FROM SILVER.SCORERS)
        UNION ALL
        SELECT 'SILVER', 'TEAMS', (SELECT COUNT(*) FROM SILVER.TEAMS)
        UNION ALL
        SELECT 'GOLD', 'LEAGUE_STANDINGS', (SELECT COUNT(*) FROM GOLD.LEAGUE_STANDINGS)
        UNION ALL
        SELECT 'GOLD', 'TOP_SCORERS', (SELECT COUNT(*) FROM GOLD.TOP_SCORERS)
        UNION ALL
        SELECT 'GOLD', 'TEAM_STATS', (SELECT COUNT(*) FROM GOLD.TEAM_STATS)
        UNION ALL
        SELECT 'GOLD', 'RECENT_MATCHES', (SELECT COUNT(*) FROM GOLD.RECENT_MATCHES)
        UNION ALL
        SELECT 'GOLD', 'UPCOMING_FIXTURES', (SELECT COUNT(*) FROM GOLD.UPCOMING_FIXTURES)
        ORDER BY LAYER, TABLE_NAME
    """)

    if not lineage_df.empty:
        # Group by layer for better visualization
        for layer in ['RAW', 'SILVER', 'GOLD']:
            layer_data = lineage_df[lineage_df['LAYER'] == layer]
            if not layer_data.empty:
                st.subheader(f"{layer} Layer")
                cols = st.columns(len(layer_data))
                for i, (idx, row) in enumerate(layer_data.iterrows()):
                    with cols[i]:
                        st.metric(
                            row['TABLE_NAME'].replace('_', ' ').title(),
                            f"{int(row['ROW_COUNT']):,}"
                        )

except Exception as e:
    st.warning(f"Could not load lineage data: {e}")

st.divider()

# Credit Consumption
st.header("💰 Credit Consumption (Last 7 Days)")

try:
    credits_df = run_query("""
        SELECT
            DATE_TRUNC('DAY', START_TIME) AS DAY,
            SUM(CREDITS_USED) AS DAILY_CREDITS
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
        WHERE WAREHOUSE_NAME = 'SNOWGOAL_WH_XS'
            AND START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
        GROUP BY 1
        ORDER BY 1 DESC
    """)

    if not credits_df.empty:
        # KPIs
        col1, col2, col3 = st.columns(3)
        with col1:
            total_credits = credits_df['DAILY_CREDITS'].sum()
            st.metric("📊 Total (7 days)", f"{total_credits:.3f} credits")
        with col2:
            avg_credits = credits_df['DAILY_CREDITS'].mean()
            st.metric("📈 Avg/Day", f"{avg_credits:.3f} credits")
        with col3:
            last_day_credits = credits_df.iloc[0]['DAILY_CREDITS']
            st.metric("🕐 Yesterday", f"{last_day_credits:.3f} credits")

        # Chart
        st.bar_chart(credits_df.set_index('DAY')['DAILY_CREDITS'])

    else:
        st.info("No credit usage data available yet.")

except Exception as e:
    st.warning(f"Could not load credit data: {e}")

st.divider()

# Technical Stack
st.header("⚙️ Technical Stack")

col1, col2 = st.columns(2)

with col1:
    st.markdown("### Data Layer")
    st.markdown("""
    - **Storage**: Snowflake Tables (VARIANT for JSON)
    - **Processing**: Snowpark Python Procedures
    - **CDC**: Streams for change capture
    - **Orchestration**: Tasks with DAG dependencies
    - **Scheduling**: CRON (7h, 17h, 00h)
    """)

with col2:
    st.markdown("### Infrastructure")
    st.markdown("""
    - **Warehouse**: SNOWGOAL_WH_XS (auto-suspend 60s)
    - **API**: football-data.org REST API
    - **Security**: External Access Integration + Secrets
    - **Frontend**: Streamlit in Snowflake
    - **Version Control**: Git + GitHub
    """)

st.divider()

# Refresh Schedule
st.header("⏰ Refresh Schedule")

st.info("🕐 Data refreshes automatically **3 times daily**: **7h**, **17h**, and **00h** (Europe/Paris timezone)")

st.markdown("""
**Pipeline Execution Order:**
1. `TASK_FETCH_ALL_LEAGUES` (CRON: 7h, 17h, 00h) - Fetch API data
2. `TASK_MERGE_TO_SILVER` - Merge to SILVER tables
3. **5 parallel tasks** - Refresh GOLD tables:
   - `TASK_REFRESH_LEAGUE_STANDINGS`
   - `TASK_REFRESH_TOP_SCORERS`
   - `TASK_REFRESH_TEAM_STATS`
   - `TASK_REFRESH_RECENT_MATCHES`
   - `TASK_REFRESH_UPCOMING_FIXTURES`
""")
