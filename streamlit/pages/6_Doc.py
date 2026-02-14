"""
SnowGoal - Documentation Page
"""

import streamlit as st

st.set_page_config(page_title="Doc | SnowGoal", page_icon="📖", layout="wide")

st.title("📖 Documentation")

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

# Data Layers
st.header("📂 Data Layers")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("### 🔴 RAW Layer")
    st.markdown("""
    **Format:** VARIANT (JSON)

    **Tables:**
    - `RAW_MATCHES`
    - `RAW_STANDINGS`
    - `RAW_SCORERS`
    - `RAW_TEAMS`
    - `RAW_COMPETITIONS`

    **CDC:** Streams capture changes
    """)

with col2:
    st.markdown("### 🟡 SILVER Layer")
    st.markdown("""
    **Format:** Structured tables

    **Tables:**
    - `MATCHES`
    - `STANDINGS`
    - `SCORERS`
    - `TEAMS`
    - `COMPETITIONS`

    **Updates:** MERGE incremental
    """)

with col3:
    st.markdown("### 🟢 GOLD Layer")
    st.markdown("""
    **Format:** Aggregations

    **Tables:**
    - `LEAGUE_STANDINGS`
    - `TOP_SCORERS`
    - `TEAM_STATS`
    - `RECENT_MATCHES`
    - `UPCOMING_FIXTURES`

    **Refresh:** INSERT OVERWRITE
    """)

st.divider()

# CDC Section
st.header("🔄 CDC (Change Data Capture)")

st.markdown("""
### Qu'est-ce que le CDC ?

Le **Change Data Capture** capture automatiquement les modifications (INSERT, UPDATE, DELETE) dans les tables RAW grâce aux **Streams** Snowflake.

### Pourquoi utiliser le CDC ?
- ✅ **Performance** : Traite uniquement les changements, pas toutes les données
- ✅ **Coûts réduits** : Moins de compute = moins de crédits
- ✅ **Traçabilité** : Historique des modifications
""")

st.subheader("📌 Exemples de requêtes SQL")

# Example 1: Check streams exist
with st.expander("Vérifier que les streams existent"):
    st.code("""
USE ROLE SNOWGOAL_ROLE;
USE WAREHOUSE SNOWGOAL_WH_XS;
USE DATABASE SNOWGOAL_DB;
USE SCHEMA RAW;

SHOW STREAMS IN SCHEMA RAW;

-- Résultat attendu : 4 streams (MATCHES, SCORERS, STANDINGS, TEAMS)
    """, language="sql")

# Example 2: Check if stream has data
with st.expander("Vérifier si un stream contient des données"):
    st.code("""
USE ROLE SNOWGOAL_ROLE;
USE WAREHOUSE SNOWGOAL_WH_XS;
USE DATABASE SNOWGOAL_DB;
USE SCHEMA RAW;

-- TRUE = Il y a des changements en attente
-- FALSE = Aucun changement (stream vide)

SELECT SYSTEM$STREAM_HAS_DATA('RAW.STREAM_RAW_MATCHES') AS HAS_DATA_MATCHES;
SELECT SYSTEM$STREAM_HAS_DATA('RAW.STREAM_RAW_SCORERS') AS HAS_DATA_SCORERS;
SELECT SYSTEM$STREAM_HAS_DATA('RAW.STREAM_RAW_STANDINGS') AS HAS_DATA_STANDINGS;
SELECT SYSTEM$STREAM_HAS_DATA('RAW.STREAM_RAW_TEAMS') AS HAS_DATA_TEAMS;
    """, language="sql")

# Example 3: See stream content
with st.expander("Voir le contenu d'un stream"):
    st.code("""
USE ROLE SNOWGOAL_ROLE;
USE WAREHOUSE SNOWGOAL_WH_XS;
USE DATABASE SNOWGOAL_DB;
USE SCHEMA RAW;

SELECT
    METADATA$ACTION,       -- Type de changement (INSERT, DELETE)
    METADATA$ISUPDATE,     -- TRUE si c'est un UPDATE
    METADATA$ROW_ID,       -- ID unique de la ligne
    DATA,                  -- Données JSON
    LOADED_AT
FROM RAW.STREAM_RAW_MATCHES
LIMIT 10;
    """, language="sql")

# Example 4: Count pending changes
with st.expander("Compter les changements en attente"):
    st.code("""
USE ROLE SNOWGOAL_ROLE;
USE WAREHOUSE SNOWGOAL_WH_XS;
USE DATABASE SNOWGOAL_DB;
USE SCHEMA RAW;

-- Nombre de changements dans chaque stream
SELECT 'MATCHES' AS STREAM, COUNT(*) AS PENDING_CHANGES
FROM RAW.STREAM_RAW_MATCHES
UNION ALL
SELECT 'SCORERS', COUNT(*) FROM RAW.STREAM_RAW_SCORERS
UNION ALL
SELECT 'STANDINGS', COUNT(*) FROM RAW.STREAM_RAW_STANDINGS
UNION ALL
SELECT 'TEAMS', COUNT(*) FROM RAW.STREAM_RAW_TEAMS;
    """, language="sql")

# Example 5: Test complete cycle
with st.expander("Tester le cycle complet (Avant/Après MERGE)"):
    st.markdown("**AVANT le MERGE :**")
    st.code("""
USE ROLE SNOWGOAL_ROLE;
USE WAREHOUSE SNOWGOAL_WH_XS;
USE DATABASE SNOWGOAL_DB;
USE SCHEMA RAW;

-- Vérifier l'état des streams AVANT
SELECT
    SYSTEM$STREAM_HAS_DATA('RAW.STREAM_RAW_MATCHES') AS MATCHES,
    SYSTEM$STREAM_HAS_DATA('RAW.STREAM_RAW_SCORERS') AS SCORERS,
    SYSTEM$STREAM_HAS_DATA('RAW.STREAM_RAW_STANDINGS') AS STANDINGS,
    SYSTEM$STREAM_HAS_DATA('RAW.STREAM_RAW_TEAMS') AS TEAMS;

-- Résultat attendu : TRUE, TRUE, TRUE, TRUE
    """, language="sql")

    st.markdown("**Exécuter le pipeline :**")
    st.code("""
USE ROLE SNOWGOAL_ROLE;
USE WAREHOUSE SNOWGOAL_WH_XS;
USE DATABASE SNOWGOAL_DB;
USE SCHEMA COMMON;

-- Déclencher manuellement le pipeline complet
EXECUTE TASK COMMON.TASK_FETCH_ALL_LEAGUES;

-- Attendre 2-3 minutes...
    """, language="sql")

    st.markdown("**APRÈS le MERGE :**")
    st.code("""
USE ROLE SNOWGOAL_ROLE;
USE WAREHOUSE SNOWGOAL_WH_XS;
USE DATABASE SNOWGOAL_DB;
USE SCHEMA RAW;

-- Vérifier l'état des streams APRÈS
SELECT
    SYSTEM$STREAM_HAS_DATA('RAW.STREAM_RAW_MATCHES') AS MATCHES,
    SYSTEM$STREAM_HAS_DATA('RAW.STREAM_RAW_SCORERS') AS SCORERS,
    SYSTEM$STREAM_HAS_DATA('RAW.STREAM_RAW_STANDINGS') AS STANDINGS,
    SYSTEM$STREAM_HAS_DATA('RAW.STREAM_RAW_TEAMS') AS TEAMS;

-- Résultat attendu : FALSE, FALSE, FALSE, FALSE (streams vides)
    """, language="sql")

st.divider()

# Task Orchestration
st.header("🔄 Task Orchestration (DAG)")

st.markdown("""
### Execution Flow

```
TASK_FETCH_ALL_LEAGUES (Root Task)
    ↓
    Schedule: CRON 0 7,17,0 * * * Europe/Paris
    Action: Call FETCH_ALL_LEAGUES() procedure

    ↓

TASK_MERGE_TO_SILVER
    ↓
    Trigger: AFTER TASK_FETCH_ALL_LEAGUES
    Action: MERGE RAW → SILVER for all tables

    ↓

5 Parallel Tasks (AFTER TASK_MERGE_TO_SILVER):
    ├── TASK_REFRESH_LEAGUE_STANDINGS
    ├── TASK_REFRESH_TOP_SCORERS
    ├── TASK_REFRESH_TEAM_STATS
    ├── TASK_REFRESH_RECENT_MATCHES
    └── TASK_REFRESH_UPCOMING_FIXTURES
```

**Total Tasks:** 7
**Warehouse:** SNOWGOAL_WH_XS (auto-suspend: 60s)
**Refresh Frequency:** 3x daily (7h, 17h, 00h)
""")

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
### Pipeline Execution Order

1. **07:00, 17:00, 00:00** - `TASK_FETCH_ALL_LEAGUES`
   - Calls Snowpark procedure `FETCH_ALL_LEAGUES()`
   - Fetches data from football-data.org API
   - Inserts JSON into RAW tables

2. **After step 1** - `TASK_MERGE_TO_SILVER`
   - MERGE operations for 5 tables:
     - Matches
     - Standings
     - Scorers
     - Teams
     - Competitions
   - Incremental updates based on Streams CDC

3. **After step 2** - **5 parallel GOLD refresh tasks**
   - All execute simultaneously using `INSERT OVERWRITE`
   - Full refresh of aggregated tables
   - No dependencies between them

**Estimated execution time:** 2-3 minutes per run
**Daily credit consumption:** < 0.1 credits
""")

st.divider()

# Cost Optimization
st.header("💰 Cost Optimization")

st.markdown("""
### Strategy

**Before optimization:**
- Dynamic Tables with `TARGET_LAG = '1 hour'`
- Refreshed 24x per day
- **~6 credits/day** ❌

**After optimization:**
- Standard tables + Task-based refresh
- Controlled refresh 3x per day
- **< 0.1 credits/day** ✅

### Key Optimizations

1. **Warehouse auto-suspend**: 60 seconds
2. **Controlled refresh**: Only when new data arrives
3. **Parallel execution**: GOLD tasks run simultaneously
4. **Incremental MERGE**: Only process changed records in SILVER
5. **INSERT OVERWRITE**: Fast full refresh for GOLD (small tables)
""")

st.divider()

# Security
st.header("🔐 Security")

st.markdown("""
### Access Control

- **Role-Based Access Control (RBAC)**
  - Role: `SNOWGOAL_ROLE`
  - Warehouse: `SNOWGOAL_WH_XS`
  - Database: `SNOWGOAL_DB`

### API Security

- **External Access Integration**
  - Allowed network rule: `football-data.org`
  - Secret: API key stored in Snowflake
  - No credentials in code

### Best Practices

- No hardcoded credentials
- Minimal privilege principle
- Encrypted data at rest (Snowflake default)
- Audit logs via ACCOUNT_USAGE
""")

st.divider()

# Footer
st.caption("📖 SnowGoal Documentation | Built 100% on Snowflake Native Features")
