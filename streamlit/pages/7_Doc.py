"""
SnowGoal - Documentation Page
"""

import streamlit as st
from pathlib import Path

st.set_page_config(page_title="Doc | SnowGoal", page_icon="📖", layout="wide")

st.title("📖 Documentation")

# Architecture Diagram
st.header("📊 Data Pipeline Flow")

st.markdown("""
```
football-data.org API + The Odds API
        |
        ↓ (Snowpark Python Procedures)
RAW Layer (VARIANT JSON)
        |
        ↓ (Streams CDC)
STAGING Views (reads from Streams + LATERAL FLATTEN)
        |
        ↓ (MERGE incremental via Tasks)
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

# Data Model (ERD)
st.header("🗄️ Data Model (ERD)")

st.markdown("""
SILVER Layer schema showing all tables and their relationships:
""")

# Display ERD Image
erd_path = Path(__file__).parent.parent / "assets" / "dbml_snowgoal.png"
st.image(str(erd_path), width=900)

st.divider()

# Data Layers
st.header("📂 Data Layers")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("### 🟤 RAW Layer (Bronze)")
    st.markdown("""
    **Format:** VARIANT (JSON)

    **Tables:**
    - `RAW_MATCHES`
    - `RAW_STANDINGS`
    - `RAW_SCORERS`
    - `RAW_TEAMS`
    - `RAW_COMPETITIONS`
    - `RAW_ODDS` 🎲

    **CDC:** Streams capture changes
    """)

with col2:
    st.markdown("### ⚪ SILVER Layer")
    st.markdown("""
    **Format:** Structured tables

    **Tables:**
    - `MATCHES`
    - `STANDINGS`
    - `SCORERS`
    - `TEAMS`
    - `COMPETITIONS`
    - `ODDS` 🎲

    **Updates:** MERGE incremental
    """)

with col3:
    st.markdown("### 🟡 GOLD Layer")
    st.markdown("""
    **Format:** Aggregations

    **Tables (9):**
    - `LEAGUE_STANDINGS`
    - `TOP_SCORERS`
    - `TEAM_STATS`
    - `RECENT_MATCHES`
    - `UPCOMING_FIXTURES`
    - `MATCH_PATTERNS` ⭐
    - `REFEREE_STATS` ⭐
    - `GEOGRAPHIC_STATS` ⭐
    - `ODDS_ANALYSIS` 🎲

    **Refresh:** INSERT OVERWRITE via Tasks
    """)

st.divider()

# CDC Section
st.header("🔄 CDC (Change Data Capture)")

st.markdown("""
### What is CDC?

**Change Data Capture** automatically captures modifications (INSERT, UPDATE, DELETE) in RAW tables using Snowflake **Streams**.

### Why use CDC?
- ✅ **Performance**: Process only changes, not all data
- ✅ **Cost reduction**: Less compute = fewer credits
- ✅ **Traceability**: Track modification history
""")

st.subheader("📌 SQL Query Examples")

# Example 1: Check streams exist
with st.expander("Check that streams exist"):
    st.code("""
USE ROLE SNOWGOAL_ROLE;
USE WAREHOUSE SNOWGOAL_WH_XS;
USE DATABASE SNOWGOAL_DB;
USE SCHEMA RAW;

SHOW STREAMS IN SCHEMA RAW;

-- Expected result: 4 streams (MATCHES, SCORERS, STANDINGS, TEAMS)
    """, language="sql")

# Example 2: Check if stream has data
with st.expander("Check if a stream contains data"):
    st.code("""
USE ROLE SNOWGOAL_ROLE;
USE WAREHOUSE SNOWGOAL_WH_XS;
USE DATABASE SNOWGOAL_DB;
USE SCHEMA RAW;

-- TRUE = There are pending changes
-- FALSE = No changes (empty stream)

SELECT SYSTEM$STREAM_HAS_DATA('RAW.STREAM_RAW_MATCHES') AS HAS_DATA_MATCHES;
SELECT SYSTEM$STREAM_HAS_DATA('RAW.STREAM_RAW_SCORERS') AS HAS_DATA_SCORERS;
SELECT SYSTEM$STREAM_HAS_DATA('RAW.STREAM_RAW_STANDINGS') AS HAS_DATA_STANDINGS;
SELECT SYSTEM$STREAM_HAS_DATA('RAW.STREAM_RAW_TEAMS') AS HAS_DATA_TEAMS;
SELECT SYSTEM$STREAM_HAS_DATA('RAW.STREAM_RAW_ODDS') AS HAS_DATA_ODDS;
    """, language="sql")

# Example 3: See stream content
with st.expander("View stream content"):
    st.code("""
USE ROLE SNOWGOAL_ROLE;
USE WAREHOUSE SNOWGOAL_WH_XS;
USE DATABASE SNOWGOAL_DB;
USE SCHEMA RAW;

SELECT
    METADATA$ACTION,       -- Change type (INSERT, DELETE)
    METADATA$ISUPDATE,     -- TRUE if it's an UPDATE
    METADATA$ROW_ID,       -- Unique row ID
    DATA,                  -- JSON data
    LOADED_AT
FROM RAW.STREAM_RAW_MATCHES
LIMIT 10;
    """, language="sql")

# Example 4: Count pending changes
with st.expander("Count pending changes"):
    st.code("""
USE ROLE SNOWGOAL_ROLE;
USE WAREHOUSE SNOWGOAL_WH_XS;
USE DATABASE SNOWGOAL_DB;
USE SCHEMA RAW;

-- Number of changes in each stream
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
with st.expander("Test complete cycle (Before/After MERGE)"):
    st.markdown("**BEFORE MERGE:**")
    st.code("""
USE ROLE SNOWGOAL_ROLE;
USE WAREHOUSE SNOWGOAL_WH_XS;
USE DATABASE SNOWGOAL_DB;
USE SCHEMA RAW;

-- Check streams state BEFORE
SELECT
    SYSTEM$STREAM_HAS_DATA('RAW.STREAM_RAW_MATCHES') AS MATCHES,
    SYSTEM$STREAM_HAS_DATA('RAW.STREAM_RAW_SCORERS') AS SCORERS,
    SYSTEM$STREAM_HAS_DATA('RAW.STREAM_RAW_STANDINGS') AS STANDINGS,
    SYSTEM$STREAM_HAS_DATA('RAW.STREAM_RAW_TEAMS') AS TEAMS;

-- Expected result: TRUE, TRUE, TRUE, TRUE
    """, language="sql")

    st.markdown("**Execute pipeline:**")
    st.code("""
USE ROLE SNOWGOAL_ROLE;
USE WAREHOUSE SNOWGOAL_WH_XS;
USE DATABASE SNOWGOAL_DB;
USE SCHEMA COMMON;

-- Manually trigger the complete pipeline
EXECUTE TASK COMMON.TASK_FETCH_ALL_LEAGUES;

-- Wait 2-3 minutes...
    """, language="sql")

    st.markdown("**AFTER MERGE:**")
    st.code("""
USE ROLE SNOWGOAL_ROLE;
USE WAREHOUSE SNOWGOAL_WH_XS;
USE DATABASE SNOWGOAL_DB;
USE SCHEMA RAW;

-- Check streams state AFTER
SELECT
    SYSTEM$STREAM_HAS_DATA('RAW.STREAM_RAW_MATCHES') AS MATCHES,
    SYSTEM$STREAM_HAS_DATA('RAW.STREAM_RAW_SCORERS') AS SCORERS,
    SYSTEM$STREAM_HAS_DATA('RAW.STREAM_RAW_STANDINGS') AS STANDINGS,
    SYSTEM$STREAM_HAS_DATA('RAW.STREAM_RAW_TEAMS') AS TEAMS;

-- Expected result: FALSE, FALSE, FALSE, FALSE (empty streams)
    """, language="sql")

st.divider()

# Task Orchestration
st.header("🔄 Task Orchestration (DAG)")

st.markdown("""
### Execution Flow

**Total Tasks:** 10
**Refresh Frequency:** 3x daily (7h, 17h, 00h)

The CRON schedule is optimized for European football match times:
- **7h (morning):** Captures overnight results and lineup updates
- **17h (afternoon):** Pre-match data before evening fixtures (most matches start 18h-21h)
- **00h (midnight):** Post-match results and final statistics
""")

# Display Task Graph
task_graph_path = Path(__file__).parent.parent / "assets" / "task_arch_snowgoal.png"
st.image(str(task_graph_path), width=900)

st.divider()

# --- SECTION LOGGING & OBSERVABILITÉ ---
st.header("🛡️ Logging & Observabilité")

st.markdown("""
### Pipeline Monitoring
Afin de garantir la fiabilité des données et la transparence des flux, le projet intègre un système de monitoring centralisé. Chaque procédure Snowpark communique son état de santé à une table technique.

- **Table de Logs :** `SNOWGOAL_DB.COMMON.PIPELINE_LOGS`
- **Niveaux de Log :**
    - `INFO` : Exécution réussie à 100%.
    - `WARNING` : **Succès Partiel**. Utilisé notamment pour la gestion du *Rate Limiting* (ex: 10 ligues sur 11 chargées si l'API est saturée).
    - `ERROR` : Échec critique nécessitant une intervention.
""")

col1, col2 = st.columns(2)
with col1:
    st.subheader("💡 Résilience")
    st.write("""
    Le pipeline utilise une stratégie de **Graceful Degradation** : si l'appel à une ligue spécifique échoue (404 ou 429), le script logue l'erreur, passe à la suivante et termine son exécution au lieu de tout bloquer.
    """)
with col2:
    st.subheader("🚀 Performance")
    st.write("""
    L'ingestion utilise le **Batch Inserting** (via Pandas et `write_pandas`). Les logs permettent de valider le volume de records insérés à chaque run.
    """)

with st.expander("🔍 Voir les derniers événements du pipeline (Live)"):
    try:
        # Import local pour utiliser ta fonction de connexion existante
        from connection import run_query
        
        logs_query = """
            SELECT 
                EVENT_TIME, 
                LEVEL, 
                COMPONENT_NAME, 
                MESSAGE,
                STACK_TRACE
            FROM SNOWGOAL_DB.COMMON.PIPELINE_LOGS 
            ORDER BY EVENT_TIME DESC 
            LIMIT 10
        """
        logs_df = run_query(logs_query)
        st.dataframe(
            logs_df,
            column_config={
                "EVENT_TIME": st.column_config.DatetimeColumn("Timestamp"),
                "MESSAGE": st.column_config.TextColumn("Résumé", width="large"),
                "STACK_TRACE": st.column_config.TextColumn("Détails Techniques", width="medium"),
            },
            use_container_width=True,
            hide_index=True
        )
    except Exception as e:
        st.error(f"Erreur de lecture des logs : {e}")

st.divider()

# Technical Stack
st.header("⚙️ Technical Stack")

col1, col2 = st.columns(2)

with col1:
    st.markdown("### Data Layer")
    st.markdown("""
    - **Storage** : Snowflake Tables (VARIANT for JSON)
    - **Processing** : Snowpark Python Procedures
    - **CDC** : Streams for change capture
    - **Orchestration** : Tasks with DAG dependencies
    - **Scheduling** : CRON (7h, 17h, 00h)
    """)

with col2:
    st.markdown("### Infrastructure")
    st.markdown("""
    - **Warehouse** : SNOWGOAL_WH_XS (auto-suspend 60s)
    - **APIs** :
      - football-data.org (matches, standings, scorers)
      - The Odds API (betting odds)
    - **Security** : External Access Integration + Secrets
    - **Frontend** : Streamlit Cloud
    - **Version Control** : Git/GitHub
    """)

st.divider()

# --- SECTION DATA QUALITY ---
st.divider()
st.header("✅ Audit de Qualité des Données (Data Quality)")

st.info("""
**Pourquoi cette section ?** Un pipeline robuste doit s'auto-contrôler. 
Ici, nous interrogeons une vue Snowflake qui analyse les anomalies potentielles 
sur les scores, les cotes et la cohérence des classements à chaque rafraîchissement.
""")

try:
    # Exécution de la requête sur la vue de monitoring
    dq_df = run_query("SELECT * FROM GOLD.DATA_QUALITY_DASHBOARD")
    
    # Calcul du nombre total d'anomalies
    total_anomalies = dq_df['ANOMALY_COUNT'].sum()
    
    if total_anomalies == 0:
        st.success("🎯 **Qualité Optimale** : Aucune anomalie détectée dans les tables Silver.")
    else:
        # On affiche un warning car le pipeline tourne, mais avec des points d'attention
        st.warning(f"⚠️ **{total_anomalies} anomalies détectées** dans les données sources.")
        
        # Le petit plus pour l'entretien : expliquer l'anomalie Leicester/Sheffield
        st.markdown("""
        > **Note technique** : Les anomalies détectées sur les points (Championship) correspondent à des 
        > sanctions administratives réelles (retraits de points). Le système identifie correctement 
        > l'écart entre les résultats sportifs et le classement officiel.
        """)

    # Affichage du tableau de bord de monitoring
    st.table(dq_df)

except Exception as e:
    st.error(f"Erreur lors de la récupération des métriques de qualité : {e}")

# Refresh Schedule
st.header("⏰ Refresh Schedule")

st.info("🕐 Data refreshes automatically **3 times daily**: **7h**, **17h**, and **00h** (Europe/Paris timezone)")

st.markdown("""
### Pipeline Execution Order

1. **07:00, 17:00, 00:00** - `TASK_FETCH_ALL_LEAGUES`
   - Calls Snowpark procedure `FETCH_ALL_LEAGUES()`
   - Fetches data from football-data.org API
   - Inserts JSON into RAW tables

2. **After step 1** - `TASK_FETCH_ODDS`
   - Calls Snowpark procedure `FETCH_ODDS()`
   - Fetches betting odds from The Odds API
   - Inserts JSON into RAW_ODDS table
   - Covers all 11 competitions

3. **After step 2** - `TASK_MERGE_TO_SILVER`
   - Calls stored procedure `SP_MERGE_TO_SILVER()`
   - MERGE operations for 6 tables:
     - Matches
     - Standings
     - Scorers
     - Teams
     - Competitions
     - Odds 🎲
   - Incremental updates based on Streams CDC

4. **After step 3** - **9 parallel GOLD refresh tasks**
   - All execute simultaneously using `INSERT OVERWRITE`
   - Full refresh of aggregated tables:
     - 5 Business Intelligence tables
     - 3 Advanced Analytics tables (using enrichment columns)
     - 1 Betting Analytics table (ODDS_ANALYSIS) 🎲
   - No dependencies between them

**Estimated execution time:** 50mn**
**Rate limit 10 calls per minutes
""")

st.divider()

# Betting Odds Integration
st.header("🎲 Betting Odds Integration")

st.markdown("""
### Data Source
**The Odds API** provides real-time betting odds from multiple bookmakers for all 11 competitions.

### Coverage
- 🏴󠁧󠁢󠁥󠁮󠁧󠁿 Premier League (PL)
- 🇪🇸 La Liga (PD)
- 🇩🇪 Bundesliga (BL1)
- 🇮🇹 Serie A (SA)
- 🇫🇷 Ligue 1 (FL1)
- 🏆 Champions League (CL)
- 🇪🇺 European Championship (EC)
- 🇵🇹 Primeira Liga (PPL)
- 🇳🇱 Eredivisie (DED)
- 🏴󠁧󠁢󠁥󠁮󠁧󠁿 Championship (ELC)
- 🇧🇷 Brasileirão (BSA)

### Data Points
For each upcoming match:
- **Home/Draw/Away odds** from 10+ bookmakers
- **Implied probabilities** (calculated from odds)
- **Bookmaker margins** (overround %)
- **Best odds** per outcome
- **Average odds** across all bookmakers
- **Last update** timestamp

### Analytics Features
The **Betting Intelligence** dashboard page provides:
1. **Upcoming Matches** - Real-time odds and probabilities
2. **Bookmaker Comparison** - Find best odds per outcome
3. **Value Bets Detector** - Compare odds vs historical H2H data
4. **Match Predictor** - Combine team form (90-day PPG) with odds
5. **Odds Summary** - Games covered, bookmakers count, avg margin

### API Rate Limits
- **500 requests/month** on free tier
- **~120 requests/month used** (11 leagues × 3 fetches/day × 30 days ÷ 8)
- Well within limits ✅
""")

st.divider()

# Cost Optimization
st.header("💰 Cost Optimization")

st.markdown("""
### Key Optimizations

1. **Streams (CDC) - Major Cost Saver** 🎯
   - **Without Streams**: MERGE would scan all 1,782+ matches on every refresh → high compute cost
   - **With Streams**: Only process changed records (e.g., 10-50 updated matches) → 95%+ compute reduction
   - Streams use Time Travel metadata (no data duplication), MERGE reads only deltas
   - Zero compute cost when idle, only consumes resources during MERGE execution

2. **Warehouse auto-suspend**: 60 seconds
   - Minimizes idle compute costs

3. **Controlled refresh**: 3x per day (7h, 17h, 00h)
   - Aligned with match schedules, no unnecessary processing

4. **Parallel execution**: GOLD tasks run simultaneously
   - Reduces total execution time

5. **XS Warehouse**: Smallest size for this workload
   - Cost-effective for small data volumes

### Cost Efficiency

- **Streams (CDC)** = Incremental processing instead of full scans
- Task-based orchestration with controlled refresh schedule
- XS warehouse with 60s auto-suspend
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
  - Allowed network rule : `football-data.org`
  - Secret : API key stored in Snowflake
  - No credentials in code

### Best Practices

- No hardcoded credentials
- Minimal privilege principle
- Encrypted data at rest (Snowflake default)
- Audit logs via ACCOUNT_USAGE
""")

st.divider()

# Footer
st.caption("📖 SnowGoal Documentation | Season 2025-2026 | Built 100% on Snowflake Native Features")
