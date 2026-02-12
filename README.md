# ⚽ SnowGoal

**Pipeline d'analyse football 100% Snowflake natif** — Top 5 ligues européennes

## 🏗️ Architecture

```
football-data.org API
        │
        ▼
Stored Procedure Snowpark (Task cron)
        │
        ▼
RAW tables (VARIANT JSON)
        │
        ▼
Stream (CDC)
        │
        ▼
Staging Views (FLATTEN)
        │
        ▼
Silver Tables (MERGE)
        │
        ├──► Gold Tables (Snowpark Python)
        │
        └──► Dynamic Tables (auto-refresh)
                │
                ▼
        Streamlit-in-Snowflake
```

## 📁 Structure

```
snowgoal/
├── deploy/                    # Scripts SQL déployables
│   ├── 00_init/              # Setup database, schemas, warehouses
│   ├── 01_raw/               # Tables RAW + Snowpipe + Stages
│   ├── 02_staging/           # Views staging (FLATTEN)
│   ├── 03_silver/            # Tables Silver + MERGE
│   ├── 04_gold/              # Dynamic Tables + Gold tables
│   ├── 05_tasks/             # Tasks + DAG orchestration
│   └── 06_security/          # RBAC + Masking policies
├── snowpark/                  # Code Python Snowpark
│   ├── procedures/           # Stored procedures (API fetch, scoring)
│   └── udfs/                 # User-defined functions
├── streamlit/                 # Dashboard Streamlit-in-Snowflake
├── tests/                     # Tests SQL + Python
├── docs/                      # Documentation PDF
└── .github/workflows/         # CI/CD GitHub Actions
```

## 🛠️ Stack

| Composant | Technologie |
|-----------|-------------|
| Source | football-data.org API |
| Extraction | Snowpark Python Stored Procedure |
| Ingestion | Snowpipe + Internal Stage |
| Stockage | Tables VARIANT (JSON) |
| CDC | Streams |
| Orchestration | Tasks + DAG natif |
| Transformations | SQL (MERGE, FLATTEN) + Snowpark |
| Gold Layer | Dynamic Tables |
| Security | RBAC + Masking Policies |
| Dashboard | Streamlit-in-Snowflake |
| CI/CD | GitHub Actions |

## 🏆 Ligues couvertes

| Ligue | Code | Pays |
|-------|------|------|
| Premier League | PL | 🏴󠁧󠁢󠁥󠁮󠁧󠁿 Angleterre |
| La Liga | PD | 🇪🇸 Espagne |
| Bundesliga | BL1 | 🇩🇪 Allemagne |
| Serie A | SA | 🇮🇹 Italie |
| Ligue 1 | FL1 | 🇫🇷 France |

## ⚡ Features Snowflake

- Snowpipe (auto-ingest)
- Streams (CDC)
- Tasks + DAG
- Dynamic Tables
- VARIANT + FLATTEN
- Snowpark Python
- RBAC
- Masking Policies
- Time Travel
- Zero-Copy Cloning
- Resource Monitors
- Streamlit-in-Snowflake
- Internal Stages
- Clustering Keys

## 🚀 Déploiement

```bash
# 1. Setup initial
snowsql -f deploy/00_init/setup.sql

# 2. Déployer les couches dans l'ordre
snowsql -f deploy/01_raw/tables.sql
snowsql -f deploy/02_staging/views.sql
# ...
```

## 📊 Dashboard

5 pages interactives :
- **Overview** : Classements, stats ligues
- **Match Center** : Résultats, stats par match
- **Team Analysis** : Analyse détaillée par équipe
- **Player Stats** : Top scorers, assists
- **Head-to-Head** : Comparaison équipes

---

#Snowflake #DataEngineering #Football #Snowpark #Streamlit
