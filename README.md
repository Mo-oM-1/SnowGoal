# SnowGoal

**Pipeline d'analyse football 100% Snowflake natif** — Top 5 ligues europeennes

---

## Architecture

```
football-data.org API
        |
        v
Stored Procedure Snowpark (Task cron)
        |
        v
RAW tables (VARIANT JSON)
        |
        v
Stream (CDC)
        |
        v
Staging Views (FLATTEN)
        |
        v
Silver Tables (MERGE)
        |
        v
Dynamic Tables (auto-refresh)
        |
        v
Streamlit-in-Snowflake
```

---

## Ligues couvertes

| Ligue | Code | Pays |
|-------|------|------|
| Premier League | PL | Angleterre |
| La Liga | PD | Espagne |
| Bundesliga | BL1 | Allemagne |
| Serie A | SA | Italie |
| Ligue 1 | FL1 | France |

---

## Features Snowflake

| Feature | Description |
|---------|-------------|
| VARIANT Columns | Stockage JSON semi-structure |
| Streams (CDC) | Capture des changements en temps reel |
| Dynamic Tables | Rafraichissement automatique des agregations |
| Snowpark Python | Procedures stockees en Python |
| External Access | Integration securisee avec APIs externes |
| Secrets Management | Stockage securise des cles API |
| Tasks + DAG | Orchestration native avec dependances |
| MERGE | Chargement incremental avec upsert |
| Streamlit in Snowflake | Dashboard natif |
| RBAC | Controle d'acces par roles |
| Masking Policies | Protection des donnees sensibles |
| Internal Stages | Stockage des fichiers Python et Streamlit |

---

## Modele de Donnees

### Schemas

| Schema | Description | Objets |
|--------|-------------|--------|
| RAW | Donnees brutes JSON | RAW_MATCHES, RAW_TEAMS, RAW_STANDINGS... |
| STAGING | Views LATERAL FLATTEN | V_MATCHES, V_TEAMS, V_STANDINGS... |
| SILVER | Tables nettoyees | MATCHES, TEAMS, STANDINGS, SCORERS... |
| GOLD | Dynamic Tables | DT_LEAGUE_STANDINGS, DT_TOP_SCORERS... |
| COMMON | Objets partages | Stages, Secrets, Procedures |

### Tables Silver

| Table | Colonnes Cles |
|-------|---------------|
| MATCHES | MATCH_ID, HOME_TEAM, AWAY_TEAM, SCORE |
| STANDINGS | TEAM_ID, POSITION, POINTS, WON, LOST |
| TEAMS | TEAM_ID, TEAM_NAME, VENUE, COACH |
| SCORERS | PLAYER_ID, GOALS, ASSISTS, TEAM |
| COMPETITIONS | COMPETITION_CODE, NAME, AREA |

---

## Pipeline ETL

| Etape | Source | Destination | Methode |
|-------|--------|-------------|---------|
| Extract | football-data.org | RAW.RAW_* | Snowpark + External Access |
| Flatten | RAW.RAW_* | STAGING.V_* | Views + LATERAL FLATTEN |
| Transform | STAGING.V_* | SILVER.* | MERGE (incremental) |
| Aggregate | SILVER.* | GOLD.DT_* | Dynamic Tables (auto) |

### Orchestration

| Task | Schedule | Description |
|------|----------|-------------|
| TASK_FETCH_ALL_LEAGUES | Every 6 hours | Fetch 5 leagues (60s delay each) |
| TASK_MERGE_TO_SILVER | After FETCH | MERGE into Silver tables |
| Dynamic Tables | Auto (30min-1h) | Refresh automatique |

---

## Deploiement

| # | Script | Description |
|---|--------|-------------|
| 1 | `00_init/01_database.sql` | Database, Schemas, Warehouses |
| 2 | `00_init/02_file_formats.sql` | JSON format, Stages |
| 3 | `01_raw/01_tables.sql` | Tables RAW (VARIANT) |
| 4 | `01_raw/02_streams.sql` | Streams CDC |
| 5 | Creer Secret + Upload Python | FOOTBALL_API_KEY + fichiers .py |
| 6 | `01_raw/03_stored_procedure.sql` | Network Rule, Integration, Procedure |
| 7 | `01_raw/04_fetch_all_procedure.sql` | Procedure FETCH_ALL_LEAGUES |
| 8 | `CALL FETCH_ALL_LEAGUES()` | Chargement initial (5 min) |
| 9 | `02_staging/01_views.sql` | Views FLATTEN |
| 10 | `03_silver/01_tables.sql` | Tables Silver |
| 11 | `03_silver/02_merge.sql` | MERGE RAW vers Silver |
| 12 | `04_gold/01_dynamic_tables.sql` | Dynamic Tables |
| 13 | `05_tasks/01_tasks.sql` | Tasks DAG |
| 14 | `06_security/01_rbac.sql` | RBAC + Masking |
| 15 | `07_streamlit/01_deploy_app.sql` | Dashboard Streamlit |

---

## Dashboard Streamlit

| Page | Description |
|------|-------------|
| Home | Vue d'ensemble avec stats live |
| Standings | Classements par ligue |
| Top Scorers | Podium des meilleurs buteurs |
| Matches | Resultats et prochains matchs |
| Teams | Details equipes avec stats Home/Away |

Acces : `Snowsight > Streamlit > SNOWGOAL_DASHBOARD`

---

## Structure

```
snowgoal/
├── deploy/
│   ├── 00_init/
│   ├── 01_raw/
│   ├── 02_staging/
│   ├── 03_silver/
│   ├── 04_gold/
│   ├── 05_tasks/
│   ├── 06_security/
│   └── 07_streamlit/
├── snowpark/
│   └── procedures/
├── streamlit/
│   ├── Home.py
│   └── pages/
└── docs/
```

---

## Configuration

### Prerequis

- Compte Snowflake (Standard ou superieur)
- Cle API football-data.org (Free Tier)

### Secret API

```sql
USE DATABASE SNOWGOAL_DB;
USE SCHEMA COMMON;

CREATE SECRET FOOTBALL_API_KEY
    TYPE = GENERIC_STRING
    SECRET_STRING = '<your-api-key>';
```

---

## Source

- **API** : football-data.org
- **Refresh** : Automatique toutes les 6 heures
- **Architecture** : Medallion (RAW - SILVER - GOLD)
