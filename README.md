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
Gold Tables (Task-based refresh)
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
| Streams (CDC) | Capture des changements |
| Tasks + DAG | Orchestration native avec dependances (7h, 17h, 00h) |
| Snowpark Python | Procedures stockees en Python |
| External Access | Integration securisee avec APIs externes |
| Secrets Management | Stockage securise des cles API |
| MERGE | Chargement incremental avec upsert |
| INSERT OVERWRITE | Refresh complet des tables Gold |
| Streamlit in Snowflake | Dashboard natif |
| Internal Stages | Stockage des fichiers Python et Streamlit |

---

## Modele de Donnees

### Schemas

| Schema | Description | Objets |
|--------|-------------|--------|
| RAW | Donnees brutes JSON | RAW_MATCHES, RAW_TEAMS, RAW_STANDINGS... |
| STAGING | Views LATERAL FLATTEN | V_MATCHES, V_TEAMS, V_STANDINGS... |
| SILVER | Tables nettoyees | MATCHES, TEAMS, STANDINGS, SCORERS... |
| GOLD | Tables agregees | LEAGUE_STANDINGS, TOP_SCORERS, TEAM_STATS... |
| COMMON | Objets partages | Stages, Secrets, Procedures, Tasks |

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
| Aggregate | SILVER.* | GOLD.* | INSERT OVERWRITE (Tasks 3x/jour) |

---

## Automatisation

Le pipeline est **100% automatique** apres le deploiement initial. Aucune intervention manuelle n'est requise.

### Flux de donnees automatise

```
[Toutes les 6h - CRON]
        |
        v
TASK_FETCH_ALL_LEAGUES
   - Appelle football-data.org API
   - Recupere 5 ligues avec 60s entre chaque (rate limiting)
   - Insere dans RAW.RAW_MATCHES, RAW_TEAMS, RAW_STANDINGS, RAW_SCORERS
   - Duree: ~5 minutes
        |
        v
TASK_MERGE_TO_SILVER (declenchee automatiquement)
   - MERGE incremental vers SILVER.MATCHES
   - MERGE incremental vers SILVER.STANDINGS
   - MERGE incremental vers SILVER.TEAMS
   - MERGE incremental vers SILVER.SCORERS
   - Deduplication avec QUALIFY ROW_NUMBER()
        |
        v
DYNAMIC TABLES (refresh automatique 30-60 min)
   - DT_LEAGUE_STANDINGS : classements enrichis
   - DT_TOP_SCORERS : meilleurs buteurs avec stats
   - DT_TEAM_STATS : statistiques par equipe
        |
        v
STREAMLIT DASHBOARD (lecture temps reel)
   - Affiche les Dynamic Tables
   - Aucun refresh manuel necessaire
```

### Composants automatises

| Composant | Type | Frequence | Description |
|-----------|------|-----------|-------------|
| TASK_FETCH_ALL_LEAGUES | Task CRON | Toutes les 6h | `0 */6 * * * UTC` |
| TASK_MERGE_TO_SILVER | Task DAG | Apres FETCH | Dependance `AFTER` |
| DT_LEAGUE_STANDINGS | Dynamic Table | 30 min | `TARGET_LAG = '30 minutes'` |
| DT_TOP_SCORERS | Dynamic Table | 30 min | `TARGET_LAG = '30 minutes'` |
| DT_TEAM_STATS | Dynamic Table | 1 heure | `TARGET_LAG = '1 hour'` |
| Streamlit Data | Requetes SQL | Temps reel | Lecture des DT Gold |

### Gestion des Tasks

```sql
-- Verifier le statut des tasks
SHOW TASKS IN SCHEMA COMMON;

-- Voir l'historique d'execution
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME LIKE 'TASK_%'
ORDER BY SCHEDULED_TIME DESC
LIMIT 20;

-- Suspendre les tasks (maintenance)
ALTER TASK TASK_FETCH_ALL_LEAGUES SUSPEND;
ALTER TASK TASK_MERGE_TO_SILVER SUSPEND;

-- Reprendre les tasks
ALTER TASK TASK_MERGE_TO_SILVER RESUME;
ALTER TASK TASK_FETCH_ALL_LEAGUES RESUME;

-- Execution manuelle (test)
EXECUTE TASK TASK_FETCH_ALL_LEAGUES;
```

### Ce qui N'EST PAS automatique

| Element | Action requise |
|---------|----------------|
| Code Streamlit | `PUT` vers stage apres modification |
| Procedures Python | `PUT` vers stage apres modification |
| Schema changes | Re-executer les scripts SQL |

### Surveillance

Les Dynamic Tables offrent une visibilite sur les rafraichissements :

```sql
-- Statut des Dynamic Tables
SELECT name, refresh_mode, target_lag, last_refresh_time
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES())
WHERE SCHEMA_NAME = 'GOLD';
```

### Orchestration DAG

```
TASK_FETCH_ALL_LEAGUES (root)
    |
    +---> TASK_MERGE_TO_SILVER (child)
              |
              +---> [Dynamic Tables auto-refresh]
```

Les tasks enfants s'executent **uniquement** si la task parent reussit.

---

## Deploiement

### Role dedie

Le projet utilise un role dedie `SNOWGOAL_ROLE` au lieu de `ACCOUNTADMIN`.

| # | Script | Role | Description |
|---|--------|------|-------------|
| 0 | `00_init/00_role.sql` | ACCOUNTADMIN | Creation du role (une seule fois) |
| 1 | `00_init/01_database.sql` | SNOWGOAL_ROLE | Database, Schemas, Warehouses |
| 2 | `00_init/02_file_formats.sql` | SNOWGOAL_ROLE | JSON format, Stages |
| 3 | `01_raw/01_tables.sql` | SNOWGOAL_ROLE | Tables RAW (VARIANT) |
| 4 | `01_raw/02_streams.sql` | SNOWGOAL_ROLE | Streams CDC |
| 5 | Creer Secret + Upload Python | SNOWGOAL_ROLE | FOOTBALL_API_KEY + fichiers .py |
| 6 | `01_raw/03_stored_procedure.sql` | SNOWGOAL_ROLE | Network Rule, Integration, Procedure |
| 7 | `01_raw/04_fetch_all_procedure.sql` | SNOWGOAL_ROLE | Procedure FETCH_ALL_LEAGUES |
| 8 | `CALL FETCH_ALL_LEAGUES()` | SNOWGOAL_ROLE | Chargement initial (5 min) |
| 9 | `02_staging/01_views.sql` | SNOWGOAL_ROLE | Views FLATTEN |
| 10 | `03_silver/01_tables.sql` | SNOWGOAL_ROLE | Tables Silver |
| 11 | `03_silver/02_merge.sql` | SNOWGOAL_ROLE | MERGE RAW vers Silver |
| 12 | `04_gold/01_dynamic_tables.sql` | SNOWGOAL_ROLE | Dynamic Tables |
| 13 | `05_tasks/01_tasks.sql` | SNOWGOAL_ROLE | Tasks DAG |
| 14 | `06_streamlit/01_deploy_app.sql` | SNOWGOAL_ROLE | Dashboard Streamlit |

Apres l'etape 0, utiliser toujours `USE ROLE SNOWGOAL_ROLE;` pour toutes les operations.

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
│   └── 06_streamlit/
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
USE ROLE SNOWGOAL_ROLE;
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
