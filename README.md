# SnowGoal

**Pipeline d'analyse football 100% Snowflake natif** — 11 compétitions internationales

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

## Compétitions couvertes (11)

**Top 5 Ligues Européennes**
| Ligue | Code | Pays |
|-------|------|------|
| Premier League | PL | Angleterre |
| La Liga | PD | Espagne |
| Bundesliga | BL1 | Allemagne |
| Serie A | SA | Italie |
| Ligue 1 | FL1 | France |

**Compétitions Internationales**
| Ligue | Code | Type |
|-------|------|------|
| Champions League | CL | UEFA |
| European Championship | EC | UEFA |

**Autres Ligues**
| Ligue | Code | Pays |
|-------|------|------|
| Primeira Liga | PPL | Portugal |
| Eredivisie | DED | Pays-Bas |
| Championship | ELC | Angleterre |
| Brasileirão | BSA | Brésil |

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
| GOLD | Tables agregees | LEAGUE_STANDINGS, TOP_SCORERS, TEAM_STATS, MATCH_PATTERNS, REFEREE_STATS, GEOGRAPHIC_STATS |
| COMMON | Objets partages | Stages, Secrets, Procedures, Tasks |

### Tables Silver

| Table | Colonnes Cles | Enrichissements |
|-------|---------------|-----------------|
| MATCHES | MATCH_ID, HOME_TEAM, AWAY_TEAM, SCORE | + REFEREE_NATIONALITY, REFEREE_ID, MATCH_DURATION, AREA_NAME, AREA_CODE, DAY_OF_WEEK, MATCH_HOUR, CURRENT_MATCHDAY |
| STANDINGS | TEAM_ID, POSITION, POINTS, WON, LOST | - |
| TEAMS | TEAM_ID, TEAM_NAME, VENUE, COACH | - |
| SCORERS | PLAYER_ID, GOALS, ASSISTS, TEAM | - |
| COMPETITIONS | COMPETITION_CODE, NAME, AREA | - |

### Tables Gold Analytics (nouvelles)

| Table | Description | Dimensions Analysées |
|-------|-------------|----------------------|
| MATCH_PATTERNS | Patterns temporels des matchs | DAY_OF_WEEK, MATCH_HOUR, Weekend vs Midweek, AVG_GOALS |
| REFEREE_STATS | Statistiques des arbitres | MATCHES_REFEREED, NATIONALITY, EXTRA_TIME%, AVG_GOALS |
| GEOGRAPHIC_STATS | Analyse géographique | AREA_NAME, TOTAL_MATCHES, HOME_WIN%, AVG_GOALS |

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
TABLES GOLD (tables normales)
   - LEAGUE_STANDINGS : classements enrichis
   - TOP_SCORERS : meilleurs buteurs avec stats
   - TEAM_STATS : statistiques par equipe
   - MATCH_PATTERNS : patterns temporels
   - REFEREE_STATS : stats arbitres
   - GEOGRAPHIC_STATS : analyse geographique
        |
        v
STREAMLIT DASHBOARD (lecture temps reel)
   - Affiche les tables GOLD
   - Aucun refresh manuel necessaire
```

### Composants automatises

| Composant | Type | Frequence | Description |
|-----------|------|-----------|-------------|
| TASK_FETCH_ALL_LEAGUES | Task CRON | Toutes les 6h | `0 */6 * * * UTC` |
| TASK_MERGE_TO_SILVER | Task DAG | Apres FETCH | Dependance `AFTER` |
| Tables GOLD | Tables normales | - | `CREATE OR REPLACE TABLE` |
| Streamlit Data | Requetes SQL | Temps reel | Lecture des tables GOLD |

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

Verification des tables GOLD :

```sql
-- Verifier le contenu des tables GOLD
SELECT 'LEAGUE_STANDINGS' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM GOLD.LEAGUE_STANDINGS
UNION ALL
SELECT 'TOP_SCORERS', COUNT(*) FROM GOLD.TOP_SCORERS
UNION ALL
SELECT 'TEAM_STATS', COUNT(*) FROM GOLD.TEAM_STATS
UNION ALL
SELECT 'MATCH_PATTERNS', COUNT(*) FROM GOLD.MATCH_PATTERNS
UNION ALL
SELECT 'REFEREE_STATS', COUNT(*) FROM GOLD.REFEREE_STATS
UNION ALL
SELECT 'GEOGRAPHIC_STATS', COUNT(*) FROM GOLD.GEOGRAPHIC_STATS;
```

### Orchestration DAG

```
TASK_FETCH_ALL_LEAGUES (root)
    |
    +---> TASK_MERGE_TO_SILVER (child)
              |
              +---> [Tables GOLD creees manuellement]
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
| 7 | `01_raw/03_fetch_all_procedure.sql` | SNOWGOAL_ROLE | Procedure FETCH_ALL_LEAGUES (11 ligues) |
| 8 | `CALL FETCH_ALL_LEAGUES()` | SNOWGOAL_ROLE | Chargement initial (~6 min, 30s entre ligues) |
| 9 | `02_staging/01_views.sql` | SNOWGOAL_ROLE | Views FLATTEN (+ 8 colonnes enrichies) |
| 10 | `03_silver/01_tables.sql` | SNOWGOAL_ROLE | Tables Silver (MATCHES enrichie) |
| 11 | `03_silver/02_merge.sql` | SNOWGOAL_ROLE | MERGE RAW vers Silver (+ nouvelles colonnes) |
| 12 | `04_gold/01_tables.sql` | SNOWGOAL_ROLE | Tables GOLD principales |
| 13 | `04_gold/02_match_analytics.sql` | SNOWGOAL_ROLE | **Tables Analytics (NOUVEAU)** |
| 14 | `05_tasks/01_tasks.sql` | SNOWGOAL_ROLE | Tasks DAG |
| 15 | `06_streamlit/01_deploy_app.sql` | SNOWGOAL_ROLE | Dashboard Streamlit (+ Analytics page) |

Apres l'etape 0, utiliser toujours `USE ROLE SNOWGOAL_ROLE;` pour toutes les operations.

---

## Dashboard Streamlit

| Page | Description | Visualisations |
|------|-------------|----------------|
| Home | Vue d'ensemble avec stats live | KPIs, tableaux |
| Standings | Classements par ligue | Tableaux |
| Top Scorers | Podium des meilleurs buteurs | Métriques, tableaux |
| Matches | Resultats et prochains matchs | Tableaux |
| Teams | Details equipes avec stats Home/Away | Métriques, tableaux |
| **Analytics** | **Analyses avancées (NOUVEAU)** | **Plotly interactif** |

### Analytics - Nouvelles visualisations Plotly

**⏰ Time Patterns**
- Weekend vs Midweek (bar charts)
- Goals par heure (line chart)
- Analyse par jour de semaine (bar + line)

**👨‍⚖️ Referee Stats**
- Top 10 arbitres (bar chart)
- Distribution nationalités (treemap)
- Spécialistes prolongations (bar chart)
- Goals vs Experience (scatter plot)

**🌍 Geographic Analysis**
- Matchs par pays (bar chart)
- Home Advantage (bar chart + pie chart)
- Goals vs Home Win% (scatter plot)

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
