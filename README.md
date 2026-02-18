# SnowGoal ⚽❄️

**Plateforme d'Analytics Football & Betting 100% Cloud Native** — Architecture Medallion sur Snowflake.

---

## 🛡️ Points Forts : Robustesse & Observabilité
Contrairement aux pipelines classiques, SnowGoal est conçu pour la production :
- **Observabilité Centralisée** : Monitoring en temps réel via `COMMON.PIPELINE_LOGS`. Chaque étape (Success, Partial Success, Error) est tracée avec métriques et stack traces.
- **Résilience API** : Gestion intelligente des erreurs HTTP (429 Rate Limit, 404 Not Found). Le pipeline utilise une stratégie de *Graceful Degradation* (continue l'exécution même si une ligue échoue).
- **Auto-Monitoring** : Un dashboard d'observabilité est intégré directement dans la documentation Streamlit pour suivre la santé des flux.

---

## 🏗️ Architecture des Données

```text
football-data.org API + The Odds API
        |
        ↓ (Snowpark Python Procedures with Logging 🛡️)
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

---

## 🎲 Intelligence de Paris (Betting)
Intégration de **The Odds API** pour 11 compétitions :
- **Comparaison de Cotes** : Analyse des meilleurs bookmakers.
- **Value Bet Detection** : Algorithme comparant les probabilités implicites des bookmakers avec les performances réelles historiques (90-day PPG).
- **Indicateurs clés** : Marges des bookmakers (overround), probabilités calculées et meilleures cotes du marché.

---

## 🔄 Automatisation & Orchestration (DAG)
Le pipeline est piloté par un graphe de tâches (Tasks) synchronisé sur les horaires des matchs européens :

1. **Root Task (07h, 17h, 00h)** : `TASK_FETCH_ALL_LEAGUES` (Ingestion football-data.org).
2. **Child Task** : `TASK_FETCH_ODDS` (Ingestion des cotes avec Rate Limiting de 3s).
3. **Child Task** : `TASK_MERGE_TO_SILVER` (Transformation incrémentale via **Streams CDC**).
4. **Final Tasks (Parallèles)** : 9 tâches de rafraîchissement des tables GOLD (Analytics & Business).

---

## ⚙️ Stack Technique Complète

| Composant | Technologie | Avantage |
|-----------|-------------|----------|
| **Ingestion** | Snowpark Python + External Access | Pas de serveur externe, 100% sécurisé (Secrets). |
| **CDC** | Snowflake Streams | Réduction des coûts de calcul (ne traite que les deltas). |
| **Batch Loading** | Pandas + `write_pandas` | Performance maximale pour l'écriture de gros volumes JSON. |
| **Orchestration** | Snowflake Tasks (DAG) | Zéro outil tiers (Airflow/dbt non requis). |
| **Frontend** | Streamlit-in-Snowflake | Visualisation temps réel avec accès direct au cache Snowflake. |

---

## 📂 Organisation du Projet

```text
snowgoal/
├── deploy/
│   ├── 00_init/         # Database, Role, Logging Table
│   ├── 01_raw/          # Tables, Streams, Procedures (Ingestion)
│   ├── 02_staging/      # Flattening Views
│   ├── 03_silver/       # Clean Tables, Merge Logic
│   ├── 04_gold/         # Analytics & Betting Tables
│   └── 05_tasks/        # DAG Orchestration
├── snowpark/            # Python source for Stored Procedures
└── streamlit/           # Dashboard (Multi-page app)
```

---

## 🚀 Installation & Déploiement

1. **Initialisation** : Exécuter les scripts de `00_init` (nécessite ACCOUNTADMIN pour le rôle).
2. **Secrets** : Configurer `api_key` et `odds_api_key` dans le schéma `COMMON`.
3. **Python** : Uploader les fichiers du dossier `snowpark/` vers le stage `@COMMON.STAGE_SCRIPTS`.
4. **Tasks** : Activer le DAG (`RESUME`) pour démarrer l'automatisation.

---
**Développé avec 100% de fonctionnalités natives Snowflake.** ⚽❄️
