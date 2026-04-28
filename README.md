# ETL Pipeline — Drug Database (OpenFDA)

![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?logo=python&logoColor=white)
![pandas](https://img.shields.io/badge/pandas-2.2-150458?logo=pandas&logoColor=white)
![SQLite](https://img.shields.io/badge/SQLite-3-003B57?logo=sqlite&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?logo=powerbi&logoColor=black)
![Tests](https://img.shields.io/badge/tests-12%20passed-brightgreen)
![License](https://img.shields.io/badge/License-MIT-green)

Pipeline ETL complet qui extrait, transforme et charge **134 000+ references medicaments** depuis l'API publique OpenFDA dans un entrepot SQLite — avec logging, controles qualite, idempotence et dashboard Power BI interactif.

---

## Contexte du projet

Le secteur pharmaceutique est soumis a des exigences strictes de **tracabilite et de qualite des donnees** (FDA 21 CFR Part 11, GxP). Ce projet simule un pipeline de donnees realiste pour alimenter un datawarehouse analytique a partir de donnees reglementaires officielles.

**Source :** [OpenFDA Drug NDC API](https://open.fda.gov/apis/drug/ndc/) — API REST publique, sans cle requise, 134 000+ references.

---

## Architecture ETL

```
+----------------------------------------------------------+
|                        EXTRACT                           |
|  OpenFDA REST API  ->  Pagination 100/batch  ->  CSV raw |
|  (api.fda.gov)         + retry reseau                    |
+----------------------------+-----------------------------+
                             |  data/raw/medicaments_raw.csv
                             v
+----------------------------------------------------------+
|                       TRANSFORM                          |
|  Nettoyage textes  ->  Deduplication NDC (PK unique)     |
|  Parsing dates     ->  Feature engineering (annee, cat.) |
|  Validation qualite -> Rapport completude par colonne    |
+----------------------------+-----------------------------+
                             |  data/transformed/medicaments_clean.csv
                             v
+----------------------------------------------------------+
|                         LOAD                             |
|  Schema SQLite     ->  UPSERT (product_ndc = PK)         |
|  5 index strategiques  ->  Validation post-chargement    |
+----------------------------+-----------------------------+
                             |  data/pharma_warehouse.db
                             v
                     Requetes SQL analytiques
                     Dashboard Power BI interactif
```

---

## Structure du projet

```
etl-pharma-pipeline/
├── README.md
├── requirements.txt
├── .gitignore
├── data/
│   ├── raw/                         # Donnees brutes (non versionnees)
│   ├── transformed/                 # Donnees nettoyees (non versionnees)
│   └── dashboard power BI/
│       ├── dashboard.pbix           # Dashboard Power BI interactif
│       └── dashboard.pdf            # Export PDF du dashboard
├── src/
│   ├── extract.py                   # Module extraction API
│   ├── transform.py                 # Module nettoyage & feature engineering
│   ├── load.py                      # Module chargement SQLite
│   ├── etl_pipeline.py              # Orchestrateur principal
│   └── queries.sql                  # Requetes analytiques metier
└── tests/
    └── test_transform.py            # 12 tests unitaires (pytest)
```

---

## Installation

```bash
# Cloner le depot
git clone https://github.com/<username>/etl-pharma-pipeline.git
cd etl-pharma-pipeline

# Environnement virtuel
python -m venv .venv
source .venv/bin/activate        # Windows : .venv\Scripts\activate

# Dependances
pip install -r requirements.txt
```

---

## Utilisation

### Pipeline complet (2000 medicaments, ~2 min)

```bash
cd src
python etl_pipeline.py
```

### Test rapide (300 medicaments, ~10 sec)

```bash
cd src
python etl_pipeline.py --max-records 300
```

**Sortie attendue :**

```
+======================================================+
|       ETL Pipeline -- Base Publique des Medicaments  |
+======================================================+

2024-xx-xx [INFO] --- ETAPE 1/3 : EXTRACT
2024-xx-xx [INFO] API disponible : 134,668 medicaments -- on en recupere 300
...
+======================================================+
|            RAPPORT D'EXECUTION ETL                   |
+======================================================+
|  [OK] Extraction  :    300 enregistrements recuperes  |
|  [->] Doublons   :      0 supprimes                   |
|  [DB] Chargement :    300 enregistrements en base     |
|  [%%] Completude :  92.5% de donnees completes        |
|  [Rx] Prescription :   93 / OTC :  115                |
+------------------------------------------------------+
|  Total     :   6.4s                                   |
+======================================================+
```

### Tests unitaires

```bash
python -m pytest tests/ -v
# -> 12 passed
```

### Requetes SQL analytiques

```bash
sqlite3 data/pharma_warehouse.db < src/queries.sql
```

---

## Detail des etapes

### 1. Extract (`src/extract.py`)

- Pagination automatique par lots de 100 (API limite a 1000/req)
- Aplatissement des champs imbriques (ingredients actifs, voies admin)
- Retry automatique en cas d'erreur reseau (5s de delai)
- Delai de politesse entre appels (150 ms) — respect des quotas API
- Sauvegarde CSV brute systematique (audit trail)

### 2. Transform (`src/transform.py`)

| Transformation | Detail |
|---|---|
| Nettoyage textes | Strip, None -> NaN, noms en MAJUSCULES (convention FDA) |
| Parsing dates | `YYYYMMDD` -> `datetime`, extraction `annee_mise_marche` |
| Deduplication | Sur `product_ndc` (identifiant unique FDA) |
| Feature engineering | `product_type_cat` (Prescription / OTC / Vaccin / ...) |
| Validation qualite | Rapport de completude par colonne (seuil alerte : >5% manquants) |

### 3. Load (`src/load.py`)

- Schema SQLite avec contrainte `PRIMARY KEY` sur `product_ndc`
- Pipeline **idempotent** : re-executable sans duplication (UPSERT)
- 5 index sur les colonnes les plus requetees
- Validation post-chargement : total, Rx vs OTC, top laboratoires, periode couverte

---

## Dashboard Power BI

Le fichier `data/dashboard power BI/dashboard.pbix` contient 4 visuels :

| Visuel | Description |
|---|---|
| 3 cartes KPI | Total medicaments, laboratoires distincts, annees couvertes |
| Bar chart | Top 10 laboratoires par nombre de references |
| Donut | Repartition par type de produit (Prescription / OTC / Vaccin...) |
| Courbe | Evolution des mises sur le marche (1940-2026) |

---

## Schema de la base

```sql
CREATE TABLE medicaments (
    product_ndc           TEXT PRIMARY KEY,   -- Identifiant unique FDA (NDC)
    generic_name          TEXT,               -- DCI en majuscules
    brand_name            TEXT,               -- Nom commercial
    labeler_name          TEXT,               -- Laboratoire fabricant
    dosage_form           TEXT,               -- Tablet, Capsule, Solution...
    product_type          TEXT,               -- Type brut FDA
    product_type_cat      TEXT,               -- Prescription / OTC / Vaccin...
    marketing_category    TEXT,               -- NDA, ANDA, OTC monograph...
    marketing_start_date  TEXT,               -- Date mise sur marche (ISO 8601)
    annee_mise_marche     INTEGER,            -- Annee extraite (feature engineering)
    finished              INTEGER DEFAULT 1,  -- Produit fini (vs API)
    route                 TEXT,               -- Voie(s) d'administration
    pharm_class           TEXT,               -- Classe pharmacologique
    active_ingredient     TEXT,               -- 1er principe actif (MAJUSCULES)
    strength              TEXT,               -- Dosage du 1er ingredient
    date_chargement       TEXT               -- Horodatage ETL
);
```

---

## Exemples de requetes analytiques

```sql
-- Vue d'ensemble
SELECT COUNT(*) total, COUNT(DISTINCT labeler_name) nb_labos,
       MIN(annee_mise_marche) debut, MAX(annee_mise_marche) fin
FROM medicaments;

-- Top 5 laboratoires
SELECT labeler_name, COUNT(*) nb
FROM medicaments WHERE labeler_name IS NOT NULL
GROUP BY labeler_name ORDER BY nb DESC LIMIT 5;

-- Repartition Rx vs OTC par decennie
SELECT (annee_mise_marche / 10) * 10 AS decennie,
       SUM(CASE WHEN product_type_cat='Prescription' THEN 1 ELSE 0 END) rx,
       SUM(CASE WHEN product_type_cat='OTC' THEN 1 ELSE 0 END) otc
FROM medicaments WHERE annee_mise_marche IS NOT NULL
GROUP BY decennie ORDER BY decennie;
```

---

## Ameliorations futures

| Amelioration | Technologie | Impact |
|---|---|---|
| Orchestration planifiee | Apache Airflow / Prefect | Mises a jour automatiques |
| Tests de qualite avances | Great Expectations | Conformite GxP / FDA |
| Stockage cloud | AWS S3 + Athena | Scalabilite multi-terabytes |
| Monitoring pipeline | Prometheus + Grafana | Alerting production |
| API de consultation | FastAPI | Exposition des donnees |
| CI/CD automatise | GitHub Actions | Deploiement continu |

---

## Licence

MIT — donnees sources © OpenFDA / U.S. Food and Drug Administration (domaine public)
