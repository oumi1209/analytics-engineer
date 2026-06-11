# Medallion Lakehouse Pipeline - PySpark

Pipeline data end-to-end suivant l'**architecture Medallion** (Bronze -> Silver -> Gold)
sur **Databricks** avec **PySpark**, modélisant la fréquentation
des gares ferroviaires françaises pour l'analyse décisionnelle.

## Objectif

Transformer des données brutes de fréquentation en un **modèle dimensionnel en étoile**
prêt à alimenter des dashboards Power BI, en passant par une chaîne de nettoyage,
d'enrichissement et de modélisation traçable et industrialisable.

## Architecture

Le projet suit l'architecture Medallion, qui organise les données en trois couches
de qualité croissante :
### 0. Génération des données (système source simulé)

- Génère un fichier CSV simulant le système source de fréquentation
- 97 gares françaises réelles avec coordonnées GPS exactes
- **Anomalies injectées volontairement** (nulls, valeurs négatives, dates malformées,
  casse incohérente, catégories invalides, doublons) pour donner un rôle concret à la
  couche Silver
- Cette étape ne fait pas partie du pipeline Medallion : elle reproduit la source qui,
  en production, alimenterait l'ingestion Bronze
- Sortie : fichier CSV brut

### 1. Bronze Layer

- Ingère les données brutes du CSV source avec une transformation minimale
- Préserve la donnée d'origine à des fins de traçabilité (lineage)
- Tables : `bronze.frequentation_raw`

### 2. Silver Layer

- Nettoie, valide et enrichit les données issues de la couche Bronze
- Applique les règles de qualité (déduplication, gestion des nulls, valeurs négatives)
- Standardise les formats (typage, casse, dates)
- Tables : `silver.frequentation_clean`


### 3. Gold Layer

- Construit le modèle en étoile prêt pour l'analytique et le reporting
- Implémente les transformations et enrichissements métier
- Tables : `gold.dim_gare`, `gold.dim_date`, `gold.dim_tranche_horaire`, `gold.fait_frequentation`


