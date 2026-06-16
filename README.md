# Medallion Lakehouse Pipeline - PySpark

End-to-end data pipeline following the **Medallion architecture** (Bronze -> Silver
-> Gold) on **Databricks** with **PySpark**, modeling the footfall of French railway
stations for decision analytics.

## Goal

Turn raw footfall data into a **star schema** ready to feed Power BI dashboards,
through a traceable, industrializable chain of cleaning, enrichment, and modeling.

## Architecture

The project follows the Medallion architecture, which organizes data into three
layers of increasing quality:

### 0. Data generation (simulated source system)

- Generates a CSV file simulating the footfall source system
- Real French stations with exact GPS coordinates
- **Deliberately injected anomalies** (nulls, negative values, malformed dates,
  inconsistent casing, invalid categories, duplicates) to give the Silver layer a
  concrete role
- This step is **not** part of the Medallion pipeline: it reproduces the source that,
  in production, would feed the Bronze ingestion
- Output: a raw CSV file

### 1. Bronze Layer

- Ingests the raw source data with minimal transformation
- Preserves the original data for traceability (lineage)
- Tables: `bronze.frequentation`, `bronze.jours_feries`, `bronze.population_communes`

### 2. Silver Layer

- Cleans, validates, and enriches the data coming from the Bronze layer
- Applies quality rules (deduplication, null handling, negative values)
- Standardizes formats (typing, casing, dates)
- Tables: `silver.frequentation_clean`

### 3. Gold Layer

- Builds the star schema, ready for analytics and reporting
- Implements the business transformations and enrichments
- Tables: `gold.dim_gare`, `gold.dim_date`, `gold.dim_tranche_horaire`, `gold.fait_frequentation`

## Tech stack

- **Databricks** (Unity Catalog) - catalog `sncf_gc`
- **PySpark** — distributed transformations
- **Delta Lake** — transactional storage

## Package structure (`src/analytics_engineer/`)

- `infra/` — technical foundation: `Dataset[T]` (a DataFrame typed by a dataclass),
  `SparkSessionHandler`, config loading, Delta writing, logging.
- `data_sources/<source>/` — **Bronze, source-aligned**: one `model.py`
  (dataclass = typed schema) and one `handler.py` per source. Each handler ingests
  **up to Bronze** (`read → clean → validate → write`).
- `products/<subject>/` — **Silver + Gold, subject-aligned** (data product). Here
  `frequentation/`: `model.py` (typed models), `silver.py` (validation + enrichment =
  the cross-source joins), `gold.py` (star schema).
- `main.py` — CLI entry point.

> **Bronze = source-aligned** (one raw source → one table) ·
> **Silver/Gold = subject-aligned**: `frequentation` is a *product* built by
> **joining** train_traffic × holidays × population — so the join lives in
> `products/frequentation/`, not in a single source.

`config.yaml` centralizes the Unity catalog and the landing-volume paths, which keeps
fully-qualified table names consistent everywhere
(`config.table("silver", "frequentation_clean")` → `sncf_gc.silver.frequentation_clean`).

### The `Dataset[T]` pattern

PySpark has no typed `Dataset[T]` like Scala. `infra/dataset.py` provides a thin
generic wrapper over a `DataFrame`, parameterized by a dataclass:

- `Dataset.schema_of(Model)` derives the `StructType` from the annotations;
- `Dataset(df, Model).validate()` checks the schema contract;
- `Dataset.create(spark, Model, rows)` builds a typed input from dataclasses — this is
  what makes the stages **unit-testable** without a cluster.

## Running the pipeline

Single entry point: run a specific stage, or the whole pipeline in
Bronze → Silver → Gold order. Once the package is installed (`uv sync`), use the
`analytics-engineer` console script (declared in `pyproject.toml`):

```bash
analytics-engineer --stage bronze
analytics-engineer --stage silver
analytics-engineer --stage gold
analytics-engineer --stage all      # default

# equivalent, without the console script:
python -m analytics_engineer.main --stage all
```

### 1. Locally (via Databricks Connect)

The pipeline writes to Unity Catalog (`sncf_gc`), so the compute runs on a **remote**
Databricks serverless/cluster, driven from your machine. This is the recommended
development mode.

```bash
uv sync                                  # installs dependencies (pyspark, pytest, ...)
uv pip install databricks-connect        # remote Spark client

# authenticate (once) against the workspace defined in databricks.yml
databricks auth login --host https://dbc-e30aefd6-ab32.cloud.databricks.com

uv run analytics-engineer --stage all
```

The session is built by `SparkSessionHandler`: locally it points to Databricks
Connect, without changing the stage code.

### 2. On Databricks

**a) Notebooks (interactive / teaching)** — open and run in order:
`00_data_generation` → `01_bronze_ingestion` → `02_silver_nettoyage` →
`03_gold_modele`. The notebooks stay self-contained (inline logic) for step-by-step
reading; `src/` is the typed, tested version of the same logic.

**b) Job via Asset Bundle (productionization)** — the job is defined in
`resources/analytics_engineer.job.yml` (3 chained tasks: bronze → silver → gold).
`databricks.yml` includes it, builds the wheel, and holds the tunable compute as
`variables` (overridable per target). Then:

```bash
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run   -t dev medallion_pipeline
```

**Where job settings live:** compute sizing (`node_type_id`, `num_workers` /
`autoscale`, `spark_version`) goes in the job's `new_cluster` block; runtime args
(`--stage bronze|silver|gold`) go in each task's `parameters`. Neither lives in
`config.yaml` (business config) nor in the Python code.

### 3. Tests

```bash
uv run pytest                  # the whole suite
uv run pytest tests/infra      # Dataset[T] tests: no Spark session, so no JDK needed
```

- The unit tests build their inputs via
  `Dataset.create(spark, Frequentation, [...])` and run on a **local SparkSession**
  (fixture `spark_fixture`) — so a **JDK** must be installed
  (`brew install openjdk@17`, then `export JAVA_HOME="$(/usr/libexec/java_home -v 17)"`).
- The `tests/infra` tests create no Spark session and pass without a JDK.
- The `spark_connect` fixture remains available for integration tests against a real
  Databricks workspace.

## Documentation

- [docs/databricks-vscode.md](docs/databricks-vscode.md) — working with Databricks
  from VS Code: auth, bundle, Databricks Connect, run/debug, tests.
- [docs/pipeline-stages.md](docs/pipeline-stages.md) — operations by stage
  (Bronze / Silver / Gold) for **reference** vs **fact** data.
- [docs/clean-code-principles.md](docs/clean-code-principles.md) — clean-code
  principles applied in this project, with before/after examples.
- [docs/spark.md](docs/spark.md) — PySpark imports and patterns.
- [docs/zen-of-python.md](docs/zen-of-python.md) · [docs/refactoring.md](docs/refactoring.md)
