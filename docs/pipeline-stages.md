# Operations by stage — reference data vs fact data

This pipeline moves two *kinds* of data through Bronze → Silver → Gold, and they are
treated differently at each stage. Knowing which is which tells you what operation to
apply and where.

| | **Fact data** | **Reference data** |
|---|---|---|
| What it is | the measured event | descriptive / lookup context |
| Here | station footfall (`frequentation`) | holidays, commune population, time slots |
| Volume | high, grows over time | small, slowly changing |
| Grain | one row per (station, date, time slot) | one row per holiday / commune / slot |
| Ends up as | the **fact table** (measures + keys) | the **dimensions** (attributes) |

Reminder on the two organizing axes (see [clean-code-principles.md](clean-code-principles.md)):
**Bronze is source-aligned** (one raw source → one table); **Silver/Gold are
subject-aligned** (the `frequentation` product, built by joining sources).

---

## Bronze — make it *technically* usable

**Goal:** turn a raw file into a typed, clean Delta table. **No business rules, no joins.**
One table per source. Code: `data_sources/<source>/handler.py`.

**Operations:** read raw → cast types → parse dates → normalize casing/whitespace →
deduplicate → add `ingestion_timestamp` → write Delta.

| | Fact source: `train_traffic` | Reference sources: `holidays`, `population` |
|---|---|---|
| Read | CSV from the landing volume | holidays = CSV ; population = typed seed via `Dataset.create` |
| Clean | parse **two** date formats, cast `int`/`double`, `initcap`/`trim`, drop exact dups | holidays: parse date, dedup by `date` ; population: already typed |
| Nulls | **kept** (validity is a Silver concern) | keys must be unique (no fan-out later) |
| Output | `bronze.frequentation` | `bronze.jours_feries`, `bronze.population_communes` |

> At Bronze everything is "just a source" — the fact source and the reference sources
> go through the *same* lifecycle (`read → clean → validate → write`).

---

## Silver — make it *business-correct* and *enriched*

**Goal:** apply validity rules to the **fact**, then **enrich** it by joining the
**reference** data. One clean table. Code: `products/frequentation/silver.py`.

**Operations:** filter/validate → left-join reference data → coalesce defaults.

| | Fact: `frequentation` | Reference: `holidays`, `population` |
|---|---|---|
| Role | the thing being validated & enriched | join inputs that add context |
| Operation | `validate_frequentation`: drop missing key/measure, negative counts, unknown segments | `enrich_frequentation`: left-join holidays → `est_jour_ferie` flag; left-join population → `code_insee`, `population` |
| Why left join | — | keep **every** fact row even with no reference match |
| Output | `silver.frequentation_clean` (validated + enriched, still one row per event) |

> Reference data has **no Silver table of its own** — it is *folded into the fact* as
> flags/attributes. That is the signal that reference data exists to serve the subject.

---

## Gold — model for *analytics* (star schema)

**Goal:** split the enriched fact into a **star schema**: descriptive **dimensions** +
one **fact table** of measures and foreign keys. Code: `products/frequentation/gold.py`.

**Operations:** select + `dropDuplicates` (dimensions), derive keys, `join` fact to a
dimension, select measures + keys.

### Dimensions (from reference / descriptive attributes)

| Dimension | Built by | From |
|---|---|---|
| `gold.dim_gare` | `build_dim_gare` — select station columns, `dropDuplicates(["gare_id"])` | station attributes incl. `population` |
| `gold.dim_date` | `build_dim_date` — derive `id_date`, `jour`, `mois`, `annee`, `trimestre`, `jour_semaine`, `is_weekend` | the `date` + `est_jour_ferie` (holiday) |
| `gold.dim_tranche_horaire` | `build_dim_tranche_horaire` — `Dataset.create` from `TRANCHE_HORAIRE_REF` | the time-slot reference (a dataclass list) |

Surrogate keys: `id_date` = `YYYYMMDD` (int), `id_tranche`, `gare_id`.

### Fact (measures + foreign keys)

| Fact table | Built by | Contains |
|---|---|---|
| `gold.fait_frequentation` | `build_fait_frequentation` — join to `dim_tranche_horaire`, derive `id_date` | measures `nb_voyageurs`, `nb_non_voyageurs` + FKs `gare_id`, `id_date`, `id_tranche` |

> Dimensions = the *reference/descriptive* side (the "who / when / what slot").
> Fact = the *measured* side (the "how many"), keyed to the dimensions.

---

## One-glance summary

| Stage | Fact (`frequentation`) | Reference (`holidays`, `population`, time slots) |
|---|---|---|
| **Bronze** | type + parse + dedup → `bronze.frequentation` | type + dedup → `bronze.jours_feries`, `bronze.population_communes` |
| **Silver** | validate (drop bad rows) → enriched fact | joined in as `est_jour_ferie`, `population`, `code_insee` |
| **Gold** | `fait_frequentation` (measures + FKs) | `dim_gare`, `dim_date`, `dim_tranche_horaire` |
