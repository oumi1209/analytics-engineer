# Clean code principles applied in this project

The principles below are the ones we actually used to refactor the notebooks into
`src/analytics_engineer/`. Each comes with a concrete *before → after* from this
codebase, so you can recognise the pattern and reuse it. They map onto the
[Zen of Python](zen-of-python.md).

---

## 1. One source of truth (DRY)

> *Don't repeat a fact — name it once, reference it everywhere.*

A value that is copy-pasted will drift. We hit exactly that bug: the catalog
prefix was hand-written and one place forgot it.

```python
# before — hand-written, and inconsistent (missing the catalog → wrong table)
spark.read.table("silver.frequentation_clean")          # bug
saveAsTable("sncf_gc.silver.frequentation_clean")

# after — one builder, impossible to get wrong
config.table("silver", "frequentation_clean")            # -> sncf_gc.silver.frequentation_clean
```

Same idea elsewhere: `write_delta_table` (one writer), `_with_id_date` (one date-key
rule), and the dataclass models (one schema definition, reused by handlers, products,
and tests).

**Files:** `infra/config_handler.py`, `infra/delta.py`, `products/frequentation/gold.py`.

---

## 2. Separate concerns with namespaces

> *Namespaces are one honking great idea — let's do more of those.*

Each package has one job, so you always know where code belongs. Crucially, Bronze
and the consumer layers are organized along **different axes**:

| Package | Aligned by | Responsibility |
|---|---|---|
| `infra/` | — | technical plumbing: Spark session, config, Delta writer, `Dataset[T]` |
| `data_sources/<src>/` | **source** | one raw source, **up to Bronze** (`read → clean → validate → write`) |
| `products/<subject>/` | **subject** | a data product (e.g. `frequentation`): Silver enrichment + Gold star schema |

A station handler never joins holidays — that's a cross-source concern. And the join
doesn't belong to *any* single source; it belongs to the **`frequentation` subject**,
so it lives in `products/frequentation/silver.py`. (That's why "organize everything
by source" breaks down: holidays and population have no Silver/Gold of their own —
they only feed the frequentation product.)

---

## 3. Make the implicit explicit — use types

> *Explicit is better than implicit.*

A bare `DataFrame` hides what columns it has. We tag it with a dataclass and derive
the schema from it, so the shape is declared once and can be checked.

```python
@dataclass
class RefHoliday:
    date: date
    nom_jour_ferie: str

ds = Dataset.create(spark, RefHoliday, rows)   # schema derived from the dataclass
ds.validate()                                  # fails loudly if a column/type is wrong
```

**Files:** `infra/dataset.py`, every `data_sources/<src>/model.py`.

---

## 4. Keep the core pure; push I/O to the edges

> *If the implementation is easy to explain, it may be a good idea.*

The transformation functions take data in and return data out — no reading, no
writing, no `current_timestamp()`. That is what makes them unit-testable with no
cluster setup.

```python
def validate_frequentation(freq: Dataset[Frequentation]) -> Dataset[Frequentation]:
    ...   # pure: no spark.read, no .write
```

I/O lives only at the edges: handlers (`read`/`write`) and the `run_*` orchestrators.
Tests call the pure function directly:

```python
result = validate_frequentation(Dataset.create(spark, Frequentation, rows))
```

---

## 5. Small functions with intention-revealing names

> *Readability counts.* Use the **business** language, not generic words.

`validate_frequentation`, `enrich_frequentation`, `build_dim_gare` say what they do
in domain terms. We deliberately renamed the generic `transformations.py` away.
Code (and comments) are in English so the whole team reads them the same way.

---

## 6. Don't over-engineer (KISS / YAGNI)

> *Simple is better than complex.*

`Dataset[T]` is ~40 lines of stdlib + PySpark — not a typed-DataFrame framework and
not a new dependency. It does exactly two things we needed (derive a schema, build a
typed test input) and stops there. Add an abstraction only when it earns its keep.

---

## 7. Fail loud, never silent

> *Errors should never pass silently.*

The old base reader swallowed everything (`except error: raise`, `raise ("string")`)
— it could not actually run. The rewrite uses real, specific failures:

```python
@abstractmethod
def read(self) -> Dataset: ...           # subclass MUST implement it

raise SchemaValidationError(...)         # says which column/type is wrong
```

**Files:** `infra/data_source_handler.py`, `infra/dataset.py`.

---

## 8. Depend on an abstraction, not a concrete session

> *In the face of ambiguity, refuse the temptation to guess* — inject what varies.

Stages receive a `SparkSession`; `SparkSessionHandler` owns *how* it is built, so the
same code runs in production, in a Databricks notebook, and in a local test — only
the factory changes.

```python
SparkSessionHandler.set_factory(lambda: SparkSession.builder.master("local[*]")...)  # tests
SparkSessionHandler.set(spark)                                                        # notebook
```

**Files:** `infra/spark.py`, `tests/conftest.py`.
