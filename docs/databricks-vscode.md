# Working with Databricks from VS Code

The **Databricks extension for VS Code** connects the local project to a remote
workspace. With it you can: deploy and run the Asset Bundle (the `medallion_pipeline`
job), run/debug local Python on a cluster or serverless via **Databricks Connect**,
sync code to the workspace, and run `pytest`.

> The pipeline writes to Unity Catalog (`sncf_gc`), so compute always runs on
> Databricks — VS Code is the cockpit, not the engine.

---

## 1. Prerequisites

- VS Code with the **Python** and **Databricks** extensions (install "Databricks"
  from the Marketplace).
- [`uv`](https://docs.astral.sh/uv/) and the project dependencies installed:
  ```bash
  uv sync
  ```
- Access to the workspace declared in [`databricks.yml`](../databricks.yml):
  `https://dbc-e30aefd6-ab32.cloud.databricks.com`.

---

## 2. Authenticate

Easiest is OAuth (user-to-machine), which writes a profile to `~/.databrickscfg`:

```bash
databricks auth login --host https://dbc-e30aefd6-ab32.cloud.databricks.com
```

Or do it from the extension: open the **Databricks** panel → **Configure Databricks**
→ pick the host and sign in. The extension reuses the same `~/.databrickscfg` profile.

---

## 3. Open the project (it is already a bundle)

The extension auto-detects `databricks.yml`. In the **Databricks** panel:

1. Sign in with the auth profile from step 2.
2. Select the bundle **target** → `dev` (the default in `databricks.yml`).
3. You'll see the workspace, the bundle, and its resources — including the
   `medallion_pipeline` job from
   [`resources/analytics_engineer.job.yml`](../resources/analytics_engineer.job.yml).

---

## 4. Select the Python interpreter

`uv sync` creates `.venv`. In VS Code: **Python: Select Interpreter** →
`./.venv/bin/python`. Tests already resolve `src/` (`pythonpath = ["src"]` in
`pyproject.toml`), so imports like `analytics_engineer.infra.dataset` work.

---

## 5. Local dev & debug with Databricks Connect

This runs your **local code** but executes Spark on **remote** compute — with
breakpoints in VS Code.

1. Install the client (match the major version to the cluster's runtime):
   ```bash
   uv pip install databricks-connect
   ```
2. In the extension, enable **Databricks Connect** and pick a cluster or serverless.
3. Open a Python file, set breakpoints, click the **Run on Databricks** icon →
   **Debug current file with Databricks Connect**. Output appears in the Debug Console.

In this project, `SparkSessionHandler.get()` returns the session the environment
provides — with Databricks Connect enabled, that's the remote one, no code change.
You can also just use the integrated terminal:

```bash
uv run analytics-engineer --stage all     # runs against the connected compute
```

---

## 6. Run a file or notebook on a cluster

For the teaching notebooks (`00_data_generation` → `03_gold_modele`) or any script:
**Run on Databricks** → **Upload and Run File**. The extension uploads the file and
runs it on the selected cluster; output shows in the Debug Console.

---

## 7. Run the tests

```bash
uv run pytest                  # whole suite
uv run pytest tests/infra      # Dataset[T] tests — no Spark session, so no JDK needed
```

- The transformation tests use a **local** SparkSession (fixture `spark_fixture`) and
  therefore need a **JDK** (`brew install openjdk@17`, then
  `export JAVA_HOME="$(/usr/libexec/java_home -v 17)"`).
- Prefer running against the cluster? Use the `spark_connect` fixture (Databricks
  Connect) instead.
- Point VS Code's **Testing** view at the `.venv` interpreter to run/debug tests from
  the UI.

---

## 8. Deploy & run the job (Asset Bundle)

From the extension's **Bundle / Workflows** view you can deploy the bundle and run the
`medallion_pipeline` job; run status and logs are shown inline. Equivalent CLI:

```bash
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run   -t dev medallion_pipeline
```

Compute sizing (`node_type_id`, `num_workers`/`autoscale`) and the `--stage`
parameters live in the bundle — see [pipeline-stages.md](pipeline-stages.md) and
`databricks.yml`.

---

## 9. Sync code to the workspace

The extension keeps a workspace copy of your local files (under the bundle's
`workspace.root_path`) so notebooks/jobs on the cluster see your latest code. It syncs
on deploy and can watch for changes.

---

## Tips & gotchas

- **Version match:** `databricks-connect` major version must match the cluster's
  Databricks Runtime (and the local `pyspark` major). Mismatches fail at connect time.
- **Unity Catalog access mode:** a single-user/assigned cluster
  (`data_security_mode: SINGLE_USER`) is needed to write `sncf_gc`.
- **Profiles:** if you have several, set `DATABRICKS_CONFIG_PROFILE` or pick the
  profile in the extension.
- **Don't commit local state:** the extension writes a `.databricks/` folder — keep it
  (and any secrets) out of git.
