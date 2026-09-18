# Project: {{CLIENT_NAME}} — {{CASE_CODENAME}}

This file is loaded by Claude Code on every session in this repo. Keep it accurate — it's the
source of truth for how this case's Databricks project is built, not just documentation.

## Case context (fill in per case)

- Client / case: {{CLIENT_NAME}} / {{CASE_CODENAME}}
- Cloud: {{CLOUD_PROVIDER}}                      <!-- aws | azure | gcp -->
- Git repo: {{GIT_REPO_URL}}, default branch {{DEFAULT_BRANCH}}
- Workspace/environment isolation strategy: {{WORKSPACE_STRATEGY}}
  <!-- separate-workspaces | single-workspace-catalog-per-env -->
  <!--
    Decide this before filling in the hosts below. Rule of thumb (Databricks guidance):
    - <=5 data engineers, short case, no regulatory constraint -> single-workspace-catalog-per-env
    - 5+ engineers, or PROJECT_TYPE=ml with a live serving endpoint -> separate-workspaces
      (per-workspace API rate limits mean staging load can otherwise degrade prod serving)
    - Regulated industry / client data-residency requirement -> separate-workspaces,
      possibly separate cloud accounts entirely
    - If the client already has a landing zone / platform team-defined workspace strategy,
      use theirs rather than picking one here.
  -->
- Databricks workspaces:
  - dev:     {{DEV_WORKSPACE_HOST}}
  - staging: {{STAGING_WORKSPACE_HOST}}      <!-- same as dev's host if single-workspace -->
  - prod:    {{PROD_WORKSPACE_HOST}}         <!-- same host is allowed but not recommended for ml -->
- Unity Catalog:
  - If separate-workspaces: catalog = {{UC_CATALOG}} in each workspace's own metastore binding,
    schemas = bronze / silver / gold {{ADDITIONAL_SCHEMAS}}
  - If single-workspace-catalog-per-env: catalogs = {{UC_CATALOG}}_dev / _staging / _prod,
    each with schemas bronze / silver / gold {{ADDITIONAL_SCHEMAS}}
  - Either way: bind the prod catalog in ISOLATED mode to the prod workspace only, so prod
    data stays unreachable from dev/staging even if a grant is misconfigured.
  - If single-workspace-catalog-per-env: use separate service principals for staging vs.
    prod deploys so a bad CI run against staging can't touch prod resources.
- Project type: {{PROJECT_TYPE}}                 <!-- bi | ml -->
  - If `bi`: consumption tool = {{BI_TOOL}}       <!-- e.g. Power BI, Tableau -->
  - If `ml`: task type = {{ML_TASK_TYPE}}         <!-- forecasting | classification | clustering -->
- Primary data sources: {{DATA_SOURCES}}
- Refresh cadence / SLA: {{REFRESH_CADENCE}}

## Non-negotiable conventions

**Infra as code.** Every Databricks resource (pipelines, jobs, clusters, permissions, model
serving endpoints) is declared in `resources/*.yml` and deployed via
`databricks bundle deploy -t <target>`. Never create or edit resources by hand in the workspace
UI — if it isn't in the bundle, it doesn't exist.

**Pipelines.** Use Lakeflow Declarative Pipelines for all transformation logic. Import with
`from pyspark import pipelines as dp` (not the legacy `import dlt`) in new code. Use
`@dp.table` for streaming tables and `@dp.materialized_view` for materialized views —
don't default everything to `@dp.table`.

**Medallion architecture.**
- Bronze: raw ingestion, schema-on-read, idempotent, minimal transformation.
- Silver: cleaned/conformed, deduplicated, expectations enforced.
- Gold: shaped for the consumer —
  - `bi` projects: a stable, documented semantic layer for {{BI_TOOL}} (clear grain, no
    breaking schema changes without a version bump).
  - `ml` projects: feature tables shaped for {{ML_TASK_TYPE}} (point-in-time correct,
    no leakage from the future, documented feature lineage).

**Expectations are mandatory on silver+.** Every quality rule you know about must be a
declared expectation, not a silent filter buried in a `.filter()` call:
- `@dp.expect("name", "condition")` — log violations, keep the row.
- `@dp.expect_or_drop("name", "condition")` — quarantine bad rows.
- `@dp.expect_or_fail("name", "condition")` — stop the pipeline; use only for conditions
  that mean downstream data is untrustworthy.

**Orchestration.** All scheduling and dependencies live in `resources/jobs.yml` as a single
job DAG: ingestion → pipeline → (ml: training/scoring | bi: refresh trigger) → notification.
Don't create ad hoc scheduled notebooks outside the bundle.

**MLflow (ml projects only).** Every training run is an MLflow run. Use
`mlflow.set_registry_uri("databricks-uc")` and register models to Unity Catalog. Use aliases
(`@champion`, `@challenger`) for promotion, not legacy numeric stages. No model reaches a
scoring job without being a registered UC model version.

**CI/CD.** GitHub Actions only:
- On PR: lint, unit tests, `databricks bundle validate -t dev`.
- On merge to {{DEFAULT_BRANCH}}: deploy to `staging`, run integration tests.
- On tag/release: deploy to `prod` (manual approval gate).

## Skills to use (from the Databricks AI Dev Kit, `.claude/skills/`)

Before writing Databricks-specific code, check whether an installed skill already covers the
pattern and follow it rather than improvising. In particular:

| Task | Skill |
|---|---|
| `databricks.yml` / `resources/*.yml`, bundle targets | `databricks-dabs` |
| Any SDK / CLI / REST call to the workspace | `databricks-python-sdk` |
| Catalog, schema, grants, lineage | `databricks-unity-catalog` |
| Streaming ingestion into bronze | `databricks-spark-structured-streaming` |
| Model training code ({{ML_TASK_TYPE}}) — ml projects only | `databricks-ml-training` |
| Experiment tracking / evaluation — ml projects only | `databricks-mlflow-evaluation` |
| Sample/test data when {{DATA_SOURCES}} isn't accessible yet | `databricks-synthetic-data-gen` |

If a task doesn't match an installed skill, say so explicitly rather than silently freelancing
a pattern one of these skills was meant to cover.

## Definition of done for a new dataset or pipeline

1. Bronze ingestion: idempotent, handles schema evolution.
2. Silver transform: expectations cover every known quality risk for this dataset.
3. Gold table(s): match the {{PROJECT_TYPE}} contract above.
4. Unit test for the transform logic under `tests/unit/`.
5. Resource declared in `resources/*.yml`, passes `databricks bundle validate -t dev`.
6. If `ml`: training/scoring wired to MLflow + UC model registry.
7. PR opened, CI green, reviewed before merge.

## Repo layout

```
databricks.yml
resources/
  pipeline.yml
  jobs.yml
  ml_jobs.yml            # ml projects only
src/
  pipelines/{bronze,silver,gold}/
  ml/{{ML_TASK_TYPE}}/    # ml projects only
  utils/
tests/{unit,integration}/
.github/workflows/{ci.yml,cd.yml}
.claude/
  skills/                # installed by AI Dev Kit — do not hand-edit
  commands/
    new-case.md          # scaffolding prompt, see below
```