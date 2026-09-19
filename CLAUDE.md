# Project: {{CLIENT_NAME}} — {{CASE_CODENAME}}

This file is loaded by Claude Code on every session in this repo. Keep it accurate — it's the
source of truth for how this case's Databricks project is built, not just documentation.

> **Fill this in before writing any code.** Until the `{{...}}` placeholders are replaced,
> several rules below cannot be evaluated at all — "gold tables shaped for
> `{{ML_TASK_TYPE}}`" and "a semantic layer for `{{BI_TOOL}}`" are unresolvable, so they
> get silently skipped rather than enforced. A half-filled CLAUDE.md is worse than none:
> it reads like a contract while checking nothing. See step 0 of the definition of done.

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

  **The decorator does not decide which one you get — the read does.** A `spark.read` /
  `spark.read.table` source produces a materialized view *no matter what you decorate it
  with*; only `spark.readStream` / Auto Loader (`cloudFiles`) produces a streaming table.
  Mismatching them does not error — it silently gives you an MV with a misleading
  decorator. After the first run, confirm what you actually built:

  ```bash
  databricks tables list <catalog> <schema> --profile <profile>   # STREAMING_TABLE vs MATERIALIZED_VIEW
  ```

  If bronze is meant to be "idempotent, handles schema evolution" (below), that means Auto
  Loader, not `spark.read.format("csv")`.

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

**Serverless constraints.** If any target runs on serverless compute (required on Free
Edition and serverless-only workspaces), these are not optional:

- **Executors have no Databricks credentials.** Anything that distributes work to Spark
  workers and then calls back into the workspace fails with
  `default auth: cannot configure default credentials`. This rules out
  `MlflowSparkStudy` / distributed Optuna — use a driver-side `optuna.create_study()`.
- **Pin every dependency the base image doesn't ship.** A `spark_python_task` needs an
  `environment_key` and an explicit `dependencies:` list. The base image's MLflow is older
  than the `mlflow.optuna` / `mlflow.pyspark` APIs, so pin `mlflow>=3.1.0` if you use them.
- **A `spark_python_task` has no notebook, so MLflow cannot infer an experiment.** Without
  an explicit `mlflow.set_experiment(...)` every run dies on
  `RESOURCE_DOES_NOT_EXIST: Could not find experiment with ID None`. Pass the path in from
  the bundle so it varies per target.
- **Serverless pipelines reject manual compute settings** — no `clusters:` block.

**MLflow (ml projects only).** Every training run is an MLflow run. Use
`mlflow.set_registry_uri("databricks-uc")` and register models to Unity Catalog. Use aliases
(`@champion`, `@challenger`) for promotion, not legacy numeric stages. No model reaches a
scoring job without being a registered UC model version.

  Aliases are not returned by default when you go looking for them — `databricks
  registered-models get <name>` omits the field entirely unless you pass
  `--include-aliases`. Don't conclude a promotion failed without it.

**CI/CD.** GitHub Actions only:
- On PR: lint, unit tests, `databricks bundle validate --strict -t dev`.
- On merge to {{DEFAULT_BRANCH}}: deploy to `staging`, run integration tests.
- On tag/release: deploy to `prod` (manual approval gate via a GitHub environment with
  required reviewers — the `environment:` key alone gates nothing until reviewers are set).

Auth and merge mechanics, both of which bite silently:

- **Workspace credentials come from repo secrets; the host comes from `databricks.yml`.**
  Prefer a per-environment service principal (`DATABRICKS_CLIENT_ID` / `_SECRET`) over a
  PAT, which ties CI to one person's identity. Name secrets per environment so a staging
  run can never reach prod.
- **A missing GitHub secret resolves to an empty string, it does not fail the step.** The
  job runs on to the CLI and dies with a confusing auth error against whatever host it
  found. If a deploy is expected to be unconfigured for a while, guard it explicitly:
  `if: env.DATABRICKS_TOKEN != ''` plus a `::warning` annotation so the skip stays visible.
  Note the `secrets` context is *not* available to `if:` at any level; `env` is available
  to step-level `if:`.
- **Merge PRs with a merge commit, not a squash.** Squashing `dev` into {{DEFAULT_BRANCH}}
  detaches the histories: the next PR from the same long-lived branch reports every file
  as an add/add conflict, and GitHub will not run `pull_request` workflows on a PR it
  cannot compute a merge commit for — so CI silently doesn't run at all. If you do squash,
  reset the branch onto {{DEFAULT_BRANCH}} immediately afterwards.

## Skills to use (from the Databricks AI Dev Kit)

Before writing Databricks-specific code, check whether an installed skill already covers the
pattern and follow it rather than improvising. In particular:

| Task | Skill |
|---|---|
| **Any Databricks work — load this first, then the one below** | `databricks-core` |
| `databricks.yml` / `resources/*.yml`, bundle targets | `databricks-dabs` |
| Pipeline transformation code (bronze/silver/gold) | `databricks-pipelines` |
| Jobs / job DAGs / scheduling | `databricks-jobs` |
| Model serving endpoints | `databricks-model-serving` |
| Any SDK / CLI / REST call to the workspace | `databricks-python-sdk` |
| Catalog, schema, grants, lineage | `databricks-unity-catalog` |
| Streaming ingestion into bronze | `databricks-spark-structured-streaming` |
| Model training code ({{ML_TASK_TYPE}}) — ml projects only | `databricks-ml-training` |
| Experiment tracking / evaluation — ml projects only | `databricks-mlflow-evaluation` |
| Sample/test data when {{DATA_SOURCES}} isn't accessible yet | `databricks-synthetic-data-gen` |

If a task doesn't match an installed skill, say so explicitly rather than silently freelancing
a pattern one of these skills was meant to cover.

## Definition of done for a new dataset or pipeline

0. **No unfilled placeholders anywhere.** Run
   `grep -rnE '\{\{[A-Z_]+\}\}|<[a-z-]+-(host|principal|email)>' . --exclude-dir=.git`
   and get nothing back. Placeholders that reach a resource file do not error — they are
   accepted as literal values, so `{{ALERT_EMAIL}}` becomes a job's notification address
   and `<staging-workspace-host>` becomes a hostname CI will fail to resolve.
1. **UC target exists.** The catalog and schema this writes to are created and you can see
   them. If the metastore has Default Storage enabled, `databricks catalogs create` fails
   asking for a `MANAGED LOCATION` — use SQL `CREATE CATALOG` instead, which uses default
   storage.
2. Bronze ingestion: idempotent, handles schema evolution.
3. Silver transform: expectations cover every known quality risk for this dataset.
4. Gold table(s): match the {{PROJECT_TYPE}} contract above.
5. Unit test for the transform logic under `tests/unit/`.
6. **Resource declared in `resources/*.yml`, and it deployed and ran — not just validated.**
   `bundle validate --strict` checks config schema, *not* whether the API will accept the
   payload: a bundle can validate clean and still fail `deploy` with a 400. So all three
   must have succeeded at least once:
   `bundle validate --strict -t dev` → `bundle deploy -t dev` → `bundle run <resource> -t dev`.
   Paste the run URL in the PR.
7. If `ml`: training/scoring has actually produced a registered UC model version with an
   alias — confirmed via `databricks registered-models get <name> --include-aliases`.
   "Wired to MLflow" is not the same as "has run once".
8. PR opened, CI green, reviewed before merge.

## Repo layout

```
databricks.yml
resources/
  pipeline.yml
  jobs.yml
  ml_jobs.yml            # ml projects only
  model_serving.yml      # ml projects only, if the model is served rather than batch-scored
src/
  pipelines/{bronze,silver,gold}/
  ml/{{ML_TASK_TYPE}}/    # ml projects only
  utils/
tests/{unit,integration}/
.github/workflows/{ci.yml,cd.yml}
pyproject.toml           # local/CI toolchain only — never installed on Databricks compute
.gitignore               # must cover .databricks/ (per-developer CLI + IDE state)
```

Skills are **not** vendored into the repo. The AI Dev Kit installs them as a plugin, so
there is no `.claude/skills/` here and nothing to hand-edit. A case can still add its own
`.claude/commands/` if it needs project-specific prompts.

A registered model is not a served model. If {{PROJECT_TYPE}} is `ml` and the model needs
an endpoint rather than batch scoring, `resources/model_serving.yml` has to actually exist
— UC registration alone serves nothing.