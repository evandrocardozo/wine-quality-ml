"""
Matches Databricks' get-started ML tutorial end to end (Parts 1-3:
GradientBoostingClassifier baseline, Optuna tuning, best-run selection,
predictions written back to Unity Catalog, and UC model registration):
https://docs.databricks.com/aws/en/getting-started/ml-get-started.html
Reads from the gold feature table this bundle builds and runs as a Job task on
serverless compute instead of interactively in a notebook.

Part 4 of the tutorial (deploying a serving endpoint) is UI-driven in the notebook,
so there's no script to port -- see resources/model_serving.yml for the
infra-as-code equivalent instead.

IMPORTANT re: "does MLflow pick the best model via alias?" -- no. MLflow just registers
whatever run you point it at. The "find the best run" step (search_runs, sorted by
metric) and the "make this the champion" step (set_registered_model_alias) are both
things THIS SCRIPT decides and does explicitly -- MLflow has no opinion of its own.
"""
import argparse

import mlflow
import mlflow.sklearn
import optuna
from mlflow.optuna.storage import MlflowStorage
from mlflow.pyspark.optuna.study import MlflowSparkStudy
from mlflow.tracking import MlflowClient
from pyspark.sql import SparkSession
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split

MODEL_ALIAS = "champion"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--n-trials", type=int, default=32)
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    mlflow.set_registry_uri("databricks-uc")
    client = MlflowClient()

    table = f"{args.catalog}.{args.schema}.gold_wine_features"
    df = spark.read.table(table).toPandas()

    y = df["high_quality"]
    X = df.drop(columns=["quality", "high_quality"])
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=1
    )

    # --- Part 1 of the tutorial: a single baseline run, autologged -------------
    mlflow.sklearn.autolog()
    with mlflow.start_run(run_name="gradient_boost"):
        baseline = GradientBoostingClassifier(random_state=0)
        baseline.fit(X_train, y_train)
        baseline_auc = roc_auc_score(
            y_test, baseline.predict_proba(X_test)[:, 1]
        )
        mlflow.log_metric("test_auc", baseline_auc)

    # --- Part 2 of the tutorial: Optuna hyperparameter search -------------------
    def objective(trial: optuna.Trial) -> float:
        mlflow.sklearn.autolog()  # re-enable on each worker
        with mlflow.start_run(nested=True):
            params = {
                "n_estimators": trial.suggest_int("n_estimators", 20, 1000),
                "learning_rate": trial.suggest_float(
                    "learning_rate", 0.05, 1.0, log=True
                ),
                "max_depth": trial.suggest_int("max_depth", 2, 5),
            }
            model = GradientBoostingClassifier(random_state=0, **params)
            model.fit(X_train, y_train)
            auc = roc_auc_score(y_test, model.predict_proba(X_test)[:, 1])
            mlflow.log_metric("test_auc", auc)
            # Optuna minimizes by default -- negate since we want to maximize AUC.
            return -auc

    with mlflow.start_run(run_name="gb_optuna"):
        experiment_id = mlflow.active_run().info.experiment_id
        storage = MlflowStorage(experiment_id=experiment_id)
        study = MlflowSparkStudy(study_name="gb-optuna-tuning", storage=storage)
        study.optimize(objective, n_trials=args.n_trials, n_jobs=4)

    # --- Search runs to retrieve the best model ---------------------------------
    best_run = mlflow.search_runs(
        order_by=["metrics.test_auc DESC", "start_time DESC"],
        max_results=10,
    ).iloc[0]
    best_auc = float(best_run["metrics.test_auc"])

    best_model_pyfunc = mlflow.pyfunc.load_model(f"runs:/{best_run.run_id}/model")

    best_model_predictions = X_test.copy()
    best_model_predictions["prediction"] = best_model_pyfunc.predict(X_test)

    # --- Part 3 of the tutorial: save results and register the model in UC -----
    predictions_table = f"{args.catalog}.{args.schema}.predictions"
    spark.sql(f"DROP TABLE IF EXISTS {predictions_table}")
    spark.createDataFrame(best_model_predictions).write.saveAsTable(
        predictions_table
    )

    model_name = f"{args.catalog}.{args.schema}.wine_quality_model"
    model_version = mlflow.register_model(
        f"runs:/{best_run.run_id}/model", model_name
    )

    # --- Not in the tutorial, but part of your CLAUDE.md convention: only move
    # the @champion alias if this run actually beats the current one. -----------
    should_promote = True
    try:
        current = client.get_model_version_by_alias(model_name, MODEL_ALIAS)
        current_auc = float(
            client.get_run(current.run_id).data.metrics.get("test_auc", 0)
        )
        should_promote = best_auc > current_auc
    except mlflow.exceptions.RestException:
        pass  # no champion registered yet -- this run becomes the first one

    alias = MODEL_ALIAS if should_promote else "challenger"
    client.set_registered_model_alias(model_name, alias, model_version.version)


if __name__ == "__main__":
    main()