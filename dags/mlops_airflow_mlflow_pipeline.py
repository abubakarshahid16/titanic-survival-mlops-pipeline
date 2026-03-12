import os
import logging
from datetime import datetime, timedelta

import pandas as pd
import mlflow
import mlflow.sklearn

from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator


# ============================================================
# GLOBAL CONFIGURATION
# ============================================================
# This section defines all important paths and MLflow settings
# used throughout the pipeline.

BASE_DIR = os.environ.get("AIRFLOW_HOME", os.getcwd())
DATA_DIR = os.path.join(BASE_DIR, "data")
ARTIFACT_DIR = os.path.join(BASE_DIR, "artifacts")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(ARTIFACT_DIR, exist_ok=True)

# Dataset path
DATASET_PATH = os.path.join(DATA_DIR, "Titanic-Dataset.csv")

# MLflow configuration
# SQLite backend is used so MLflow experiment tracking and model registry
# can work locally in a simple assignment environment.
MLFLOW_TRACKING_URI = f"sqlite:///{os.path.join(BASE_DIR, 'mlflow.db')}"
MLFLOW_EXPERIMENT_NAME = "Titanic_Survival_Airflow_MLflow"
REGISTERED_MODEL_NAME = "TitanicSurvivalModel"

mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

default_args = {
    "owner": "student",
    "depends_on_past": False,
    "retries": 2,  # Required for Task 3 retry demonstration
    "retry_delay": timedelta(seconds=20),
}


# ============================================================
# TASK 2 - DATA INGESTION
# ============================================================
# Requirements covered:
# - Load Titanic CSV file
# - Print dataset shape
# - Log missing values count
# - Push dataset path using XCom
def ingest_data(**context):
    if not os.path.exists(DATASET_PATH):
        raise FileNotFoundError(
            f"Titanic dataset not found at {DATASET_PATH}. "
            f"Please place Titanic-Dataset.csv inside the data folder."
        )

    df = pd.read_csv(DATASET_PATH)

    logging.info("========== TASK 2: DATA INGESTION ==========")
    logging.info(f"Dataset shape: {df.shape}")
    logging.info("Missing values count:")
    logging.info(df.isnull().sum())

    # Push dataset path using XCom
    context["ti"].xcom_push(key="dataset_path", value=DATASET_PATH)

    return DATASET_PATH


# ============================================================
# TASK 3 - DATA VALIDATION
# ============================================================
# Requirements covered:
# - Check missing percentage in Age and Embarked
# - Raise exception if missing > 30%
# - Demonstrate retry behavior with intentional failure
def validate_data(**context):
    ti = context["ti"]
    dataset_path = ti.xcom_pull(task_ids="data_ingestion", key="dataset_path")

    df = pd.read_csv(dataset_path)

    age_missing_pct = df["Age"].isnull().mean() * 100
    embarked_missing_pct = df["Embarked"].isnull().mean() * 100

    logging.info("========== TASK 3: DATA VALIDATION ==========")
    logging.info(f"Age missing percentage: {age_missing_pct:.2f}%")
    logging.info(f"Embarked missing percentage: {embarked_missing_pct:.2f}%")

    # Intentional failure on first run only to demonstrate Airflow retry
    retry_flag_file = os.path.join(ARTIFACT_DIR, "validation_retry_flag.txt")
    if not os.path.exists(retry_flag_file):
        with open(retry_flag_file, "w") as f:
            f.write("failed_once_for_retry_demo")
        raise Exception("Intentional failure for retry demonstration in Airflow.")

    if age_missing_pct > 30 or embarked_missing_pct > 30:
        raise ValueError("Missing percentage in Age or Embarked is greater than 30%.")

    logging.info("Validation passed successfully.")
    return "validation_passed"


# ============================================================
# TASK 4 - PARALLEL PROCESSING (PART 1)
# HANDLE MISSING VALUES
# ============================================================
# Requirements covered:
# - Handle missing values in Age and Embarked
# - This task runs in parallel with feature_engineering
def handle_missing_values(**context):
    ti = context["ti"]
    dataset_path = ti.xcom_pull(task_ids="data_ingestion", key="dataset_path")

    df = pd.read_csv(dataset_path)

    logging.info("========== TASK 4: HANDLE MISSING VALUES ==========")

    # Fill missing Age using median
    age_imputer = SimpleImputer(strategy="median")
    df["Age"] = age_imputer.fit_transform(df[["Age"]]).ravel()

    # Fill missing Embarked using most frequent value
    embarked_imputer = SimpleImputer(strategy="most_frequent")
    df["Embarked"] = embarked_imputer.fit_transform(df[["Embarked"]]).ravel()

    output_path = os.path.join(ARTIFACT_DIR, "missing_handled.csv")
    df.to_csv(output_path, index=False)

    ti.xcom_push(key="missing_handled_path", value=output_path)
    logging.info(f"Missing values handled file saved at: {output_path}")

    return output_path


# ============================================================
# TASK 4 - PARALLEL PROCESSING (PART 2)
# FEATURE ENGINEERING
# ============================================================
# Requirements covered:
# - Create FamilySize
# - Create IsAlone
# - This task runs in parallel with handle_missing_values
def feature_engineering(**context):
    ti = context["ti"]
    dataset_path = ti.xcom_pull(task_ids="data_ingestion", key="dataset_path")

    df = pd.read_csv(dataset_path)

    logging.info("========== TASK 4: FEATURE ENGINEERING ==========")

    # FamilySize = SibSp + Parch + 1
    df["FamilySize"] = df["SibSp"] + df["Parch"] + 1

    # IsAlone = 1 if passenger has no family aboard, else 0
    df["IsAlone"] = (df["FamilySize"] == 1).astype(int)

    output_path = os.path.join(ARTIFACT_DIR, "feature_engineered.csv")
    df.to_csv(output_path, index=False)

    ti.xcom_push(key="feature_engineered_path", value=output_path)
    logging.info(f"Feature engineered file saved at: {output_path}")

    return output_path


# ============================================================
# TASK 4 - MERGE PARALLEL OUTPUTS
# ============================================================
# Since missing value handling and feature engineering run in parallel,
# this task merges the outputs into one final processed dataset.
def merge_parallel_outputs(**context):
    ti = context["ti"]

    missing_path = ti.xcom_pull(task_ids="handle_missing_values", key="missing_handled_path")
    feature_path = ti.xcom_pull(task_ids="feature_engineering", key="feature_engineered_path")

    df_missing = pd.read_csv(missing_path)
    df_feature = pd.read_csv(feature_path)

    logging.info("========== TASK 4: MERGING PARALLEL OUTPUTS ==========")

    # Keep cleaned Age and Embarked from the missing-values output
    df_feature["Age"] = df_missing["Age"]
    df_feature["Embarked"] = df_missing["Embarked"]

    output_path = os.path.join(ARTIFACT_DIR, "processed_merged.csv")
    df_feature.to_csv(output_path, index=False)

    ti.xcom_push(key="processed_path", value=output_path)
    logging.info(f"Merged processed file saved at: {output_path}")

    return output_path


# ============================================================
# TASK 5 - DATA ENCODING
# ============================================================
# Requirements covered:
# - Encode categorical variables (Sex, Embarked)
# - Drop irrelevant columns
def encode_data(**context):
    ti = context["ti"]
    processed_path = ti.xcom_pull(task_ids="merge_parallel_outputs", key="processed_path")

    df = pd.read_csv(processed_path)

    logging.info("========== TASK 5: DATA ENCODING ==========")

    # Drop irrelevant columns
    df = df.drop(columns=["PassengerId", "Name", "Ticket", "Cabin"], errors="ignore")

    # Encode Sex column
    df["Sex"] = df["Sex"].map({"male": 0, "female": 1})

    # One-hot encode Embarked
    df = pd.get_dummies(df, columns=["Embarked"], drop_first=True)

    output_path = os.path.join(ARTIFACT_DIR, "encoded_data.csv")
    df.to_csv(output_path, index=False)

    ti.xcom_push(key="encoded_path", value=output_path)
    logging.info(f"Encoded dataset saved at: {output_path}")

    return output_path


# ============================================================
# TASK 6 - MODEL TRAINING WITH MLFLOW
# ============================================================
# Requirements covered:
# - Start MLflow run
# - Log model type and hyperparameters
# - Train Logistic Regression or Random Forest
# - Log model artifact and dataset size
def train_model(**context):
    ti = context["ti"]
    encoded_path = ti.xcom_pull(task_ids="data_encoding", key="encoded_path")

    df = pd.read_csv(encoded_path)

    logging.info("========== TASK 6: MODEL TRAINING WITH MLFLOW ==========")

    X = df.drop("Survived", axis=1)
    y = df["Survived"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    # Save test data for evaluation task
    X_test_path = os.path.join(ARTIFACT_DIR, "X_test.csv")
    y_test_path = os.path.join(ARTIFACT_DIR, "y_test.csv")
    X_test.to_csv(X_test_path, index=False)
    y_test.to_csv(y_test_path, index=False)

    ti.xcom_push(key="X_test_path", value=X_test_path)
    ti.xcom_push(key="y_test_path", value=y_test_path)

    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}

    # Allow multiple runs with different hyperparameters for Task 10
    model_type = conf.get("model_type", "random_forest")

    mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)

    with mlflow.start_run(run_name=f"{model_type}_run") as run:
        if model_type == "logistic_regression":
            C = float(conf.get("C", 1.0))
            max_iter = int(conf.get("max_iter", 300))

            model = LogisticRegression(
                C=C,
                max_iter=max_iter,
                random_state=42
            )

            mlflow.log_param("model_type", "LogisticRegression")
            mlflow.log_param("C", C)
            mlflow.log_param("max_iter", max_iter)

        else:
            n_estimators = int(conf.get("n_estimators", 200))
            max_depth_value = conf.get("max_depth", 8)
            max_depth = None if max_depth_value in [None, "None", "null"] else int(max_depth_value)

            model = RandomForestClassifier(
                n_estimators=n_estimators,
                max_depth=max_depth,
                random_state=42
            )

            mlflow.log_param("model_type", "RandomForest")
            mlflow.log_param("n_estimators", n_estimators)
            mlflow.log_param("max_depth", max_depth if max_depth is not None else "None")

        # Log dataset information
        mlflow.log_param("dataset_size", len(df))
        mlflow.log_param("train_size", len(X_train))
        mlflow.log_param("test_size", len(X_test))

        # Train model
        model.fit(X_train, y_train)

        # Log trained model artifact
        mlflow.sklearn.log_model(model, artifact_path="model")

        # Save run ID for later tasks
        ti.xcom_push(key="run_id", value=run.info.run_id)
        ti.xcom_push(key="model_type_used", value=model_type)

        logging.info(f"MLflow run started with run_id: {run.info.run_id}")
        logging.info(f"Model type used: {model_type}")

    return "training_completed"


# ============================================================
# TASK 7 - MODEL EVALUATION
# ============================================================
# Requirements covered:
# - Compute Accuracy, Precision, Recall, F1-score
# - Log all metrics to MLflow
# - Push accuracy using XCom
def evaluate_model(**context):
    ti = context["ti"]
    run_id = ti.xcom_pull(task_ids="model_training", key="run_id")

    X_test_path = ti.xcom_pull(task_ids="model_training", key="X_test_path")
    y_test_path = ti.xcom_pull(task_ids="model_training", key="y_test_path")

    X_test = pd.read_csv(X_test_path)
    y_test = pd.read_csv(y_test_path).squeeze()

    logging.info("========== TASK 7: MODEL EVALUATION ==========")

    model_uri = f"runs:/{run_id}/model"
    model = mlflow.sklearn.load_model(model_uri)

    y_pred = model.predict(X_test)

    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred, zero_division=0)
    recall = recall_score(y_test, y_pred, zero_division=0)
    f1 = f1_score(y_test, y_pred, zero_division=0)

    # Log evaluation metrics to the same MLflow run
    with mlflow.start_run(run_id=run_id):
        mlflow.log_metric("accuracy", accuracy)
        mlflow.log_metric("precision", precision)
        mlflow.log_metric("recall", recall)
        mlflow.log_metric("f1_score", f1)

    # Push accuracy for branching logic
    ti.xcom_push(key="accuracy", value=float(accuracy))
    ti.xcom_push(key="rejection_reason", value="Accuracy below threshold of 0.80")

    logging.info(f"Accuracy: {accuracy:.4f}")
    logging.info(f"Precision: {precision:.4f}")
    logging.info(f"Recall: {recall:.4f}")
    logging.info(f"F1-score: {f1:.4f}")

    return float(accuracy)


# ============================================================
# TASK 8 - BRANCHING LOGIC
# ============================================================
# Requirements covered:
# - Use BranchPythonOperator
# - If Accuracy >= 0.80 -> register_model
# - Else -> reject_model
def branching_decision(**context):
    ti = context["ti"]
    accuracy = ti.xcom_pull(task_ids="model_evaluation", key="accuracy")

    logging.info("========== TASK 8: BRANCHING LOGIC ==========")
    logging.info(f"Accuracy received from XCom: {accuracy}")

    if accuracy >= 0.80:
        logging.info("Accuracy is >= 0.80, proceeding to register_model.")
        return "register_model"
    else:
        logging.info("Accuracy is < 0.80, proceeding to reject_model.")
        return "reject_model"


# ============================================================
# TASK 9 - MODEL REGISTRATION
# ============================================================
# Requirements covered:
# - Register approved model in MLflow Model Registry
def register_model_task(**context):
    ti = context["ti"]
    run_id = ti.xcom_pull(task_ids="model_training", key="run_id")

    logging.info("========== TASK 9: MODEL REGISTRATION ==========")

    model_uri = f"runs:/{run_id}/model"
    registered_model = mlflow.register_model(
        model_uri=model_uri,
        name=REGISTERED_MODEL_NAME
    )

    logging.info(
        f"Model successfully registered in MLflow Model Registry. "
        f"Model Name: {REGISTERED_MODEL_NAME}, Version: {registered_model.version}"
    )

    return f"Model registered as version {registered_model.version}"


# ============================================================
# TASK 9 - MODEL REJECTION
# ============================================================
# Requirements covered:
# - Log rejection reason if accuracy is low
def reject_model_task(**context):
    ti = context["ti"]
    accuracy = ti.xcom_pull(task_ids="model_evaluation", key="accuracy")
    reason = ti.xcom_pull(task_ids="model_evaluation", key="rejection_reason")
    run_id = ti.xcom_pull(task_ids="model_training", key="run_id")

    logging.info("========== TASK 9: MODEL REJECTION ==========")

    with mlflow.start_run(run_id=run_id):
        mlflow.set_tag("model_status", "rejected")
        mlflow.set_tag("rejection_reason", reason)

    logging.info(f"Model rejected because accuracy ({accuracy:.4f}) is below 0.80.")
    logging.info(f"Rejection reason logged to MLflow: {reason}")

    return "Model rejected"


# ============================================================
# TASK 1 - DAG DESIGN
# ============================================================
# Requirements covered:
# - Create DAG with parallel tasks and branching logic
# - Demonstrate no cyclic dependencies
# - DAG structure is visible in Airflow Graph View
# - Task dependencies can be explained from this layout
with DAG(
    dag_id="mlops_airflow_mlflow_pipeline",
    default_args=default_args,
    description="End-to-end MLOps pipeline using Apache Airflow and MLflow for Titanic survival prediction",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["mlops", "airflow", "mlflow", "titanic"],
) as dag:

    # Start node
    start = EmptyOperator(task_id="start")

    # Task 2
    data_ingestion = PythonOperator(
        task_id="data_ingestion",
        python_callable=ingest_data
    )

    # Task 3
    data_validation = PythonOperator(
        task_id="data_validation",
        python_callable=validate_data
    )

    # Task 4 parallel branch 1
    handle_missing_values_task = PythonOperator(
        task_id="handle_missing_values",
        python_callable=handle_missing_values
    )

    # Task 4 parallel branch 2
    feature_engineering_task = PythonOperator(
        task_id="feature_engineering",
        python_callable=feature_engineering
    )

    # Merge results from parallel tasks
    merge_parallel_outputs_task = PythonOperator(
        task_id="merge_parallel_outputs",
        python_callable=merge_parallel_outputs
    )

    # Task 5
    data_encoding = PythonOperator(
        task_id="data_encoding",
        python_callable=encode_data
    )

    # Task 6
    model_training = PythonOperator(
        task_id="model_training",
        python_callable=train_model
    )

    # Task 7
    model_evaluation = PythonOperator(
        task_id="model_evaluation",
        python_callable=evaluate_model
    )

    # Task 8
    branching = BranchPythonOperator(
        task_id="branching_decision",
        python_callable=branching_decision
    )

    # Task 9 success path
    register_model = PythonOperator(
        task_id="register_model",
        python_callable=register_model_task
    )

    # Task 9 rejection path
    reject_model = PythonOperator(
        task_id="reject_model",
        python_callable=reject_model_task
    )

    # End node
    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success"
    )

    # ========================================================
    # DAG DEPENDENCIES
    # ========================================================
    # start -> data_ingestion -> data_validation
    # data_validation -> [handle_missing_values, feature_engineering]  (parallel tasks)
    # parallel tasks -> merge_parallel_outputs -> data_encoding
    # data_encoding -> model_training -> model_evaluation
    # model_evaluation -> branching_decision
    # branching_decision -> register_model OR reject_model
    # register_model / reject_model -> end

    start >> data_ingestion >> data_validation
    data_validation >> [handle_missing_values_task, feature_engineering_task]
    [handle_missing_values_task, feature_engineering_task] >> merge_parallel_outputs_task
    merge_parallel_outputs_task >> data_encoding >> model_training >> model_evaluation >> branching
    branching >> register_model >> end
    branching >> reject_model >> end
