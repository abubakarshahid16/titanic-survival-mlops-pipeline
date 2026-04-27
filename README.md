# Titanic Survival MLOps Pipeline

An end-to-end MLOps project that uses the Titanic survival dataset to demonstrate orchestration, experiment tracking, model comparison, and registry workflows with **Airflow** and **MLflow**.

This repository is best positioned as a **production-pattern learning project**: the prediction task is simple, but the real value is in the pipeline structure around it.

## Problem this project solves

Many machine-learning repos stop at training one model in a notebook. Real ML systems need more than that:

- repeatable pipeline execution
- experiment comparison
- artifact logging
- model versioning
- traceable evaluation across runs

This project uses the Titanic dataset as a manageable example for demonstrating those MLOps building blocks.

## What this project does

The workflow combines:

- Airflow for orchestration
- MLflow for experiment tracking and registry
- multiple model configurations
- artifact and metric logging
- screenshot-backed proof of execution

## Project highlights

- orchestrated machine-learning workflow with Airflow
- experiment tracking and run comparison with MLflow
- multiple model configurations for branching experiments
- artifact logging and model registry usage
- full visual proof across setup, execution, tracking, and registry views

## Stack

- Orchestration: Apache Airflow
- Experiment tracking: MLflow
- Language: Python
- Dataset: Titanic survival dataset
- Models: Logistic Regression, Random Forest

## Pipeline overview

1. prepare project structure and dataset
2. run the DAG inside Airflow
3. execute multiple model configurations
4. compare runs in MLflow
5. inspect metrics, artifacts, and model versions
6. identify and register the strongest run

## Visual proof

### Project Setup

![Project Setup](screenshots/01_Project_Setup.png)

### Airflow Execution

| Graph View | Grid View |
| --- | --- |
| ![Airflow Graph View](screenshots/02_Airflow_Graph_View.png) | ![Airflow Grid Success](screenshots/03_Airflow_Grid_Success.png) |

### MLflow Tracking

| Runs Comparison | Best Run Metrics |
| --- | --- |
| ![MLflow Runs Comparison](screenshots/04_MLflow_Runs_Comparison.png) | ![MLflow Best Run Metrics](screenshots/05_MLflow_Best_Run_Metrics.png) |

### Registry and Artifacts

| Model Registry | Logged Artifacts |
| --- | --- |
| ![MLflow Model Registry](screenshots/06_MLflow_Model_Registry.png) | ![MLflow Artifacts Logged](screenshots/07_MLflow_Artifacts_Logged.png) |

## Local setup

```bash
pip install -r requirements.txt
```

Set Airflow home, initialize Airflow, and start:

- Airflow webserver
- Airflow scheduler
- MLflow UI

Then place `Titanic-Dataset.csv` inside `data/` and trigger the DAG.

## Repository contents

- `dags/`: Airflow DAG code
- `data/`: dataset input location
- `screenshots/`: execution and result images
- `technical_report.md`: supporting report
- `requirements.txt`: dependencies

## Why this project matters

- It demonstrates real MLOps thinking instead of notebook-only ML.
- It shows orchestration and experiment tracking together.
- It uses visual evidence instead of unsupported claims.
- It is useful for portfolio positioning in ML platform, pipeline, and applied MLOps roles.

## Industrial positioning

In a production environment, the same pattern would expand into:

- scheduled data ingestion
- automated validation checks
- environment-specific deployments
- model approval gates
- monitoring and retraining triggers

That makes this repository a solid **MLOps workflow prototype** rather than just a Titanic classifier.
