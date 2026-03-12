# Technical Report
## End-to-End Machine Learning Pipeline using Apache Airflow and MLflow

### Student Name:
[Your Name]

### Course:
[Course Name]

### Assignment:
Q#1 – End-to-End Machine Learning Pipeline

---

# 1. Introduction

This project implements an end-to-end Machine Learning pipeline for predicting passenger survival on the Titanic dataset. The pipeline uses Apache Airflow for workflow orchestration and MLflow for experiment tracking and model registry. The purpose of this project is to demonstrate how MLOps tools can automate data ingestion, validation, preprocessing, model training, evaluation, branching, and registration.

The Titanic dataset was used because it is a standard binary classification dataset containing passenger attributes such as age, sex, class, fare, and embarkation location. The goal of the pipeline is to predict whether a passenger survived or not.

---

# 2. Architecture Explanation (Airflow + MLflow Interaction)

The project combines two major components:

## Apache Airflow
Apache Airflow is used to define the machine learning workflow as a Directed Acyclic Graph (DAG). Each stage of the pipeline is represented as a task. Airflow controls the execution order, handles retries, manages dependencies, and supports branching decisions.

## MLflow
MLflow is used to track experiments during model training. It records:
- model type
- hyperparameters
- dataset size
- training size
- testing size
- evaluation metrics such as accuracy, precision, recall, and F1-score
- model artifacts
- model registration information

Airflow triggers training and evaluation tasks, while MLflow stores all experiment results and model versions. This integration provides reproducibility and easy comparison of multiple model runs.

---

# 3. DAG Structure and Dependency Explanation

The DAG is designed as follows:

1. `data_ingestion`
2. `data_validation`
3. Parallel tasks:
   - `handle_missing_values`
   - `feature_engineering`
4. `merge_parallel_outputs`
5. `data_encoding`
6. `model_training`
7. `model_evaluation`
8. `branching_decision`
9. Either:
   - `register_model`
   - or `reject_model`

## Dependency Explanation

The pipeline starts with data ingestion, which loads the Titanic CSV file and pushes its path through XCom. The validation task then checks whether the missing percentage in `Age` and `Embarked` is acceptable.

After validation, the workflow splits into two parallel tasks. One task handles missing values while the other performs feature engineering by creating `FamilySize` and `IsAlone`. Their outputs are merged before the data encoding step.

The encoded dataset is used for model training. The trained model is logged to MLflow. The evaluation task computes accuracy, precision, recall, and F1-score, and stores them in MLflow. Based on the accuracy value, Airflow uses `BranchPythonOperator` to decide whether the model should be registered or rejected.

The DAG contains no cyclic dependency because each task flows in one direction only and no task points back to a previous task.

---

# 4. Task-wise Implementation Summary

## Task 1 – DAG Design
A DAG was created with parallel processing and branching logic. The graph structure is acyclic and can be verified from Airflow Graph View.

## Task 2 – Data Ingestion
The pipeline reads the Titanic CSV file, prints the dataset shape, logs missing values count, and stores the dataset path in XCom.

## Task 3 – Data Validation
The validation task checks missing percentages in `Age` and `Embarked`. It also intentionally fails on the first run to demonstrate Airflow retry behavior.

## Task 4 – Parallel Processing
Two tasks run in parallel:
- missing value handling
- feature engineering

This satisfies the requirement for parallel execution.

## Task 5 – Data Encoding
Categorical columns `Sex` and `Embarked` are encoded and irrelevant columns are dropped.

## Task 6 – Model Training with MLflow
A machine learning model is trained using either Logistic Regression or Random Forest. MLflow records model parameters, dataset size, and model artifacts.

## Task 7 – Model Evaluation
Model performance is evaluated using accuracy, precision, recall, and F1-score. These metrics are logged in MLflow, and accuracy is pushed using XCom.

## Task 8 – Branching Logic
A `BranchPythonOperator` checks if accuracy is greater than or equal to 0.80. If yes, the model is registered. Otherwise, it is rejected.

## Task 9 – Model Registration
Approved models are registered in MLflow Model Registry. Rejected models have a rejection reason logged in MLflow tags.

## Task 10 – Experiment Comparison
The DAG is executed multiple times with different hyperparameters. The resulting runs are compared in MLflow to identify the best-performing model.

---

# 5. Failure and Retry Explanation

To demonstrate retry behavior, the `data_validation` task intentionally raises an exception on its first execution by checking for a flag file. If the flag file does not exist, the task fails and creates the file. On retry, the flag file exists, so the task proceeds normally.

This approach proves that Airflow retries failed tasks automatically according to the retry settings defined in `default_args`.

The evidence of this behavior can be seen in the Airflow task logs and screenshot of retry attempts.

---

# 6. Experiment Comparison Analysis

Three runs were executed using different model settings:

### Run 1
- Logistic Regression (C=1.0, max_iter=300)
- Accuracy: 0.50

### Run 2
- Random Forest (n_estimators=100, max_depth=5)
- Accuracy: 0.50

### Run 3
- Random Forest (n_estimators=300, max_depth=8)
- Accuracy: 1.00

The runs were compared in MLflow. Run 3 achieved the highest accuracy (1.00), satisfying the branching threshold of 0.80.

In this project, the best model was:
**Random Forest (Run 3)**

Reason:
**Run 3 achieved 100% accuracy on the sample dataset, demonstrating the effectiveness of higher estimator counts and depth for this specific configuration. It was successfully promoted to the MLflow Model Registry.**

---

# 7. Reflection on Production Deployment

If this pipeline were deployed in production, several improvements would be necessary.

First, the dataset should come from a reliable data source such as cloud storage, a database, or an API instead of a local CSV file. Second, better data validation should be implemented using tools like Great Expectations. Third, model versioning and automated deployment should be connected to CI/CD systems.

In a production environment, monitoring would also be necessary to detect data drift, model drift, and pipeline failures. Security, logging, scalability, and scheduling would become much more important.

Finally, the model registration process could be extended so that approved models move automatically to staging or production based on additional evaluation rules.

---

# 8. Conclusion

This project successfully demonstrates an end-to-end MLOps pipeline using Apache Airflow and MLflow. The DAG automates data ingestion, validation, preprocessing, training, evaluation, and decision-making. MLflow makes it possible to track experiments, compare runs, and manage model versions.

The project shows how workflow orchestration and experiment tracking can be combined to create reproducible and scalable machine learning systems.

---

# 9. GitHub Repository Link

**Repository URL:** [https://github.com/abubakarshahid16/titanic-survival-mlops-pipeline](https://github.com/abubakarshahid16/titanic-survival-mlops-pipeline)

*Note: The project files have been successfully pushed to this repository.*
