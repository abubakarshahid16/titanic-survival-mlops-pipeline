# MLOps Assignment: Titanic Survival Pipeline

This project implements an end-to-end MLOps pipeline using Airflow for orchestration and MLflow for experiment tracking, applied to the Titanic dataset.

## Setup Instructions

1.  **Create a virtual environment**
    ```powershell
    python -m venv venv
    ```
2.  **Activate virtual environment**
    -   **On Windows:**
        ```powershell
        venv\Scripts\activate
        ```
    -   **On Linux / Mac:**
        ```bash
        source venv/bin/activate
        ```
3.  **Install dependencies**
    ```bash
    pip install -r requirements.txt
    ```
4.  **Set Airflow home directory**
    -   **On Windows PowerShell:**
        ```powershell
        $env:AIRFLOW_HOME = (Get-Location).Path
        ```
    -   **On Linux / Mac:**
        ```bash
        export AIRFLOW_HOME=$(pwd)
        ```
5.  **Create required folders**
    ```bash
    mkdir dags
    mkdir data
    mkdir artifacts
    ```
    *Note: Ensure `Titanic-Dataset.csv` is placed inside the `data` folder.*

6.  **Initialize Airflow database**
    ```bash
    airflow db init
    ```
7.  **Create Airflow admin user**
    ```bash
    airflow users create --username admin --firstname admin --lastname admin --role Admin --email admin@example.com
    ```
    *(You will be prompted for a password)*

8.  **Start Airflow webserver**
    ```bash
    airflow webserver --port 8080
    ```
9.  **Start Airflow scheduler**
    *(Open another terminal and run:)*
    ```bash
    airflow scheduler
    ```
10. **Start MLflow UI**
    *(Open another terminal and run:)*
    ```bash
    mlflow ui --backend-store-uri sqlite:///mlflow.db --port 5000
    ```

## Running the DAG

1.  Open Airflow UI: [http://localhost:8080](http://localhost:8080)
2.  Enable the DAG: `mlops_airflow_mlflow_pipeline`
3.  Trigger DAG runs with the following configurations for Task 10:

### Experiment Configurations (Task 10)

-   **Run 1**
    ```json
    {
      "model_type": "logistic_regression",
      "C": 1.0,
      "max_iter": 300
    }
    ```
-   **Run 2**
    ```json
    {
      "model_type": "random_forest",
      "n_estimators": 100,
      "max_depth": 5
    }
    ```
-   **Run 3**
    ```json
    {
      "model_type": "random_forest",
      "n_estimators": 300,
      "max_depth": 8
    }
    ```

## Required Screenshots
All proof screenshots for Task 1-10 have been automatically captured and are stored in the `screenshots/` folder:
- `01_Project_Setup.png`
- `02_Airflow_Graph_View.png`
- `03_Airflow_Grid_Success.png`
- `...` (and others)

## Submission
To submit:
1. Ensure `mlops_airflow_mlflow_pipeline.py` is in `dags/`.
2. Ensure `Titanic-Dataset.csv` is in `data/`.
3. Ensure the `screenshots/` folder and `technical_report.md` are present.
4. ZIP the entire `mlops_assignment` folder and submit.
