from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from model.train_pipeline import train_and_log

with DAG(
    dag_id="training_pipeline",
    start_date=datetime(2025,1,1),
    schedule_interval="@daily",
    catchup=False,
    tags=["training","mlflow"],
) as dag:
    PythonOperator(task_id="train_model", python_callable=train_and_log)
