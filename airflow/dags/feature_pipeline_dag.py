from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from data_pipeline.jobs import feature_pipeline  # your file

with DAG(
    dag_id="feature_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["features","s3","warehouse"],
) as dag:
    t1 = PythonOperator(task_id="extract",  python_callable=feature_pipeline.extract_from_s3)
    t2 = PythonOperator(task_id="transform", python_callable=feature_pipeline.transform)
    t3 = PythonOperator(task_id="load",     python_callable=feature_pipeline.load_to_warehouse)
    t1 >> t2 >> t3
