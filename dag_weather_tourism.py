from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from weather_tourism.scripts.extract import extract_realtime_data
from weather_tourism.scripts.transform import transform_weather

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 1),
}

with DAG(
    dag_id='weather_tourism_realtime',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['weather', 'realtime'],
) as dag:

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract_realtime_data
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform_weather
    )

    extract_task >> transform_task  # type: ignore
