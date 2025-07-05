from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from weather_tourism.scripts.historical.extract import extract_historical_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
}


with DAG(
    dag_id='weather_tourism_historical',
    default_args=default_args,
    schedule=None,  # Exécution manuelle ou ponctuelle
    catchup=False,
    tags=['weather', 'historical'],
) as dag:

    extract_historical = PythonOperator(
        task_id='extract_historical_weather',
        python_callable=extract_historical_data,
            )

    extract_historical # type: ignore
