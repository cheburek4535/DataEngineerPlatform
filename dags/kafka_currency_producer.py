from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from services.currency.producer_service import get_currency
from logger import logger

default_args = {
    'owner': 'Cheburek',
    'start_date': datetime(2026, 5, 21),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'kafka_currency_producer',
    default_args=default_args,
    description='Fetch currency rates and publish to Kafka',
    schedule_interval='0 */1 * * *',
    catchup=False,
    tags=['currency', 'kafka', 'producer'],
) as dag:

    fetch_currency = PythonOperator(
            task_id='fetch_and_publish_currency',
        python_callable=get_currency,
    )