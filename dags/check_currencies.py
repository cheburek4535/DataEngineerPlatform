from airflow import DAG
from airflow.operators.python import PythonOperator
from services.db.db import get_session
from services.currency.sharpjumps import check_currency_anomalies
from logger import logger
from datetime import datetime, timedelta

def task_check_currencies(**context):
    db = get_session()
    try:
        logger.info("Проверяем все валюты")
        check_currency_anomalies(db)
    finally:
        db.close()

default_args = {
    'owner': 'Cheburek',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': False,
}
with DAG(
    dag_id='check_currencies',
    description='Проверка всех валют на скачки и сбор данных',
    start_date=datetime(2026, 5, 12),
    schedule_interval=None, #120 минут
    catchup=False,
    default_args=default_args,
    tags=['anomalies', 'etl', 'currencies'],
)    as dag:
    check_currencies = PythonOperator(task_id='check_currencies', python_callable=task_check_currencies)