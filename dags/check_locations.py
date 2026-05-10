from airflow import DAG
from airflow.operators.python import PythonOperator
from services.db.db import get_session
from services.locations.service import check_all_locations
from logger import logger
from datetime import datetime, timedelta

def task_check_all_locations(**context):
    db = get_session()
    try:
        logger.info("Проверяем все локации")
        check_all_locations(db)
    finally:
        db.close()

default_args = {
    'owner': 'Cheburek',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': False,
}
with DAG(
    dag_id='check_locations',
    description='Проверка всех локаций на аномалии',
    start_date=datetime(2026, 5, 9),
    schedule_interval='*/30 * * * *', #30 минут
    catchup=False,
    default_args=default_args,
    tags=['anomalies', 'etl'],
)    as dag:
    check_locations = PythonOperator(task_id='check_all_locations', python_callable=task_check_all_locations)