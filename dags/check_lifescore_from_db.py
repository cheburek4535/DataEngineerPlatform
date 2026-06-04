from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from services.db.db import get_session
from services.life_score.service import process_all_locations
from logger import logger

default_args = {
    'owner': 'Cheburek',
    'start_date': datetime(2026, 5, 21),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def check_lifescores_from_db():
    db = get_session()
    offset = 0
    processed = 0
    try:
        while processed < 25000:
            logger.info(f'Проверяем локации с {offset}')
            process_all_locations(db=db, offset=offset, limit=500, mode="from_db")
            offset += 500
            processed += 500
        logger.info("Достигнут лимит 25000 локаций")
        return processed
    except Exception as e:
        logger.error(e)
        raise
    finally:
        db.close()

with DAG(
    'check_lifescores_from_db',
    default_args=default_args,
    description='Проверка всех локаций на качество жизни из БД',
    schedule_interval=None,
    catchup=False,
    tags=['lifescore'],
max_active_runs=1,
) as dag:

    check_lifescores_from_db = PythonOperator(
            task_id='check_lifescores_from_db',
        python_callable=check_lifescores_from_db,
    )