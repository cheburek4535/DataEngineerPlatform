from airflow import DAG
from airflow.operators.python import PythonOperator
from services.db.db import get_session
from services.air_quality.service import check_all_locations
from logger import logger
from datetime import datetime, timedelta
from services.api_limiter import RateLimitExceeded

def task_check_all_locations(**context):
    db = get_session()
    try:
        batch_id = "daily_" + datetime.now().strftime("%Y-%m-%d")
        logger.info(f"Старт batch_id={batch_id}")
        check_all_locations(db, batch_id=batch_id, max_locations_per_run=None)
    except RateLimitExceeded as e:
        logger.error(e.message)
        raise
    finally:
        db.close()


default_args = {
    "owner": "Cheburek",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": False,
}

with DAG(
    dag_id="check_air_quality",
    description="Проверка всех локаций на качество воздуха",
    start_date=datetime(2026, 5, 9),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["aq", "etl"],
    max_active_runs=1,
) as dag:
    check_locations = PythonOperator(
        task_id="check_air_quality",
        python_callable=task_check_all_locations,
    )