from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from extract import get_weather, collect_raw_weather
from transform import save_structured_weather, collect_weather
from db import get_session
from anomalies import compare_weather, check_anomalies, save_anomaly

def task_extract_weather(**context):
    db = get_session()
    try:
        weather = get_weather(lon=37.62, lat=55.75)
        if weather:
            collect_raw_weather(db, weather)
    finally:
        db.close()

def task_transform_weather(**context):
    db = get_session()
    try:
        from db import RawWeather
        last_raw = db.query(RawWeather).order_by(RawWeather.id.desc()).first()
        if last_raw:
            save_structured_weather(db, last_raw)
    finally:
        db.close()

def task_detect_anomalies(**context):
    db = get_session()
    try:
        from db import LocationToTrack
        locations = db.query(LocationToTrack).all()
        for loc in locations:
            check_anomalies(db, loc.lat, loc.lon, loc)
    finally:
        db.close()


default_args = {
    'owner': 'Cheburek',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': False,
}
with DAG(
    dag_id='weather_pipeline',
    description='Weather ETL + anomaly detection',
    start_date=datetime(2026, 5, 9),
    schedule_interval='*/15 * * * *', #15 минут
    catchup=False,
    default_args=default_args,
    tags=['weather', 'etl'],
)    as dag:

    extract = PythonOperator(
        task_id='extract_weather',
        python_callable=task_extract_weather,
    )
    transform = PythonOperator(
        task_id='transform_weather',
        python_callable=task_transform_weather,
    )
    detect = PythonOperator(
        task_id='detect_anomalies',
        python_callable=task_detect_anomalies,
    )
    extract >> transform >> detect

