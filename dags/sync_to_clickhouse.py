from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import clickhouse_connect
from services.db.db import get_session
from services.db import models
from logger import logger

CH_HOST = "clickhouse"
CH_USER = "default"
CH_PASSWORD = "ch_password"
CH_DB = "weather_analytics"

def get_ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST,
        user=CH_USER,
        password=CH_PASSWORD,
        database=CH_DB
    )

def sync_weather(**context):
    pg = get_session()
    ch = get_ch_client()

    result = ch.query("SELECT max(id) FROM weather")
    max_id = result.result_rows[0][0] or 0

    new_rows = pg.query(models.Weather).filter(models.Weather.id > max_id).all()

    if not new_rows:
        logger.info("Нет новых данных для синхронизации weather")
        pg.close()
        return

    data = [[
        r.id, r.lat, r.lon, r.timestamp, r.temperature, r.humidity, r.pressure, r.wind_speed, r.collected_at
    ] for r in new_rows]

    ch.insert('weather', data,
              column_names=['id', 'lat', 'lon', 'timestamp', 'temperature', 'humidity', 'pressure', 'wind_speed', 'collected_at'])
    logger.info(f"Синхронизировано {len(data)} записей weather")
    pg.close()

def sync_anomalies(**context):
    pg = get_session()
    ch = get_ch_client()
    result = ch.query("SELECT max(id) FROM anomalies")
    max_id = result.result_rows[0][0] or 0

    new_rows = pg.query(models.Anomaly).filter(models.Anomaly.id > max_id).all()

    if not new_rows:
        logger.info("Нет новых данных для синхронизации anomalies")
        pg.close()
        return
    data = [[
        a.id, a.location.lat, a.location.lon, a.location_id, a.anomaly_temperature, a.anomaly_pressure, a.anomaly_humidity, a.anomaly_wind_speed, str(a.additional_data), a.found_at
    ] for a in new_rows]
    ch.insert('anomalies', data,
              column_names=['id', 'lat', 'lon', 'location_id', 'anomaly_temperature', 'anomaly_pressure', 'anomaly_humidity', 'anomaly_wind_speed', 'additional_data', 'found_at'])
    logger.info(f"Синхронизировано {len(data)} записей anomalies")
    pg.close()

default_args = {
    'owner': 'Cheburek',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='sync_to_clickhouse',
    default_args=default_args,
    description='Синхронизация PostgreSQL с ClickHouse',
    schedule_interval='*/15 * * * *',
    start_date=datetime(2026, 5, 10),
    catchup=False,
    tags=['clickhouse', 'sync'],
) as dag:

    sync_w = PythonOperator(
        task_id='sync_weather',
        python_callable=sync_weather,
    )

    sync_a = PythonOperator(
        task_id='sync_anomalies',
        python_callable=sync_anomalies,
    )
    sync_w >> sync_a
