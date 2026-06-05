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

    ch.command("TRUNCATE TABLE weather")

    new_rows = pg.query(models.Weather).all()

    if not new_rows:
        logger.info("Нет новых данных для синхронизации weather")
        pg.close()
        return

    data = [[
        r.id, r.location.lat, r.location.lon, r.location_id, r.timestamp, r.temperature, r.humidity, r.pressure, r.wind_speed, r.collected_at
    ] for r in new_rows]

    ch.insert('weather', data,
              column_names=['id', 'lat', 'lon', 'location_id', 'timestamp', 'temperature', 'humidity', 'pressure', 'wind_speed', 'collected_at'])
    logger.info(f"Синхронизировано {len(data)} записей weather")
    pg.close()

def sync_anomalies(**context):
    pg = get_session()
    ch = get_ch_client()

    # Clear and reload
    ch.command("TRUNCATE TABLE anomalies")

    rows = pg.query(models.Anomaly).all()
    data = [
        [
            a.id, a.location.lat, a.location.lon, a.location_id,
            a.anomaly_temperature, a.anomaly_pressure,
            a.anomaly_humidity, a.anomaly_wind_speed,
            str(a.additional_data), a.found_at
        ]
        for a in rows
    ]

    if data:
        ch.insert(
            'anomalies',
            data,
            column_names=[
                'id', 'lat', 'lon', 'location_id', 'anomaly_temperature',
                'anomaly_pressure', 'anomaly_humidity', 'anomaly_wind_speed',
                'additional_data', 'found_at'
            ]
        )
        logger.info(f"Синхронизировано {len(data)} записей anomalies")
    else:
        logger.info("Таблица anomalies в PG пуста")

    pg.close()



def sync_currency(**context):
    pg = get_session()
    ch = get_ch_client()

    ch.command("TRUNCATE TABLE IF EXISTS weather_analytics.currencies")

    rows = pg.query(models.Currency).all()
    data = [
        [r.id, r.name, r.code, float(r.value_in_rubles), r.created_at, r.updated_at]  # Decimal → float
        for r in rows
    ]

    ch.insert('weather_analytics.currencies', data,
              column_names=['id', 'name', 'code', 'value_in_rubles', 'created_at', 'updated_at'])
    logger.info(f"✅ {len(data)} currencies")
    pg.close()

def sync_currency_sharp_changes(**context):
    pg = get_session()
    ch = get_ch_client()

    # max(id) из CH
    result = ch.query("SELECT max(id) FROM weather_analytics.currency_sharp_changes")
    max_id = result.result_rows[0][0] or 0

    new_rows = pg.query(models.CurrencySharpChange).filter(
        models.CurrencySharpChange.id > max_id
    ).all()

    if not new_rows:
        logger.info("Нет новых sharp_changes")
        pg.close()
        return

    data = [[
        r.id,
        float(r.change_percents),
        float(r.value_in_rubles),
        float(r.previous_value) if r.previous_value else None,
        r.currency.code,  # ← currency.code вместо currency_id!
        r.found_at
    ] for r in new_rows]

    ch.insert('weather_analytics.currency_sharp_changes', data,
              column_names=['id', 'change_percents', 'value_in_rubles', 'previous_value', 'currency_code', 'found_at'])
    logger.info(f"✅ {len(data)} sharp_changes")
    pg.close()

def sync_air_quality(**context):
    pg = get_session()
    ch = get_ch_client()

    result = ch.query("SELECT max(id) FROM air_quality")
    max_id = result.result_rows[0][0] or 0

    new_rows = pg.query(models.AirQuality).filter(models.AirQuality.id > max_id).all()

    if not new_rows:
        logger.info("Нет новых данных для синхронизации air quality")
        pg.close()
        return

    data = [
        [
            a.id, a.location.lat, a.location.lon, a.location_id,
            a.pm25, a.pm10,
            a.no2, a.o3,
            a.so2, a.co, a.collected_at, a.is_good, a.air_quality_level
        ]
        for a in new_rows
    ]

    if data:
        ch.insert(
            'air_quality',
            data,
            column_names=[
                'id', 'lat', 'lon', 'location_id', 'pm25', 'pm10', 'no2', 'o3', 'so2', 'co', 'collected_at', 'is_good', 'aq_level'
            ]
        )
        logger.info(f"Синхронизировано {len(data)} записей air_quality")
    else:
        logger.info("Таблица air_quality в PG пуста")

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
    schedule_interval=None,
    start_date=datetime(2026, 5, 10),
    catchup=False,
    tags=['clickhouse', 'sync'],
max_active_runs=1,
) as dag:

    sync_w = PythonOperator(
        task_id='sync_weather',
        python_callable=sync_weather,
    )

    sync_a = PythonOperator(
        task_id='sync_anomalies',
        python_callable=sync_anomalies,
    )

    sync_c = PythonOperator(
        task_id='sync_currency',
        python_callable=sync_currency,
    )

    sync_sc = PythonOperator(
        task_id='sync_currency_sharp_changes',
        python_callable=sync_currency_sharp_changes,
    )
    sync_aq = PythonOperator(
        task_id='sync_air_quality',
        python_callable=sync_air_quality,
    )
    sync_w >> sync_a >> sync_c >> sync_sc >> sync_aq
