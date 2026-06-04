from grpc_client import LifeScoreClient
from sqlalchemy.orm import Session
from sqlalchemy.sql import asc, desc
from services.db.models import LocationToTrack, Weather, AirQuality, Anomaly
import requests
from logger import logger

def process_all_locations(db: Session, offset: int = 0, limit: int = 500):
    locations = db.query(LocationToTrack).filter_by(is_on_land=True).order_by(asc(LocationToTrack.id)).offset(offset).limit(limit).all()
    result = []
    for location in locations:
        weather_rows = db.query(Weather).filter(Weather.lat==location.lat, Weather.lon==location.lon).order_by(desc(Weather.timestamp)).limit(200).all()
        aq_rows = db.query(AirQuality).filter(AirQuality.location_id==location.id).order_by(desc(AirQuality.collected_at)).limit(10).all()
        weather_anomalies = db.query(Anomaly).filter(Anomaly.location_id==location.id).order_by(desc(Anomaly.found_at)).limit(5).all()

        if not weather_rows or not aq_rows:
            continue
        result.append({
            'loc_id': location.id,
            'aq': [{'pm25': a.pm25, 'pm10': a.pm10, 'no2': a.no2, 'o3': a.o3, 'so2': a.so2, 'co': a.co} for a in aq_rows],
            'weather': [{'temp': w.temperature, 'hum': w.humidity, 'pres': w.pressure, 'wind': w.wind_speed} for w in weather_rows],
            'anomalies': [{'temp': a.anomaly_temperature, 'hum': a.anomaly_humidity, 'pres': a.anomaly_pressure, 'wind': a.anomaly_wind_speed} for a in weather_anomalies],

        })
    scores = calculate_locs_go(result)
    if scores:
        for score in scores:
            saved = save_score(db, score)
    return None

# def calculate_locs_go(data: list[dict]):
#     if not data:
#         return None
#     response = requests.post("http://golang:8000/life-score", json=data)
#     if response.status_code != 200:
#         logger.error(f"Go service returned error: {response.status_code}")
#         logger.error(f"Response text: {response.text}")
#         return None
#
#     result = response.json()
#     scores = result.get('result', [])
#
#     return scores

# В твоем основном коде
def calculate_locs_go(data: list[dict]):
    if not data:
        return None

    # Используем gRPC клиент
    client = LifeScoreClient()
    try:
        # Выбирай метод:
        # scores = client.calculate_scores_batch(data)  # Если все помещается в память
        scores = client.calculate_scores_streaming(data)  # Для очень больших данных
        return scores
    finally:
        client.close()

def save_score(db: Session, score: dict):
    if score:
        exists = db.query(LocationLifeScore).filter_by(location_id=score.get('location_id')).first()
        if not exists:
            db.add(score)
        else:
            exists = score
        db.commit()


