from typing import Optional

from grpc_client import LifeScoreClient
from sqlalchemy.orm import Session
from sqlalchemy.sql import asc, desc
from services.db.models import LocationToTrack, Weather, AirQuality, Anomaly, LocationLifeScore
from datetime import datetime, timezone
from logger import logger

def process_all_locations(db: Session, offset: int = 0, limit: int = 500, mode: str = 'from_db', loc: LocationToTrack = None):
    if mode == 'from_db':
        locations = db.query(LocationToTrack).filter_by(is_on_land=True).order_by(asc(LocationToTrack.id)).offset(offset).limit(limit).all()
        logger.info(f"Найдено {len(locations)} локаций для проверки")
        result = []
        for location in locations:
            weather_rows = db.query(Weather).filter(Weather.location_id==location.id).order_by(desc(Weather.timestamp)).limit(200).all()
            logger.info(f"Найдено {len(weather_rows)} записей о погоде для локации {location.id}")
            aq_rows = db.query(AirQuality).filter(AirQuality.location_id==location.id).order_by(desc(AirQuality.collected_at)).limit(10).all()
            logger.info(f"Найдено {len(aq_rows)} записей о AQ для локации {location.id}")
            weather_anomalies = db.query(Anomaly).filter(Anomaly.location_id==location.id).order_by(desc(Anomaly.found_at)).limit(5).all()
            logger.info(f"Найдено {len(weather_anomalies)} записей о аномалиях для локации {location.id}")

            if not weather_rows:
                logger.info(f"Не найдено Weather rows для локации {location.id}, скип")
                continue
            result.append({
                'loc_id': location.id,
                'aq': [{'pm25': a.pm25, 'pm10': a.pm10, 'no2': a.no2, 'o3': a.o3, 'so2': a.so2, 'co': a.co} for a in aq_rows],
                'weather': [{'temp': w.temperature, 'hum': w.humidity, 'pres': w.pressure, 'wind': w.wind_speed} for w in weather_rows],
                'anomalies': [{'temp': a.anomaly_temperature, 'hum': a.anomaly_humidity, 'pres': a.anomaly_pressure, 'wind': a.anomaly_wind_speed} for a in weather_anomalies],

            })
            logger.info(f"Локация {location.id} добавлена в батч")
    elif mode == 'from_param' and loc:
        # weather = db.query(Weather).filter_by(lat=loc.lat, lon=loc.lon).first()
        result = {}
        # ТУТ ЛОГИКА ДЛЯ БУДУЩЕГО ПАЙПЛАЙНА БУДЕТ
    else:
        return None
    scores = calculate_locs_go(result)
    if scores:
        for score in scores:
            save_score(db, score)
    return None

def calculate_locs_go(data: list[dict]):
    if not data:
        return None
    logger.info("Отправляем батч в Go")

    # Используем gRPC клиент
    client = LifeScoreClient(host="host.docker.internal", port=50051)
    try:
        scores = client.calculate_batch(data) # Если все помещается в память
        # scores = client.calculate_scores_streaming(data)  # Для очень больших данных
        if scores:
            logger.info(f"Go вернул результат из {len(scores)} элементов! Первый элемент ответа: {scores[0]}")

        return scores
    finally:
        client.close()

def save_score(db: Session, score: dict) -> Optional[LocationLifeScore]:
    if score:
        exists = db.query(LocationLifeScore).filter_by(location_id=score.get('location_id')).first()
        if not exists:
            db_ls = LocationLifeScore(created_at=datetime.now(timezone.utc), **score)
            db.add(db_ls)
            db.commit()
            db.refresh(db_ls)
            logger.info(f"LS для локации {db_ls.location_id} ДОБАВЛЕН в БД")
            return db_ls
        else:
            for key, value in score.items():
                setattr(exists, key, value)
            exists.updated_at = datetime.now(timezone.utc)
            db.commit()
            logger.info(f"LS для локации {exists.location_id} ОБНОВЛЕН в БД (уже существовал)")
            return exists

    return None


