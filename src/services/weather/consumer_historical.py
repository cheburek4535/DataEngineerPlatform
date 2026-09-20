from services.db.db import get_session
from services.db.models import RawHistoricalWeather, HistoricalWeatherStats, Location
from sqlalchemy.orm import Session
from logger import logger
from datetime import datetime
import json


def save_historical_weather(db: Session, message: dict) -> HistoricalWeatherStats:
    """Сохраняет статистику погоды за год"""

    stats = message.get('statistics', {})

    # Проверяем, есть ли уже данные за этот год
    existing = db.query(HistoricalWeatherStats).filter_by(
        location_id=message['location_id'],
        year=message['year']
    ).first()

    if existing:
        logger.info(f"Historical weather for {message['location_id']} already exists")
        return existing

    # Сохраняем сырые данные
    raw = RawHistoricalWeather(
        location_id=message['location_id'],
        year=message['year'],
        data_json=message
    )
    db.add(raw)
    db.flush()

    # Сохраняем статистику
    weather_stats = HistoricalWeatherStats(
        location_id=message['location_id'],
        year=message['year'],
        temp_mean=stats['temperature']['mean'],
        temp_std=stats['temperature']['std'],
        temp_min=stats['temperature']['min'],
        temp_max=stats['temperature']['max'],
        humidity_mean=stats['humidity']['mean'],
        humidity_std=stats['humidity']['std'],
        pressure_mean=stats['pressure']['mean'],
        wind_speed_mean=stats['wind_speed']['mean'],
        # Сохраняем сэмплы как JSON (для Go)
        temp_samples=json.dumps(stats['temperature'].get('samples', [])),
        humidity_samples=json.dumps(stats['humidity'].get('samples', [])),
        processed_at=datetime.utcnow()
    )
    db.add(weather_stats)
    db.commit()

    return weather_stats


def process_historical_weather_message(message: dict) -> bool:
    """Обработчик сообщения из Kafka"""
    db = get_session()
    try:
        save_historical_weather(db, message)
        logger.info(f"Processed historical weather for location {message['location_id']}")
        return True
    except Exception as e:
        logger.error(f"Error processing historical weather: {e}")
        db.rollback()
        return False
    finally:
        db.close()