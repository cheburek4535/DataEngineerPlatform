from typing import Optional, Dict
from sqlalchemy.orm import Session
from logger import logger
from services.db.db import get_session
from services.db.models import RawAirQuality, AirQuality, LocationToTrack
from services.api_limiter import ApiLimiter, RateLimitExceeded


def process_air_quality_msg(msg_value: dict) -> bool:
    db = get_session()
    try:
        loc_id = msg_value.get('location_id')
        if loc_id is None:
            return False
        logger.info(f"ОБРАБОТКА ЛОКАЦИИ {loc_id}")
        raw_aq = save_raw_aq(db, msg_value)
        transformed_aq = transform_air_quality(raw_aq)
        if not transformed_aq:
            logger.error("Не удалось трансформировать AQ")
            return False
        structured_aq = save_structured_aq(db, transformed_aq, loc_id)

        db.commit()
        logger.info(f"Successfully processed aq data")
        return True

    except Exception as e:
        db.rollback()
        logger.info(f"Failed to process aq data: {e}")
        return False
    finally:
        db.close()



def save_raw_aq(db: Session, data: dict) -> RawAirQuality:
    db_aq = RawAirQuality(
        # lat=data['lat'],
        # lon=data['lon'],
        json_data=data,
        collected_at=data.get('collected_at'),
    )
    db.add(db_aq)
    db.flush()
    db.refresh(db_aq)
    return db_aq


def transform_air_quality(raw_aq: RawAirQuality) -> Optional[Dict]:
    measurements = raw_aq.json_data.get("measurements", [])
    if not measurements:
        return None

    # Группируем по параметру, собираем все значения
    params = {}
    for m in measurements:
        p = m["parameter"]
        v = m["value"]
        if v is not None:
            params.setdefault(p, []).append(v)

    # Считаем среднее по каждому параметру
    def avg_or_none(key):
        values = params.get(key)
        return round(sum(values) / len(values), 3) if values else None

    return {
        "pm25": avg_or_none("pm25"),
        "pm10": avg_or_none("pm10"),
        "no2": avg_or_none("no2"),
        "o3": avg_or_none("o3"),
        "so2": avg_or_none("so2"),
        "co": avg_or_none("co"),
    }

def save_structured_aq(db: Session, aq: dict, loc_id: int) -> AirQuality:
    db_aq = AirQuality(
        location_id = loc_id,
        **aq
    )
    db.add(db_aq)
    db.flush()
    db.refresh(db_aq)
    return db_aq


