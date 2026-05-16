from typing import Optional, Dict
from services.air_quality.extract import collect_raw_aq, extract_air_quality
from sqlalchemy.orm import Session

from services.db.models import RawAirQuality, AirQuality, LocationToTrack
from services.api_limiter import ApiLimiter, RateLimitExceeded

def transform_air_quality(raw_aq: RawAirQuality) -> Optional[Dict]:
    """
    Из сырого ответа OpenAQ извлекает структурированные данные.
    Группирует измерения всех станций вокруг одной точки в одну строку:
    берёт среднее PM2.5, среднее NO₂ и т.д. по всем станциям поблизости.
    """
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

def save_structured_aq(db: Session, raw_aq: dict, location: LocationToTrack) -> Optional[AirQuality]:
    db_aq = AirQuality(
        location = location,
        **raw_aq
    )
    db.add(db_aq)
    db.commit()
    db.refresh(db_aq)
    return db_aq

def collect_aq(db: Session, location: LocationToTrack, limiter: ApiLimiter) -> Optional[dict]:
    lat = location.lat
    lon = location.lon
    aq = extract_air_quality(lon=lon, lat=lat, limiter=limiter)
    if aq:
        raw_aq = collect_raw_aq(db, aq)
        transformed_aq = transform_air_quality(raw_aq)
        structured_aq = save_structured_aq(db, transformed_aq, location)
        return structured_aq
    return None

# print(transform_air_quality(extract_air_quality(136.0, 35.14)))