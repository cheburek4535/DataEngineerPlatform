from sqlalchemy.orm import Session
from sqlalchemy.sql import asc, desc
from services.db.models import LocationToTrack, Weather, AirQuality, Anomaly


def process_all_locations(db: Session):
    locations = db.query(LocationToTrack).order_by(asc(LocationToTrack.id)).all()
    result = {}
    for location in locations:
        weather_rows = db.query(Weather).filter(Weather.lat==location.lat, Weather.lon==location.lon).order_by(desc(Weather.timestamp)).limit(10).all()
        aq_rows = db.query(AirQuality).filter(AirQuality.location_id==location.id).order_by(desc(AirQuality.collected_at)).limit(10).all()
        weather_anomalies = db.query(Anomaly).filter(Anomaly.location_id==location.id).order_by(desc(Anomaly.found_at)).limit(15).all()

        result[location.id] = {'weather': weather_rows, 'air_quality': aq_rows, 'weather_anomalies': weather_anomalies}
    calculated = calculate_locs_go(result)
    saved = save_calculated(calculated)
    return saved
