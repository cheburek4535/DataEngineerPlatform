from services.weather.anomalies.anomalies import check_anomalies
from services.db.models import LocationToTrack
from services.db.db import get_session
from logger import logger
from sqlalchemy.orm import Session
import random
from typing import List, Optional


def add_location_to_track(db: Session, lat: float, lon: float, check_interval: int=0):
    try:
        loc = LocationToTrack(lat=lat, lon=lon, check_interval=check_interval)
        db.add(loc)
        db.commit()
        db.refresh(loc)
        print(f'Локация добавлена под номером {loc.id}')
        return loc
    except Exception as e:
        db.rollback()
        logger.error(f"Ошибка сохранения локации {e}")

def delete_location_from_track(db: Session, loc_id: int):
    try:
        loc = db.query(LocationToTrack).filter(LocationToTrack.id == loc_id).first()
        if loc:
            db.delete(loc)
            db.commit()
            return True
        else:
            return False
    except Exception as e:
        db.rollback()
        logger.error(f"Ошибка удаления локации {e}")


def generate_locs_for_track(db: Session, count: int) -> Optional[List[LocationToTrack]]:
    try:
        exists_locs = db.query(LocationToTrack).all()
        current_count = len(exists_locs)
        MAX_LIMIT = 25000

        if current_count >= MAX_LIMIT:
            logger.warning("Уже максимальное кол-во локаций в БД")
            return None

        allowed_to_generate = min(count, MAX_LIMIT - current_count)

        if allowed_to_generate <= 0:
            return None

        used_coordinates = {(loc.lat, loc.lon) for loc in exists_locs}
        new_locs = []

        while len(new_locs) < allowed_to_generate:
            random_lat = round(random.uniform(-90.00, 90.00), 2)
            random_lon = round(random.uniform(-180.00, 180.00), 2)
            current_pair = (random_lat, random_lon)

            if current_pair not in used_coordinates:
                used_coordinates.add(current_pair)
                new_locs.append(
                    LocationToTrack(lat=random_lat, lon=random_lon, check_interval=0)
                )

        db.bulk_save_objects(new_locs)
        db.commit()
        return new_locs

    except Exception as e:
        db.rollback()
        raise e

# add_location_to_track(db,59.57, 30.19, 180)
# add_location_to_track(db,55.45, 37.37, 180)
# add_location_to_track(db,67.52, 42.69, 180)
# add_location_to_track(db,14.88, 52.42, 180)
generate_locs_for_track(db=get_session(), count=15000)