from logger import logger
from services.db.models import LocationToTrack
from sqlalchemy.orm import Session
import random
from typing import Optional

from services.utils.is_on_water import is_on_water


def generate_loc(db: Session) -> Optional[LocationToTrack]:
    try:
        exists_locs = db.query(LocationToTrack.lat, LocationToTrack.lon).all()
        used_coordinates = {(loc.lat, loc.lon) for loc in exists_locs}

        max_attempts = 1000  # Защита от бесконечного цикла
        attempts = 0

        while attempts < max_attempts:
            attempts += 1

            random_lat = round(random.uniform(-90.00, 90.00), 2)
            random_lon = round(random.uniform(-180.00, 180.00), 2)
            current_pair = (random_lat, random_lon)

            if current_pair not in used_coordinates:
                used_coordinates.add(current_pair)

                # Проверяем, что точка на суше
                if not is_on_water(random_lat, random_lon):
                    new_location = LocationToTrack(lat=random_lat, lon=random_lon)

                    db.add(new_location)
                    db.commit()
                    db.refresh(new_location)
                    logger.info(f"Локация {new_location.id} на суше. Передаем дальше")
                    return new_location

        return None

    except Exception as e:
        db.rollback()
        raise e
