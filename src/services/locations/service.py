from services.anomalies.anomalies import check_anomalies
from services.db.models import Anomaly, LocationToTrack
from datetime import datetime, timezone
from logger import logger
import time
from sqlalchemy.orm import Session



def check_all_locations(db: Session):
    try:
        locations = db.query(LocationToTrack).all()
        for location in locations:
            try:

                    logger.info(f"Проверка локации {location.id} (координаты {location.lat}, {location.lon})")
                    check = check_anomalies(db, location.lat, location.lon, location)

                    if check:
                        logger.warning(
                            f"Обнаружена аномалия в локации {location.id} "
                            f"(координаты {location.lat}, {location.lon}). "
                            f"Дополнительные данные: {check.additional_data}"
                        )
                    else:
                        logger.info(f"Аномалий в локации {location.id} не обнаружено")

                    location.last_checked_at = datetime.now(timezone.utc)
                    db.commit()
            except Exception as e:
                logger.error(f"Ошибка при проверке локации {location.id}: {e}")
                db.rollback()  # Откатываем транзакцию в случае ошибки
                continue  # Продолжаем со следующей локацией

            time.sleep(1)
    except Exception as e:
        logger.error(f"Критическая ошибка при проверке локаций: {e}")
        db.rollback()
    finally:
        db.close()  # Закрываем сессию

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




# add_location_to_track(db,59.57, 30.19, 180)
# add_location_to_track(db,55.45, 37.37, 180)
# add_location_to_track(db,67.52, 42.69, 180)
# add_location_to_track(db,14.88, 52.42, 180)