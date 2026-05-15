from services.weather.anomalies.anomalies import check_anomalies
from services.db.models import LocationToTrack
from services.db.db import get_session
from datetime import datetime, timezone
from logger import logger
import time
from sqlalchemy.orm import Session
from sqlalchemy.sql import desc, asc
import random
from typing import List, Optional


def check_all_locations(db: Session):
    try:
        locations = db.query(LocationToTrack).order_by(asc(LocationToTrack.id)).all()
        # Счетчик для контроля лимита в минуту
        location_counter = 0
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
                    db.expire_all()

                    # Увеличиваем счетчик после успешной обработки локации
                    location_counter += 1
                    if location_counter >= 5000:
                        logger.warning("Достигнут лимит 5000 локаций")
                        break

                    # ХИТРОСТЬ: Если обработали 500 локаций и это ЕЩЕ НЕ конец списка
                    if location_counter % 500 == 0 and location_counter < len(locations):
                        logger.info(
                            f"Обработано {location_counter} локаций. Спим 60 секунд для сброса минутного лимита API...")
                        time.sleep(60)
                    else:
                        # Микро-пауза в 0.02 сек между обычными запросами.
                        # Она нужна, чтобы база данных успевала отдыхать и не было микро-спама к API.
                        time.sleep(0.02)
            except Exception as e:
                logger.error(f"Ошибка при проверке локации {location.id}: {e}")
                db.rollback()
                continue

    except Exception as e:
        logger.error(f"Критическая ошибка при проверке локаций: {e}")
        db.rollback()
    finally:
        db.close()  # Закрываем сессию


# def check_all_locations_batch(db: Session):
#     try:
#         # 1. Получаем все локации из БД
#         locations = db.query(LocationToTrack).all()
#         if not locations:
#             logger.info("Нет локаций для проверки")
#             return
#
#         BATCH_SIZE = 100
#
#         # 2. Нарезаем список локаций на чанки по 100 элементов
#         for i in range(0, len(locations), BATCH_SIZE):
#             chunk_locations = locations[i:i + BATCH_SIZE]
#             chunk_coordinates = [(loc.lat, loc.lon) for loc in chunk_locations]
#
#             logger.info(f"Пакетная обработка {len(chunk_coordinates)} локаций...")
#
#             try:
#                 # 3. Скачиваем и сохраняем погоду для всего батча за ОДИН запрос к API
#                 # Функция возвращает список созданных и сохраненных объектов Weather
#                 created_weather_objects = collect_weather_batch(db, chunk_coordinates)
#
#                 # Создаем словарь для быстрого поиска погоды по координатам: {(lat, lon): weather_object}
#                 weather_map = {(w.lat, w.lon): w for w in created_weather_objects}
#
#                 # 4. Проверяем аномалии локально для этой пачки объектов
#                 for location in chunk_locations:
#                     try:
#                         # Ищем погоду для конкретной локации в нашей сохраненной пачке
#                         # Используем round, чтобы избежать проблем со сравнением float
#                         loc_key = (location.lat, location.lon)
#                         weather_data = weather_map.get(loc_key)
#
#                         if not weather_data:
#                             logger.error(f"Данные погоды для локации {location.id} ({loc_key}) не найдены в ответе API")
#                             continue
#
#                         # Передаем уже готовую погоду в функцию аномалий
#                         check = check_anomalies_for_batch(db, location, weather_data)
#
#                         if check:
#                             logger.warning(
#                                 f"Обнаружена аномалия в локации {location.id} "
#                                 f"(координаты {location.lat}, {location.lon}). "
#                                 f"Дополнительные данные: {check.additional_data}"
#                             )
#                         else:
#                             logger.info(f"Аномалий в локации {location.id} не обнаружено")
#
#                         # Обновляем время проверки локации
#                         location.last_checked_at = datetime.now(timezone.utc)
#
#                     except Exception as loc_e:
#                         logger.error(f"Ошибка проверки аномалий для локации {location.id}: {loc_e}")
#
#                 # Фиксируем транзакцию для всего батча (сохраняем аномалии и last_checked_at)
#                 db.commit()
#
#             except Exception as batch_e:
#                 logger.error(f"Ошибка при обработке батча локаций: {batch_e}")
#                 db.rollback()
#                 continue  # Переходим к следующему батчу
#
#     except Exception as e:
#         logger.error(f"Критическая ошибка при проверке локаций: {e}")
#         db.rollback()
#     finally:
#         db.close()


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
        MAX_LIMIT = 4999

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
generate_locs_for_track(db=get_session(), count=1699)