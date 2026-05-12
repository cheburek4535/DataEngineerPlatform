# from typing import Optional
# import openmeteo_requests
# import json
#
# import requests_cache
# from retry_requests import retry
#
#
# cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
# retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
# openmeteo = openmeteo_requests.Client(session = retry_session)
# from typing import List, Dict, Optional, Tuple
# import json
#
#
# # Функция теперь принимает список кортежей [(lat, lon), (lat, lon), ...]
# def get_weather_batch(coordinates: List[Tuple[float, float]]) -> List[Dict]:
#     if not coordinates:
#         return []
#
#     url = "https://api.open-meteo.com/v1/forecast"
#
#     # Разделяем список кортежей на два отдельных списка: широты и долготы
#     latitudes = [coord[0] for coord in coordinates]
#     longitudes = [coord[1] for coord in coordinates]
#
#     params = {
#         'latitude': latitudes,  # Передаем список
#         'longitude': longitudes,  # Передаем список
#         'current': 'temperature_2m,wind_speed_10m,relative_humidity_2m,pressure_msl'
#     }
#
#     batch_results = []
#     try:
#         # Библиотека возвращает список ответов для каждой локации по порядку
#         responses = openmeteo.weather_api(url, params=params)
#
#         for response in responses:
#             current = response.Current()
#             current_temperature = current.Variables(0).Value()
#             current_wind_speed = current.Variables(1).Value()
#             current_relative_humidity = current.Variables(2).Value()
#             current_pressure = current.Variables(3).Value()
#
#             latitude = response.Latitude()
#             longitude = response.Longitude()
#
#             data = {
#                 'temp': float(current_temperature),
#                 'wind_speed': float(current_wind_speed),
#                 'humidity': float(current_relative_humidity),
#                 'pressure': float(current_pressure),
#                 'latitude': float(latitude),
#                 'longitude': float(longitude),
#                 'timestamp': current.Time(),
#                 'raw_json': {
#                     'latitude': float(latitude),
#                     'longitude': float(longitude),
#                     'elevation': response.Elevation(),
#                     'timezone': response.Timezone(),
#                     'timezone_abbreviation': response.TimezoneAbbreviation(),
#                     'utc_offset_seconds': response.UtcOffsetSeconds(),
#                     'current': {
#                         'time': current.Time(),
#                         'interval': current.Interval(),
#                         'temperature_2m': float(current_temperature),
#                         'wind_speed_10m': float(current_wind_speed),
#                         'relative_humidity_2m': float(current_relative_humidity),
#                         'pressure_msl': float(current_pressure)
#                     }
#                 }
#             }
#             batch_results.append(data)
#
#         return batch_results
#
#     except Exception as e:
#         print(f"Ошибка пакетного запроса погоды: {e}")
#         return []
#
#
# import random
#
# # 1. Генерируем тестовый батч из 100 случайных локаций
# test_coordinates = []
# used_pairs = set()
#
# while len(test_coordinates) < 100:
#     # Округляем до 2 знаков, как в вашей исходной логике
#     lat = round(random.uniform(-90.00, 90.00), 2)
#     lon = round(random.uniform(-180.00, 180.00), 2)
#
#     if (lat, lon) not in used_pairs:
#         used_pairs.add((lat, lon))
#         test_coordinates.append((lat, lon))
#
# # 2. Выводим информацию для контроля перед запуском
# print(f"Сгенерировано тестовых локаций: {len(test_coordinates)}")
# print(f"Пример первых трех точек: {test_coordinates[:3]}\n")
#
# print("Отправка пакетного запроса в Open-Meteo...")
#
# # 3. Вызываем модифицированную функцию для 100 точек за раз
# weather_results = get_weather_batch(test_coordinates)
#
# # 4. Проверяем результат выполнения
# print(f"\nПолучено ответов от API: {len(weather_results)}")
#
# if weather_results:
#     print("\n--- Пример структуры данных первой локации ---")
#     first_item = weather_results[0]
#     print(f"Координаты: {first_item['latitude']}, {first_item['longitude']}")
#     print(f"Температура: {first_item['temp']} °C")
#     print(f"Скорость ветра: {first_item['wind_speed']} м/с")
#     print(f"Влажность: {first_item['humidity']}%")
#     print(f"Давление: {first_item['pressure']} гПа")
#     print(f"Ключи в raw_json: {list(first_item['raw_json'].keys())}")
# else:
#     print("Ошибка: API вернул пустой список ответов.")
