# test_proto.py
import sys

sys.path.insert(0, '/opt/airflow/src/generated')

import generated.lifescore.service_pb2 as service_pb2

# Проверяем, что доступно в модуле
print("Available in service_pb2:")
print(dir(service_pb2))

# Пробуем создать объекты разными способами
try:
    # Способ 1: через прямой атрибут
    loc = service_pb2.LocationData()
    print("✓ service_pb2.LocationData() works")
except AttributeError as e:
    print(f"✗ service_pb2.LocationData() failed: {e}")

try:
    # Способ 2: через DESCRIPTOR (всегда работает)
    from google.protobuf import symbol_database

    sym_db = symbol_database.Default()

    # Ищем по полному имени пакета
    LocationData = sym_db.GetSymbol('lifescore.LocationData')
    loc = LocationData()
    print("✓ sym_db.GetSymbol('lifescore.LocationData') works")

    # Проверяем другие типы
    BatchRequest = sym_db.GetSymbol('lifescore.BatchRequest')
    LifeScoreResponse = sym_db.GetSymbol('lifescore.LifeScoreResponse')
    AirQualityData = sym_db.GetSymbol('lifescore.AirQualityData')
    WeatherData = sym_db.GetSymbol('lifescore.WeatherData')
    AnomalyData = sym_db.GetSymbol('lifescore.AnomalyData')
    print("✓ All message types accessible via symbol_database")

except Exception as e:
    print(f"✗ symbol_database failed: {e}")

# Выведем все зарегистрированные символы
print("\nAll registered symbols in service_pb2:")
for name in dir(service_pb2):
    obj = getattr(service_pb2, name)
    if hasattr(obj, 'DESCRIPTOR'):
        print(f"  - {name}")