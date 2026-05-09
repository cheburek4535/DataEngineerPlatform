from db import Base, engine, RawWeather, Anomaly, Weather, LocationToTrack
try:
    Base.metadata.create_all(bind=engine)
    print("✅ Все таблицы созданы успешно!")
except Exception as e:
    print(f"❌ Ошибка инициализации БД: {e}")
