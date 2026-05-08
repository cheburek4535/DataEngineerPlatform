
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base
from dotenv import load_dotenv
from logger import logger
from config import settings

load_dotenv()


#SQLALCHEMY_DATABASE_URL = "sqlite:///./sql_app.db"
SQLALCHEMY_DATABASE_URL = f"postgresql+psycopg://{settings.db_user}:{settings.db_password}@{settings.db_host}:{settings.db_port}/{settings.db_name}"

engine = create_engine(SQLALCHEMY_DATABASE_URL, echo=False, pool_pre_ping=True)

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)

print("DATABASE_URL:", engine.url)




Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_session():
    """Возвращает сессию для использования в скриптах"""
    return SessionLocal()

from collector.models import *
def init():
    try:
        logger.info("Удаляем старые таблицы")
        Base.metadata.drop_all(bind=engine)
        logger.info("Создаем таблицы в PostgreSQL...")
        print("Таблицы в моделях:", list(Base.metadata.tables.keys()))
        Base.metadata.create_all(bind=engine)
        logger.info("✅ Все таблицы созданы успешно!")
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации БД: {e}")

if __name__ == "__main__":
    #migrate_all_data()
    #force_create()
    init()
