""" Модуль для настройки и управления взаимодействием с БД  """

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from models import Base

from logging_config import get_logger

logger = get_logger()

DATABASE_URL = "postgresql+asyncpg://admin:admin1224@localhost/sst_video_app"

# Конфиги БД
engine = create_async_engine(DATABASE_URL, echo=True)  # echo=True(для логов)
SessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)

async def init_db():
    """Функция инициализации базы данных, создание таблиц"""
    try:
        logger.debug("Начало инициализации базы данных...")
        async with engine.begin() as conn:
            # Создание всех таблиц, связанных с моделями, если они еще не созданы
            await conn.run_sync(Base.metadata.create_all)
        logger.info("База данных успешно инициализирована.")
    except SQLAlchemyError as e:
        # Логируем ошибку с подробным описанием
        logger.error(f"Ошибка при инициализации базы данных: {repr(e)}.")
        raise
    except Exception as e:
        # Обработка других ошибок с их логированием
        logger.critical(f"Неожиданная ошибка при инициализации БД: {repr(e)}.")
        raise

