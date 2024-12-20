""" Модуль для настройки и управления взаимодействием с БД  """

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker
from models import videos

DATABASE_URL = "postgresql+asyncpg://admin:admin1224@localhost/sst_video_app"

# Конфиги БД
engine = create_async_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

async def init_db():
    """Функция инициализации базы данных"""
    async with engine.begin() as conn:
        await conn.run_sync(videos.metadata.create_all)
