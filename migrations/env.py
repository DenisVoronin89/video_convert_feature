

from logging.config import fileConfig
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import pool
from alembic import context
from models import Base
from logging_config import get_logger

# Инициализация логгера
logger = get_logger()

# Этот объект Alembic Config предоставляет доступ к значениям, указанным в файле .ini
config = context.config

# Интерпретируем файл конфигурации для настройки логирования
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Указываем метаданные для автоматической генерации миграций
target_metadata = Base.metadata  # Используем метаданные из вашего основного объекта Base

async def run_migrations_offline() -> None:
    """Запуск миграций в 'offline' режиме."""
    url = config.get_main_option("sqlalchemy.url")  # Получаем URL базы данных из конфигурации
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    """Запуск миграций в 'online' режиме."""
    # Используем асинхронный движок для подключения
    connectable = create_async_engine(
        config.get_main_option("sqlalchemy.url"),  # Используем URL из конфигурации
        poolclass=pool.NullPool,
    )

    # Используем асинхронное подключение для выполнения миграций
    async with connectable.connect() as connection:
        context.configure(
            connection=connection, target_metadata=target_metadata
        )

        with context.begin_transaction():
            await context.run_migrations()
