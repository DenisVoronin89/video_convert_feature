import asyncio
from logging.config import fileConfig

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio import AsyncConnection
from alembic import context

# Импортируем Base и модели
from models import Base  # Замени на правильный импорт

# Настройка логов Alembic
config = context.config
fileConfig(config.config_file_name)

# Получаем URL базы данных из конфигурации Alembic
db_url = config.get_main_option("sqlalchemy.url")

# Используем Base.metadata для target_metadata
target_metadata = Base.metadata

def include_object(object, name, type_, reflected, compare_to):
    """
    Функция для исключения системных таблиц.
    """
    # Игнорируем таблицы, которые не относятся к твоим моделям
    if type_ == "table" and name in {"spatial_ref_sys", "layer", "topology"}:
        return False
    return True

def run_migrations_offline():
    """
    Запуск миграций в оффлайн-режиме.
    """
    context.configure(
        url=db_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_object=include_object,  # Исключаем системные таблицы
    )

    with context.begin_transaction():
        context.run_migrations()

async def run_migrations_online():
    """
    Запуск миграций в онлайн-режиме с использованием асинхронного движка.
    """
    connectable = create_async_engine(db_url)

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

def do_run_migrations(connection: AsyncConnection):
    """
    Запуск миграций внутри асинхронного контекста.
    """
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
        include_object=include_object,  # Исключаем системные таблицы
    )

    with context.begin_transaction():
        context.run_migrations()

if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())