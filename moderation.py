"""  Модуль логики модерации. """

# moderation.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from models import Video  # Импортируем модель Video, если она в другом модуле

async def process_profile_after_payment(profile_id: int, session: AsyncSession):
    """Обрабатываем профиль после успешной оплаты"""
    # Отправляем профиль на модерацию
    moderation_result = await send_for_moderation(profile_id)

    if moderation_result:
        # Профиль прошел модерацию, обновляем его статус в БД
        await approve_profile(profile_id, session)
        print(f"Профиль {profile_id} успешно прошел модерацию.")
    else:
        # Профиль отклонен, удаляем или возвращаем в ожидание
        await reject_profile(profile_id, session)
        print(f"Профиль {profile_id} не прошел модерацию.")


async def approve_profile(profile_id: int, session: AsyncSession):
    """Обновляем профиль, если он прошел модерацию"""
    query = select(Video).filter(Video.id == profile_id)
    result = await session.execute(query)
    profile = result.scalars().first()

    if profile:
        profile.is_moderated = True
        await session.commit()
        print(f"Профиль {profile_id} одобрен и прошел модерацию.")


async def reject_profile(profile_id: int, session: AsyncSession):
    """Удаляем профиль, если он не прошел модерацию"""
    query = select(Video).filter(Video.id == profile_id)
    result = await session.execute(query)
    profile = result.scalars().first()

    if profile:
        await session.delete(profile)
        await session.commit()
        print(f"Профиль {profile_id} отклонен и удален.")


async def send_for_moderation(profile_id: int) -> bool:
    """Моковая функция для отправки профиля на модерацию"""
    # Логика отправки профиля на модерацию
    # Например, просто всегда возвращаем True (профиль прошел модерацию)
    print(f"Профиль {profile_id} отправлен на модерацию.")
    return True
