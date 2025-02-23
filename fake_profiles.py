import random
from faker import Faker
from database import get_db_session  # Берём сессию из твоего модуля
from models import *
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
import asyncio

# Инициализация Faker
fake = Faker()

# Фиксированные хэштеги
FIXED_HASHTAGS = [
    "спорт", "автомобили", "искусство", "технологии", "программирование",
    "маркетинг", "девушки", "природа", "кулинария", "недвижимость"
]

# Генерация фейковых данных для пользователя и профиля с хештегами и избранным
async def generate_user_profile(session: AsyncSession, full_profile=True):
    # Генерация нового пользователя
    user = User(
        wallet_number=fake.unique.uuid4(),  # Генерация уникального кошелька
        is_profile_created=True
    )
    session.add(user)
    await session.commit()  # Асинхронная коммитация для добавления пользователя в БД

    # Генерация профиля пользователя с случайными данными
    profile_data = {
        "name": fake.name(),
        "website_or_social": fake.url() if full_profile and random.choice([True, False]) else None,
        "user_logo_url": f"https://fakeurl.com/{fake.uuid4()}.jpg",
        "video_url": f"https://fakevideo.com/{fake.uuid4()}.mp4" if full_profile and random.choice([True, False]) else None,
        "preview_url": f"https://fakevideo.com/{fake.uuid4()}.jpg" if full_profile and random.choice([True, False]) else None,
        "activity_and_hobbies": fake.text(max_nb_chars=200) if full_profile and random.choice([True, False]) else None,
        "is_moderated": random.choice([True, False]),
        "is_incognito": random.choice([True, False]),
        "is_in_mlm": random.choice([0, 1, 2]),
        "is_admin": random.choice([True, False]),
        "adress": [{"street": fake.street_address(), "city": fake.city()}] if full_profile and random.choice([True, False]) else None,
        "city": fake.city() if full_profile and random.choice([True, False]) else None,
        "coordinates": None,  # Для простоты оставим это поле пустым
        "followers_count": random.randint(0, 10000)
    }

    # Создаем профиль пользователя
    profile = UserProfiles(
        user_id=user.id,
        **profile_data
    )
    session.add(profile)
    await session.commit()  # Асинхронная коммитация для добавления профиля

    # Генерация случайных хэштегов
    hashtags = []
    if random.choice([True, False]):  # Для случайных профилей добавляем хештеги
        # Выбираем случайное количество хэштегов (от 1 до 5)
        selected_hashtags = random.sample(FIXED_HASHTAGS, k=random.randint(1, 5))

        for tag in selected_hashtags:
            # Проверяем, существует ли хэштег с таким значением
            result = await session.execute(select(Hashtag).where(Hashtag.tag == tag))
            existing_hashtag = result.scalars().first()

            if existing_hashtag:
                hashtag = existing_hashtag  # Используем существующий хэштег
            else:
                hashtag = Hashtag(tag=tag)  # Создаем новый хэштег
                session.add(hashtag)
                await session.commit()  # Сохраняем новый хэштег в базу данных

            hashtags.append(hashtag)

        # Привязываем хэштеги к профилю через ассоциативную таблицу
        for hashtag in hashtags:
            if profile.video_url:  # Проверяем, что video_url не None
                profile_hashtag = ProfileHashtag(
                    profile_id=profile.id,  # Связь через ID профиля
                    hashtag_id=hashtag.id  # Связь через ID хэштега
                )
                session.add(profile_hashtag)

        await session.commit()  # Асинхронная коммитация для привязки хэштегов к профилю

# Генерация избранных профилей для 50 пользователей
async def generate_favorites(session: AsyncSession):
    # Получаем всех пользователей
    result = await session.execute(select(User))
    users = result.scalars().all()

    # Получаем все профили
    result = await session.execute(select(UserProfiles))
    profiles = result.scalars().all()

    # Сохраняем избранные профили для 50 пользователей
    for _ in range(50):
        user = random.choice(users)  # Случайный пользователь
        profile = random.choice(profiles)  # Случайный профиль, который этот пользователь добавит в избранное

        favorite = Favorite(user_id=user.id, profile_id=profile.id)
        session.add(favorite)

    await session.commit()  # Асинхронная коммитация для добавления в избранное

# Генерация 100 пользователей (50 полных профилей и 50 частичных)
async def generate_profiles():
    async for session in get_db_session():  # Изменение на асинхронный контекст для сессии
        for _ in range(50):  # 50 профилей с полными данными
            await generate_user_profile(session, full_profile=True)

        for _ in range(50):  # 50 профилей с неполными данными
            await generate_user_profile(session, full_profile=False)

        await generate_favorites(session)  # Генерация избранных профилей

async def main():
    await generate_profiles()

# Для запуска асинхронной функции
if __name__ == "__main__":
    asyncio.run(main())
