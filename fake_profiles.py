import random
from faker import Faker
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import joinedload
from geoalchemy2 import Geometry
from geoalchemy2.elements import WKTElement
from shapely.geometry import Point, MultiPoint
from models import *  # Модели из твоего файла
from database import get_db_session  # Асинхронная сессия из твоего модуля
import asyncio

fake = Faker()

FIXED_HASHTAGS = [
    "спорт", "автомобили", "искусство", "технологии", "программирование",
    "маркетинг", "девушки", "природа", "кулинария", "недвижимость"
]

async def generate_user_profile(session: AsyncSession, full_profile=True, with_coordinates=False):
    # Генерация нового пользователя
    user = User(
        wallet_number=fake.unique.uuid4(),
        is_profile_created=True
    )
    session.add(user)
    await session.commit()

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
        "coordinates": None,
        "followers_count": random.randint(0, 10000),
        "language": fake.language_code() if full_profile and random.choice([True, False]) else None
    }

    # Если нужно, добавляем координаты
    if with_coordinates:
        num_points = random.randint(2, 5)
        coordinates = [(random.uniform(-180, 180), random.uniform(-90, 90)) for _ in range(num_points)]
        points = [Point(coord[1], coord[0]) for coord in coordinates]
        multi_point = MultiPoint(points)
        profile_data["coordinates"] = str(multi_point)

    # Создаем профиль пользователя
    profile = UserProfiles(
        user_id=user.id,
        **profile_data
    )
    session.add(profile)
    await session.commit()

    # Генерация хештегов
    hashtags = []
    if random.choice([True, False]):
        selected_hashtags = random.sample(FIXED_HASHTAGS, k=random.randint(1, 5))
        for tag in selected_hashtags:
            result = await session.execute(
                select(Hashtag).options(joinedload(Hashtag.profiles)).where(Hashtag.tag == tag))
            existing_hashtag = result.unique().scalars().first()

            if existing_hashtag:
                hashtag = existing_hashtag
            else:
                hashtag = Hashtag(tag=tag)
                session.add(hashtag)

            hashtags.append(hashtag)

        await session.commit()

        # Привязываем хэштеги к профилю через ассоциативную таблицу
        for hashtag in hashtags:
            profile_hashtag = ProfileHashtag(
                profile_id=profile.id,
                hashtag_id=hashtag.id
            )
            session.add(profile_hashtag)

        await session.commit()

# Генерация избранных профилей для пользователей
async def generate_favorites(session: AsyncSession):
    result = await session.execute(select(UserProfiles))
    all_profiles = result.scalars().all()

    for profile in all_profiles:
        num_favorites = random.randint(1, 5)
        favorites = random.sample([p for p in all_profiles if p.id != profile.id], num_favorites)

        for favorite in favorites:
            profile_favorite = Favorite(
                user_id=profile.user_id,
                profile_id=favorite.id
            )
            session.add(profile_favorite)

        await session.commit()

# Генерация пользователей и профилей
async def generate_profiles():
    async for session in get_db_session():
        for _ in range(50):  # 50 профилей с полными данными
            await generate_user_profile(session, full_profile=True, with_coordinates=False)

        for _ in range(50):  # 50 профилей с неполными данными
            await generate_user_profile(session, full_profile=False, with_coordinates=False)

        for _ in range(50):  # 50 профилей с координатами
            await generate_user_profile(session, full_profile=True, with_coordinates=True)

        await generate_favorites(session)

async def main():
    await generate_profiles()

if __name__ == "__main__":
    asyncio.run(main())
