"""  Модуль для описания таблиц в БД, реализация связей между хэштегами и видео """

from sqlalchemy import Column, Integer, String, ForeignKey, Index, Boolean, DateTime, func
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import Geometry

Base = declarative_base()


# Таблица пользователей
class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    wallet_number = Column(String(150), unique=True, nullable=False)
    is_profile_created = Column(Boolean, default=False, nullable=False) # Флаг меняем при создании профиля

    # Индекс для ускорения поиска по кошельку
    __table_args__ = (
        Index('ix_user_wallet_number', 'wallet_number'),
    )

    # Связь с профилем
    profile = relationship('UserProfiles', back_populates='user', uselist=False)

    # Связь с избранным
    favorites = relationship('Favorite', back_populates='user', cascade="all, delete-orphan")


# Таблица профилей пользователей
class UserProfiles(Base):
    __tablename__ = 'user_profiles'

    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    name = Column(String(100), unique=False, nullable=False)
    user_logo_url = Column(String(255), nullable=False, unique=True)
    video_url = Column(String(255), nullable=True, unique=True)
    preview_url = Column(String(255), nullable=True, unique=True)
    activity_and_hobbies = Column(String(500), nullable=True)
    is_moderated = Column(Boolean, default=True, nullable=False)
    is_incognito = Column(Boolean, default=False, nullable=False)
    is_in_mlm = Column(Integer, nullable=True, default=0)
    is_admin = Column(Boolean, nullable=True, default=False)
    adress = Column(String(255), nullable=True)
    city = Column(String(55), nullable=True)
    coordinates = Column(Geometry('POINT', srid=4326), nullable=True)
    followers_count = Column(Integer, default=0, nullable=True)  # Счётчик подписчиков летит из редиски

    # Связь с хэштегами
    hashtags = relationship('Hashtag', secondary='video_hashtags', back_populates='videos')

    # Связь с таблицей пользователей
    user_id = Column(Integer, ForeignKey('users.id', ondelete='CASCADE'), nullable=False)

    # Обратная связь
    user = relationship('User', back_populates='profile')

    # Связь с избранным юзеров
    favorited_by = relationship('Favorite', back_populates='profile', cascade="all, delete-orphan")


# Таблица избранного
class Favorite(Base):
    __tablename__ = 'favorites'

    user_id = Column(Integer, ForeignKey('users.id', ondelete='CASCADE'), primary_key=True)
    profile_id = Column(Integer, ForeignKey('user_profiles.id', ondelete='CASCADE'), primary_key=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    # Связь с пользователем
    user = relationship('User', back_populates='favorites')

    # Связь с профилем
    profile = relationship('UserProfiles', back_populates='favorited_by')


# Таблица хэштегов
class Hashtag(Base):
    __tablename__ = 'hashtags'

    id = Column(Integer, primary_key=True)
    tag = Column(String(255), unique=True, nullable=False)

    # Связь с видео через ассоциативную таблицу
    videos = relationship('UserProfiles', secondary='video_hashtags', back_populates='hashtags')


# Ассоциативная таблица для связи хэштегов с видео URL
class VideoHashtag(Base):
    __tablename__ = 'video_hashtags'

    # Составной первичный ключ: связь между видео и хэштегами
    video_url = Column(String(255), ForeignKey('user_profiles.video_url', ondelete='CASCADE'), primary_key=True)
    hashtag_id = Column(Integer, ForeignKey('hashtags.id', ondelete='CASCADE'), primary_key=True)

    # Индексы
    __table_args__ = (
        Index('ix_video_hashtags_video_url', 'video_url'),
        Index('ix_video_hashtags_hashtag_id', 'hashtag_id'),
    )
