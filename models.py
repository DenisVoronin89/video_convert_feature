"""  Модуль для описания таблиц в БД, реализация связей между хэштегами и видео """

from sqlalchemy import Column, Integer, String, ForeignKey, Table, Boolean, Float, DateTime, func
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

# Таблица профилей пользователей
class UserProfiles(Base):
    __tablename__ = 'user_profiles'

    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    wallet_number = Column(String(150), unique=True, nullable=False)
    name = Column(String(100), unique=False, nullable=False)
    user_logo_url = Column(String(255), nullable=False, unique=True)
    video_url = Column(String(255), nullable=False, unique=True)
    preview_url = Column(String(255), nullable=False, unique=True)
    activity_and_hobbies = Column(String(500), nullable=False)
    is_moderated = Column(Boolean, default=True, nullable=False)
    is_incognito = Column(Boolean, default=False, nullable=False)
    adress = Column(String(255), nullable=True)
    coordinates = Column(ARRAY(Float), nullable=True)

    # Связь с хэштегами
    hashtags = relationship('Hashtag', secondary='video_hashtags', back_populates='videos')


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
