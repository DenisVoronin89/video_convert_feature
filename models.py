from sqlalchemy import Column, Integer, String, ForeignKey, Table
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


# Основная таблица приложения
class Video(Base):
    __tablename__ = 'videos'

    id = Column(Integer, primary_key=True)
    wallet_number = Column(String(50), unique=True, nullable=False)
    name = Column(String(100), unique=True, nullable=False)
    user_logo_url = Column(String(255), nullable=False, unique=True)
    video_url = Column(String(255), nullable=False, unique=True)
    preview_url = Column(String(255), nullable=False, unique=True)
    activity_and_hobbies = Column(String(500), nullable=False)
    is_moderated = Column(Boolean, default=False, nullable=False)  # Флаг модерации


    # Связь с хэштэгами
    hashtags = relationship('Hashtag', secondary='video_hashtags', back_populates='videos')


# Таблица хэштэгов
class Hashtag(Base):
    __tablename__ = 'hashtags'

    id = Column(Integer, primary_key=True)
    tag = Column(String(50), unique=True, nullable=False)

    # Связь с видео
    videos = relationship('Video', secondary='video_hashtags', back_populates='hashtags')


# Ассоциативная таблица для связи видео и хэштэгов
class VideoHashtag(Base):
    __tablename__ = 'video_hashtags'

    video_id = Column(Integer, ForeignKey('videos.id', ondelete='CASCADE'), primary_key=True)
    hashtag_id = Column(Integer, ForeignKey('hashtags.id', ondelete='CASCADE'), primary_key=True)


# Сессия для работы с БД
async def get_session(engine):
    return AsyncSession(bind=engine)


