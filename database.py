"""
01.database.py — DB 테이블 정의 및 연결 설정

테이블 구조:
  raw_news        : 크롤링 원본 저장 (1차)
  processed_news  : LLM 가공 결과 저장 (2차)
  past_news       : 아티스트 과거 관련 뉴스 저장 (3차)
"""

from datetime import datetime
from contextlib import contextmanager

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    Float,
    String,
    Text,
    Boolean,
    DateTime,
    ForeignKey,
    JSON,
    UniqueConstraint,
)
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

# ── DB 연결 설정 ──
DATABASE_URL = "sqlite:///k_enter_news.db"

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},
    echo=False,
)

SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()


# ── 1차 테이블: raw_news (크롤링 원���) ──
class RawNews(Base):
    __tablename__ = "raw_news"

    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(500), nullable=False)
    content = Column(Text, nullable=False)
    url = Column(String(1000), unique=True, nullable=False)
    published_at = Column(DateTime, nullable=True)
    crawled_at = Column(DateTime, default=datetime.now)
    is_processed = Column(Boolean, default=False)
    category = Column(String(50), nullable=True)

    processed = relationship("ProcessedNews", back_populates="raw", uselist=False)

    def __repr__(self):
        return f"<RawNews(id={self.id}, title='{self.title[:30]}...')>"


# ── 2차 테���블: processed_news (LLM 가공 결과) ──
class ProcessedNews(Base):
    __tablename__ = "processed_news"

    id = Column(Integer, primary_key=True, autoincrement=True)
    raw_news_id = Column(Integer, ForeignKey("raw_news.id"), nullable=False)
    category = Column(String(20), nullable=True)
    summary = Column(JSON, nullable=True)
    keywords = Column(JSON, nullable=True)
    sentiment = Column(String(10), nullable=True)
    sentiment_score = Column(Float, nullable=True)
    artist_tags = Column(JSON, nullable=True)
    tts_text = Column(Text, nullable=True)
    source_name = Column(String(100), nullable=True)
    url = Column(String(1000), nullable=True)
    processed_at = Column(DateTime, default=datetime.now)
    thumbnail_url = Column(String(1000), nullable=True)

    raw = relationship("RawNews", back_populates="processed")

    def __repr__(self):
        return f"<ProcessedNews(id={self.id}, category='{self.category}')>"


# ── 3차 테이블: past_news (아티스트 과거 관�� 뉴스) ──
class PastNews(Base):
    __tablename__ = "past_news"

    id = Column(Integer, primary_key=True, autoincrement=True)
    processed_news_id = Column(Integer, ForeignKey("processed_news.id"), nullable=False)
    artist_name = Column(String(200), nullable=False)
    artist_type = Column(String(20), nullable=True)
    artist_agency = Column(String(100), nullable=True)
    title = Column(String(500), nullable=False)
    content = Column(Text, nullable=True)
    url = Column(String(1000), nullable=False)
    published_at = Column(DateTime, nullable=True)
    summary = Column(Text, nullable=True)
    category = Column(String(20), nullable=True)
    keywords = Column(JSON, nullable=True)
    sentiment = Column(String(10), nullable=True)
    sentiment_score = Column(Float, nullable=True)
    relevance_score = Column(Float, nullable=True)
    relation_type = Column(String(50), nullable=True)
    crawled_at = Column(DateTime, default=datetime.now)
    source_name = Column(String(100), nullable=True)
    thumbnail_url = Column(String(1000), nullable=True)

    __table_args__ = (
        UniqueConstraint("url", "artist_name", name="uq_past_url_artist"),
    )

    processed = relationship("ProcessedNews", backref="past_news_list")

    def __repr__(self):
        return f"<PastNews(id={self.id}, artist='{self.artist_name}', title='{self.title[:30]}...')>"


# ── 테이블 생성 ���─
Base.metadata.create_all(engine)


# ── 세션 헬퍼 ──
@contextmanager
def get_session():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
