"""
04.vectorstore.py — 임베딩 + ChromaDB 저장

역할:
  스크립트 실행 시: k_enter_news.db → 임베딩 → ChromaDB 저장 (2개 collection)
  get_stores()       : 저장된 벡터스토어 불러오기
  enrich_recent_with_past() : 최신 ��스로 과거 뉴스 유사도 검색
"""

import sqlite3
import json
from langchain_core.documents import Document
from langchain_chroma import Chroma
from langchain_huggingface import HuggingFaceEmbeddings

EMBED_MODEL = "intfloat/multilingual-e5-base"
CHROMA_DIR = "./chroma_db"


# ═══════════════════════════════════════════════════
# 유사도 검색 헬퍼 (다른 모듈에서 import)
# ═══════════════════════════════════════════════════

def get_stores():
    """저장된 ChromaDB 벡터스토어 불러오기"""
    emb = HuggingFaceEmbeddings(model_name=EMBED_MODEL)
    recent = Chroma(
        collection_name="recent_news",
        embedding_function=emb,
        persist_directory=CHROMA_DIR,
    )
    past = Chroma(
        collection_name="past_news",
        embedding_function=emb,
        persist_directory=CHROMA_DIR,
    )
    return recent, past


def enrich_recent_with_past(recent_doc: Document, past_store: Chroma, k: int = 3):
    """최신 뉴스 1건 → past_news에서 유사한 과거 뉴스 k건 검색"""
    return past_store.similarity_search_with_score(
        recent_doc.page_content, k=k
    )


# ═══════════════════════════════════════════════════
# 임베딩 + 저장 (스크립트 실행 시)
# ═══════════════════════════════════════════════════

def build_and_save():
    conn = sqlite3.connect("k_enter_news.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    embedding = HuggingFaceEmbeddings(model_name=EMBED_MODEL)

    # ── processed_news → recent_news collection ──
    cursor.execute(
        """
    SELECT id, raw_news_id, category, summary, keywords, artist_tags,
           sentiment, sentiment_score, source_name, url, processed_at
    FROM processed_news
    """
    )

    recent_docs = []
    for row in cursor.fetchall():
        keywords = json.loads(row["keywords"]) if row["keywords"] else []
        artists = json.loads(row["artist_tags"]) if row["artist_tags"] else []

        content = f"""{row["summary"]}

아티스트: {', '.join(artists)}
키워드: {', '.join(keywords)}
카테고리: {row["category"]}"""

        doc = Document(
            page_content=content,
            metadata={
                "id": row["id"],
                "raw_news_id": row["raw_news_id"],
                "category": row["category"],
                "sentiment": row["sentiment"],
                "sentiment_score": row["sentiment_score"],
                "source": row["source_name"],
                "url": row["url"],
                "processed_at": row["processed_at"],
            },
        )
        recent_docs.append(doc)

    print(f"recent_news: {len(recent_docs)}건")

    # ── past_news → past_news collection ──
    cursor.execute(
        """
    SELECT id, processed_news_id, artist_name, artist_type, artist_agency,
           title, content, url, published_at, summary, category, keywords,
           sentiment, sentiment_score, relevance_score, relation_type,
           crawled_at, source_name
    FROM past_news
    """
    )

    past_docs = []
    for row in cursor.fetchall():
        keywords = json.loads(row["keywords"]) if row["keywords"] else []

        content = f"""{row["summary"] or row["content"] or row["title"]}

아티스트: {row["artist_name"]}
키워드: {', '.join(keywords)}
카테고리: {row["category"]}
관계유형: {row["relation_type"] or "N/A"}"""

        doc = Document(
            page_content=content,
            metadata={
                "id": row["id"],
                "processed_news_id": row["processed_news_id"],
                "artist_name": row["artist_name"],
                "artist_type": row["artist_type"],
                "artist_agency": row["artist_agency"],
                "category": row["category"],
                "sentiment": row["sentiment"],
                "sentiment_score": row["sentiment_score"],
                "relevance_score": row["relevance_score"],
                "relation_type": row["relation_type"],
                "source": row["source_name"],
                "url": row["url"],
                "published_at": row["published_at"],
            },
        )
        past_docs.append(doc)

    print(f"past_news: {len(past_docs)}건")
    conn.close()

    # ── Chroma 저장 ──
    print("임베딩 시작...")

    recent_store = Chroma.from_documents(
        documents=recent_docs,
        embedding=embedding,
        collection_name="recent_news",
        persist_directory=CHROMA_DIR,
    )
    print(f"recent_news 저장 완료: {recent_store._collection.count()}건")

    past_store = Chroma.from_documents(
        documents=past_docs,
        embedding=embedding,
        collection_name="past_news",
        persist_directory=CHROMA_DIR,
    )
    print(f"past_news 저장 완료: {past_store._collection.count()}건")


if __name__ == "__main__":
    build_and_save()
