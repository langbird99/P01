"""
06.app.py — Streamlit 뉴스 대시보드

실행: poetry run streamlit run 06.app.py
"""

import asyncio
import io
import json
import sqlite3
from pathlib import Path
from datetime import datetime

import edge_tts
import pandas as pd
import streamlit as st

DB_PATH = Path("k_enter_news.db")

st.set_page_config(
    page_title="K-ENT News Viewer",
    page_icon="📰",
    layout="wide",
)

CAT_COLOR = {
    "아이돌": "#7C3AED",
    "드라마": "#DB2777",
    "영화": "#EA580C",
    "글로벌": "#2563EB",
    "기타": "#6B7280",
}

SENT_LABEL = {
    "positive": "긍정",
    "neutral": "중립",
    "negative": "부정",
}


def _open():
    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row
    return con


@st.cache_data(show_spinner=False)
def tts_to_bytes(text: str) -> bytes:
    async def _gen():
        communicate = edge_tts.Communicate(
            text=text,
            voice="ko-KR-SunHiNeural",
            rate="-20%",
        )
        buf = io.BytesIO()
        async for chunk in communicate.stream():
            if chunk["type"] == "audio":
                buf.write(chunk["data"])
        return buf.getvalue()
    return asyncio.run(_gen())


def _j(v):
    if v is None:
        return []
    if isinstance(v, (list, dict)):
        return v
    try:
        return json.loads(v)
    except Exception:
        return []


def load_processed():
    con = _open()
    cur = con.cursor()
    cur.execute("""
        SELECT
            p.id,
            r.title,
            p.url,
            p.category,
            p.summary,
            p.keywords,
            p.artist_tags,
            p.sentiment,
            p.sentiment_score,
            p.source_name,
            p.tts_text,
            p.processed_at,
            p.thumbnail_url
        FROM processed_news p
        JOIN raw_news r ON r.id = p.raw_news_id
        ORDER BY p.id DESC
    """)
    rows = cur.fetchall()
    con.close()

    items = []
    for r in rows:
        items.append({
            "id": r["id"],
            "title": r["title"] or "",
            "url": r["url"] or "",
            "category": r["category"] or "기타",
            "summary": _j(r["summary"]),
            "keywords": _j(r["keywords"]),
            "artist_tags": _j(r["artist_tags"]),
            "sentiment": r["sentiment"] or "neutral",
            "sentiment_score": r["sentiment_score"] if r["sentiment_score"] is not None else 0.0,
            "source_name": r["source_name"] or "",
            "tts_text": r["tts_text"] or "",
            "processed_at": r["processed_at"] or "",
            "thumbnail_url": r["thumbnail_url"] or "",
        })
    return items


def load_past():
    con = _open()
    cur = con.cursor()
    cur.execute("""
        SELECT
            id,
            processed_news_id,
            artist_name,
            title,
            url,
            summary,
            relation_type,
            relevance_score,
            sentiment,
            category,
            source_name,
            published_at,
            thumbnail_url
        FROM past_news
        ORDER BY id DESC
    """)
    rows = cur.fetchall()
    con.close()

    items = []
    for r in rows:
        items.append({
            "id": r["id"],
            "processed_news_id": r["processed_news_id"],
            "artist_name": r["artist_name"] or "",
            "title": r["title"] or "",
            "url": r["url"] or "",
            "summary": r["summary"] or "",
            "relation_type": r["relation_type"] or "",
            "relevance_score": r["relevance_score"] if r["relevance_score"] is not None else 0.0,
            "sentiment": r["sentiment"] or "neutral",
            "category": r["category"] or "기타",
            "source_name": r["source_name"] or "",
            "published_at": r["published_at"] or "",
            "thumbnail_url": r["thumbnail_url"] or "",
        })
    return items


def render_badges(category, sentiment):
    cat_color = CAT_COLOR.get(category, "#6B7280")
    sent_text = SENT_LABEL.get(sentiment, sentiment)

    st.markdown(
        f"""
        <div style="display:flex; gap:8px; margin:6px 0 10px 0;">
            <span style="
                background:{cat_color}22;
                color:{cat_color};
                border:1px solid {cat_color}66;
                padding:3px 10px;
                border-radius:999px;
                font-size:12px;
                font-weight:600;
            ">{category}</span>
            <span style="
                background:#f3f4f6;
                color:#374151;
                border:1px solid #d1d5db;
                padding:3px 10px;
                border-radius:999px;
                font-size:12px;
                font-weight:600;
            ">{sent_text}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_processed_card(item):
    with st.container(border=True):
        cols = st.columns([1.2, 2.2])

        with cols[0]:
            if item["thumbnail_url"]:
                st.image(item["thumbnail_url"], use_container_width=True)
            else:
                st.caption("이미지 없음")

        with cols[1]:
            st.subheader(item["title"])
            render_badges(item["category"], item["sentiment"])

            st.write(f"**출처:** {item['source_name'] or '-'}")
            st.write(f"**가공시각:** {item['processed_at'] or '-'}")
            st.write(f"**감성점수:** {float(item['sentiment_score']):.2f}")

            if item["artist_tags"]:
                st.write("**아티스트:** " + ", ".join(map(str, item["artist_tags"])))
            if item["keywords"]:
                st.write("**키워드:** " + ", ".join(map(str, item["keywords"])))

            summary = item["summary"]
            if isinstance(summary, list) and summary:
                st.write("**요약**")
                for line in summary:
                    st.write(f"- {line}")
            elif isinstance(summary, str) and summary.strip():
                st.write("**요약**")
                st.write(summary)

            if item["tts_text"]:
                st.info(item["tts_text"])
                if st.button(
                    "🔊 음성 듣기",
                    key=f"tts_{item['id']}",
                ):
                    with st.spinner("음성 생성 중..."):
                        audio = tts_to_bytes(
                            item["tts_text"][:500]
                        )
                        st.audio(audio, format="audio/mp3", autoplay=True)

            if item["url"]:
                st.link_button("원문 보기", item["url"])


def render_past_card(item):
    with st.container(border=True):
        cols = st.columns([1.2, 2.2])

        with cols[0]:
            if item["thumbnail_url"]:
                st.image(item["thumbnail_url"], use_container_width=True)
            else:
                st.caption("이미지 없음")

        with cols[1]:
            st.subheader(item["title"])
            render_badges(item["category"], item["sentiment"])

            st.write(f"**아티스트:** {item['artist_name'] or '-'}")
            st.write(f"**관계유형:** {item['relation_type'] or '-'}")
            st.write(f"**관련도:** {float(item['relevance_score']):.2f}")
            st.write(f"**출처:** {item['source_name'] or '-'}")
            st.write(f"**발행일:** {item['published_at'] or '-'}")

            if item["summary"]:
                st.write("**요약**")
                st.write(item["summary"])

            if item["url"]:
                st.link_button("원문 보기", item["url"])


def main():
    st.title("📰 K-ENT 뉴스 대시보드")
    st.caption("k_enter_news.db 기반 최신 기사 / 과거 기사 / 이미지 확인 화면")

    if not DB_PATH.exists():
        st.error("현재 폴더에 k_enter_news.db 파일이 없습니다.")
        st.stop()

    tab1, tab2, tab3 = st.tabs(["최신 기사", "과거 기사", "간단 통계"])

    processed = load_processed()
    past = load_past()

    with st.sidebar:
        st.header("필터")
        keyword = st.text_input("검색어", "")
        category = st.selectbox("카테고리", ["전체", "아이돌", "드라마", "영화", "글로벌", "기타"])

    def match_processed(item):
        if category != "전체" and item["category"] != category:
            return False
        if keyword.strip():
            q = keyword.lower()
            pool = " ".join([
                item["title"],
                item["source_name"],
                " ".join(map(str, item["artist_tags"])),
                " ".join(map(str, item["keywords"])),
            ]).lower()
            return q in pool
        return True

    def match_past(item):
        if category != "전체" and item["category"] != category:
            return False
        if keyword.strip():
            q = keyword.lower()
            pool = " ".join([
                item["title"],
                item["artist_name"],
                item["source_name"],
                item["relation_type"],
            ]).lower()
            return q in pool
        return True

    filtered_processed = [x for x in processed if match_processed(x)]
    filtered_past = [x for x in past if match_past(x)]

    with tab1:
        st.subheader(f"최신 기사 {len(filtered_processed)}건")
        for item in filtered_processed:
            render_processed_card(item)

    with tab2:
        st.subheader(f"과거 기사 {len(filtered_past)}건")
        for item in filtered_past:
            render_past_card(item)

    with tab3:
        st.subheader("간단 통계")

        col1, col2, col3 = st.columns(3)
        col1.metric("최신 기사 수", len(processed))
        col2.metric("과거 기사 수", len(past))
        col3.metric(
            "이미지 연결 수",
            sum(1 for x in processed if x["thumbnail_url"]) + sum(1 for x in past if x["thumbnail_url"])
        )

        cat_count = {}
        for item in processed:
            cat = item["category"] or "기타"
            cat_count[cat] = cat_count.get(cat, 0) + 1

        if cat_count:
            df = pd.DataFrame(
                [{"category": k, "count": v} for k, v in cat_count.items()]
            )
            st.bar_chart(df.set_index("category"))

        st.write("### processed_news 미리보기")
        st.dataframe(pd.DataFrame(processed), use_container_width=True)

        st.write("### past_news 미리보기")
        st.dataframe(pd.DataFrame(past), use_container_width=True)


if __name__ == "__main__":
    main()
