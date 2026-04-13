"""
03.processor.py — LLM 가공 + 이미지 수집 통합

역할:
  process_and_save()  : raw_news → LLM 가공 → processed_news 저장
  fetch_all_images()  : processed_news + past_news 썸네일 이미지 수집
"""

import os
import json
import time
from datetime import datetime
from urllib.parse import quote

from playwright.sync_api import sync_playwright
from sqlalchemy import or_
from sqlalchemy.exc import SQLAlchemyError
from openai import OpenAI

from database import RawNews, ProcessedNews, PastNews, get_session


# ═══════════════════════════════════════════════════
# Part 1: LLM 가공 (raw_news → processed_news)
# ═══════════════════════════════════════════════════

client = OpenAI(
    api_key=os.getenv("OPENROUTER_API_KEY"),
    base_url="https://openrouter.ai/api/v1",
)

SYSTEM_PROMPT = """너는 K-엔터테인먼트 뉴스 분석 전문가야.
주어진 뉴스 기사를 분석해서 아래 JSON 형식으로만 응답해. 다른 텍스트는 절대 포함하지 마.

{
  "summary": "핵심 내용 3줄 이내 요약",
  "category": "아이돌 | 드라마 | 영화 | 글로벌 중 택1",
  "sentiment": "positive | negative | neutral 중 택1",
  "sentiment_score": 0.82,
  "keywords": ["키워드1", "키워드2", "키워드3"],
  "artist_tags": ["아티스트1", "아티스트2"],
  "source_name": "출처 사이트명",
  "tts_text": "라디오 앵커가 읽는 것처럼 자연스러운 브리핑 2~3문장"
}

규칙:
- summary: 핵심 내용을 5줄 이내로 요약
- category: "아이돌", "드라마", "영화", "글로벌" 중 가장 적합한 것 택1
- sentiment: 기사 논조가 긍정이면 "positive", 부정이면 "negative", 중립이면 "neutral"
- sentiment_score: 0.0(매우 부정) ~ 1.0(매우 긍정) 사이 소수점
- keywords: 기사의 핵심 키워드 3~5개
- artist_tags: 기사에 등장하는 아티스트/배우/인물 이름들 (없으면 빈 리스트)
- source_name: URL에서 출처 사이트명 추출 (예: "위버스", "빌보드")
- tts_text: 라디오 뉴스 앵커가 읽는 것처럼 자연스러운 브리핑 2~3문장"""

LLM_MODEL = "google/gemini-2.5-flash"
LLM_DELAY = 2


def process_single(raw: RawNews) -> dict:
    content = (raw.content or "")[:3000]

    user_message = f"""아래 뉴스를 분석해줘.

제목: {raw.title or ""}
URL: {raw.url or ""}
발행일: {raw.published_at or ""}
본문:
{content}"""

    response = client.chat.completions.create(
        model=LLM_MODEL,
        temperature=0.3,
        timeout=60,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ],
    )

    result = json.loads(response.choices[0].message.content)

    return {
        "raw_news_id": raw.id,
        "category": result.get("category", ""),
        "summary": result.get("summary", ""),
        "keywords": result.get("keywords", []),
        "sentiment": result.get("sentiment", ""),
        "sentiment_score": result.get("sentiment_score", 0.0),
        "artist_tags": result.get("artist_tags", []),
        "tts_text": result.get("tts_text", ""),
        "source_name": result.get("source_name", ""),
        "url": raw.url or "",
        "processed_at": datetime.now(),
    }


def process_and_save(session, batch_size: int = 50) -> int:
    """미처리된 raw_news를 LLM으로 가공 → processed_news에 저장"""
    unprocessed = (
        session.query(RawNews)
        .filter(RawNews.is_processed == False)
        .limit(batch_size)
        .all()
    )

    if not unprocessed:
        print("[가공] 처리할 뉴스가 없습니다.")
        return 0

    print(f"[가공] {len(unprocessed)}건 처리 시작...")
    processed_count = 0

    for raw in unprocessed:
        try:
            print(f"  → 가공 중: {raw.title[:40]}...")

            result = process_single(raw)
            time.sleep(LLM_DELAY)

            session.add(ProcessedNews(**result))
            raw.is_processed = True
            session.commit()
            processed_count += 1

        except Exception as e:
            print(f"  [오류] '{raw.title[:30]}...': {e}")
            session.rollback()

    print(f"[가공 완료] {processed_count}/{len(unprocessed)}건 처리됨")
    return processed_count


# ═══════════════════════════════════════════════════
# Part 2: 이미지 수집 (Bing 이미지 검색 → thumbnail_url 저장)
# ═══════════════════════════════════════════════════

def _loads_maybe(v):
    if v is None:
        return []
    if isinstance(v, list):
        return v
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return []
        try:
            obj = json.loads(s)
            return obj if isinstance(obj, list) else []
        except Exception:
            return []
    return []


def _clean_query(text: str) -> str:
    return " ".join((text or "").split()).strip()


def _norm_url(url: str | None) -> str:
    return (url or "").strip()


def _is_good_image_url(url: str) -> bool:
    if not url:
        return False
    low = url.lower()

    if not low.startswith("http"):
        return False

    bad_keywords = [
        "logo", "sprite", "icon", "tracker", "spacer", "blank",
        ".svg", "r.bing.com", "bing.com/rp/", "th.bing.com/th?id=ovp",
    ]
    if any(k in low for k in bad_keywords):
        return False

    return True


def get_all_used_thumbnail_urls(session) -> set[str]:
    used = set()

    processed_rows = (
        session.query(ProcessedNews.thumbnail_url)
        .filter(
            ProcessedNews.thumbnail_url.is_not(None),
            ProcessedNews.thumbnail_url != "",
        )
        .all()
    )
    for (u,) in processed_rows:
        if u:
            used.add(_norm_url(u))

    past_rows = (
        session.query(PastNews.thumbnail_url)
        .filter(
            PastNews.thumbnail_url.is_not(None),
            PastNews.thumbnail_url != "",
        )
        .all()
    )
    for (u,) in past_rows:
        if u:
            used.add(_norm_url(u))

    return used


def get_used_urls_for_artist(session, artist_name: str) -> set[str]:
    name = (artist_name or "").strip()
    if not name:
        return set()

    used = set()

    rows = (
        session.query(PastNews.thumbnail_url)
        .filter(
            PastNews.artist_name.ilike(name),
            PastNews.thumbnail_url.is_not(None),
            PastNews.thumbnail_url != "",
        )
        .all()
    )
    for (u,) in rows:
        if u:
            used.add(_norm_url(u))

    p_rows = (
        session.query(ProcessedNews.artist_tags, ProcessedNews.thumbnail_url)
        .filter(
            ProcessedNews.thumbnail_url.is_not(None),
            ProcessedNews.thumbnail_url != "",
        )
        .all()
    )
    for tags, thumb in p_rows:
        arr = _loads_maybe(tags)
        arr = [str(x).strip().lower() for x in arr]
        if name.lower() in arr and thumb:
            used.add(_norm_url(thumb))

    return used


def extract_bing_image_candidates(query: str, headless: bool = True, max_candidates: int = 20) -> list[str]:
    query = _clean_query(query)
    if not query:
        return []

    search_url = f"https://www.bing.com/images/search?q={quote(query)}&form=HDRSC3"

    with sync_playwright() as p:
        browser = None
        context = None

        try:
            browser = p.chromium.launch(
                headless=headless,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                ],
            )

            context = browser.new_context(
                locale="ko-KR",
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/135.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1440, "height": 900},
            )

            page = context.new_page()
            page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
            """)

            page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(3000)

            cards = page.locator("a.iusc")
            count = min(cards.count(), 60)

            results = []
            seen = set()

            for i in range(count):
                card = cards.nth(i)

                m_attr = card.get_attribute("m")
                if m_attr:
                    try:
                        m_json = json.loads(m_attr)
                        for key in ("murl", "turl"):
                            url = _norm_url(m_json.get(key))
                            if _is_good_image_url(url) and url not in seen:
                                seen.add(url)
                                results.append(url)
                    except Exception:
                        pass

                img = card.locator("img").first
                if img.count() > 0:
                    for attr_name in ("src", "data-src", "data-thumb", "data-imageurl"):
                        url = _norm_url(img.get_attribute(attr_name))
                        if _is_good_image_url(url) and url not in seen:
                            seen.add(url)
                            results.append(url)

                if len(results) >= max_candidates:
                    break

            return results

        except Exception as e:
            print(f"[bing_candidates] 예외 발생: {e}")
            return []

        finally:
            try:
                if context:
                    context.close()
            except Exception:
                pass
            try:
                if browser:
                    browser.close()
            except Exception:
                pass


def pick_non_duplicate_bing_image(
    session,
    query: str,
    *,
    artist_name: str | None = None,
    headless: bool = True,
) -> str | None:
    all_used = get_all_used_thumbnail_urls(session)
    artist_used = get_used_urls_for_artist(session, artist_name or "") if artist_name else set()

    candidates = extract_bing_image_candidates(query, headless=headless, max_candidates=20)
    if not candidates:
        return None

    for url in candidates:
        if url not in all_used and url not in artist_used:
            return url

    for url in candidates:
        if url not in artist_used:
            return url

    return candidates[0]


def build_query_for_processed(article) -> tuple[str, str]:
    artists = _loads_maybe(getattr(article, "artist_tags", None))
    if artists:
        artist = str(artists[0]).strip()
        return f"{artist} official photo", artist

    keywords = _loads_maybe(getattr(article, "keywords", None))
    if keywords:
        return f"{keywords[0]} kpop official photo", ""

    source_name = getattr(article, "source_name", None) or ""
    if source_name.strip():
        return f"{source_name} k-entertainment", ""

    return "kpop idol official photo", ""


def build_query_for_past(article) -> tuple[str, str]:
    artist_name = (getattr(article, "artist_name", None) or "").strip()
    if artist_name:
        return f"{artist_name} official photo", artist_name

    title = (getattr(article, "title", None) or "").strip()
    if title:
        return title[:80], ""

    return "kpop artist official photo", ""


def fetch_images_for_processed(session, sleep_sec: float = 1.5, headless: bool = True):
    articles = (
        session.query(ProcessedNews)
        .filter(
            or_(ProcessedNews.thumbnail_url.is_(None), ProcessedNews.thumbnail_url == "")
        )
        .all()
    )

    print(f"\n[processed_news] 총 {len(articles)}건 이미지 처리 시작")

    success = 0
    failed = 0

    for article in articles:
        query, artist_name = build_query_for_processed(article)

        print(f"\n[processed_news] 처리 중 ID={article.id}")
        print(f"  - 검색어: {query}")

        image_url = pick_non_duplicate_bing_image(
            session,
            query,
            artist_name=artist_name,
            headless=headless,
        )

        if image_url:
            try:
                article.thumbnail_url = image_url
                session.commit()
                success += 1
                print(f"  - 성공: {image_url}")
            except SQLAlchemyError as e:
                session.rollback()
                failed += 1
                print(f"  - DB 저장 실패: {e}")
        else:
            failed += 1
            print("  - 실패: 적합한 이미지 없음")

        time.sleep(sleep_sec)

    print(f"\n[processed_news] 완료: {success}/{len(articles)}건 성공")


def fetch_images_for_past(session, sleep_sec: float = 1.5, headless: bool = True):
    articles = (
        session.query(PastNews)
        .filter(
            or_(PastNews.thumbnail_url.is_(None), PastNews.thumbnail_url == "")
        )
        .all()
    )

    print(f"\n[past_news] 총 {len(articles)}건 이미지 처리 시작")

    success = 0
    failed = 0

    for article in articles:
        query, artist_name = build_query_for_past(article)

        print(f"\n[past_news] 처리 중 ID={article.id}")
        print(f"  - 검색어: {query}")

        image_url = pick_non_duplicate_bing_image(
            session,
            query,
            artist_name=artist_name,
            headless=headless,
        )

        if image_url:
            try:
                article.thumbnail_url = image_url
                session.commit()
                success += 1
                print(f"  - 성공: {image_url}")
            except SQLAlchemyError as e:
                session.rollback()
                failed += 1
                print(f"  - DB 저장 실패: {e}")
        else:
            failed += 1
            print("  - 실패: 적합한 이미지 없음")

        time.sleep(sleep_sec)

    print(f"\n[past_news] 완료: {success}/{len(articles)}건 성공")


def fetch_all_images(headless: bool = True):
    with get_session() as session:
        fetch_images_for_processed(session, headless=headless)
        fetch_images_for_past(session, headless=headless)


if __name__ == "__main__":
    fetch_all_images(headless=True)
