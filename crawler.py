"""
02.crawler.py — 최신 뉴스 + 과거 뉴스 크롤러 통합

역할:
  crawl_and_save()   : RSS + Tavily + Playwright로 최신 뉴스 수집 → raw_news 저장
  crawl_past_news()  : processed_news 아티스트 기반 과거 뉴스 수집 → past_news 저장
"""

import os
import re
import json
import asyncio
import time
import logging
import feedparser
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse, quote_plus
from sqlalchemy.exc import IntegrityError
from playwright.async_api import async_playwright
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

# ── 로깅 설정 ──
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("crawler")


# ═══════════════════════════════════════════════════
# 공통 설정
# ═══════════════════════════════════════════════════

def _env_int(name: str, default: int) -> int:
    v = (os.getenv(name) or "").strip()
    try:
        return int(v) if v else default
    except Exception:
        return default


RAW_CONTENT_MAX_CHARS = _env_int("RAW_CONTENT_MAX_CHARS", 8000)
PW_MIN_CONTENT_LEN = 500


# ═══════════════════════════════════════════════════
# Part 1: 최신 뉴스 크롤링 (RSS + Tavily + Playwright)
# ═══════════════════════════════════════════════════

LOOKBACK_DAYS = max(1, _env_int("CRAWL_LOOKBACK_DAYS", 10))
TAVILY_MAX_RESULTS = max(1, _env_int("TAVILY_MAX_RESULTS", 10))
MAX_PER_DOMAIN = max(1, _env_int("CRAWL_MAX_PER_DOMAIN", 10))
TAVILY_RETRY = 2

DEFAULT_QUERIES = {
    "K-POP_EN": (
        "K-pop comeback OR new album OR Billboard Hot 100 OR world tour OR "
        "music video OR idol group debut"
    ),
    "K-POP_KR": ("아이돌 컴백 OR 신곡 발매 OR 뮤직비디오 OR 월드투어 OR 빌보드 차트"),
    "K-DRAMA_EN": (
        "Korean drama premiere OR Netflix Korea series OR K-drama casting OR "
        "K-drama ratings OR new drama release"
    ),
    "K-DRAMA_KR": ("한국 드라마 방영 OR 넷플릭스 드라마 OR 드라마 캐스팅 OR 시청률"),
    "ACTOR_EN": (
        "Korean actor confirmed OR Korean actress casting OR award ceremony OR "
        "Korean celebrity interview"
    ),
    "ACTOR_KR": ("배우 출연 확정 OR 캐스팅 OR 시상식 수상 OR 배우 인터뷰"),
    "K-MOVIE_EN": (
        "Korean film box office OR Korean movie trailer OR film festival OR "
        "Korean movie release date"
    ),
    "K-MOVIE_KR": ("한국 영화 개봉 OR 박스오피스 OR 영화제 OR 예고편 공개"),
    "ENTERTAINMENT_EN": (
        "K-entertainment agency HYBE OR JYP OR YG OR SM contract OR "
        "K-pop controversy OR idol lawsuit"
    ),
    "ENTERTAINMENT_KR": (
        "하이브 OR JYP OR YG OR SM 기획사 OR 아이돌 논란 OR 계약 분쟁"
    ),
}

RSS_FEEDS = {
    "K-POP": [
        "https://www.soompi.com/feed",
        "https://www.allkpop.com/feed",
        "https://www.koreaboo.com/feed/",
        "https://feeds.feedburner.com/kpopstarz/rss",
        "https://www.billboard.com/feed/",
    ],
    "K-DRAMA": [
        "https://www.soompi.com/feed",
        "https://www.hancinema.net/rss.xml",
    ],
    "K-MOVIE": [
        "https://www.hancinema.net/rss.xml",
    ],
    "ENTERTAINMENT": [
        "https://www.allkpop.com/feed",
        "https://www.soompi.com/feed",
        "https://www.koreaboo.com/feed/",
    ],
}

SOURCE_QUOTA = {
    "고품질 글로벌": {
        "domains": ["soompi.com", "billboard.com", "nme.com"],
        "limit": 10,
    },
    "트렌드/속보": {
        "domains": ["allkpop.com", "koreaboo.com", "blip.kr", "korea.net"],
        "limit": 7,
    },
    "영화/배우": {
        "domains": ["cine21.com", "maxmovie.com", "hancinema.net"],
        "limit": 5,
    },
    "보조 데이터": {
        "domains": ["hanteonews.com", "kpopstarz.com"],
        "limit": 5,
    },
}

DOMAIN_SELECTORS = {
    "soompi.com": ["div.article-body", "div.entry-content"],
    "allkpop.com": ["div.article-content", "div.entry-content"],
    "billboard.com": ["div.article__body", "div.a-content"],
    "koreaherald.com": ["div.view_con_t", "div.article-text"],
    "koreaboo.com": ["div.entry-content", "article .post-body"],
    "blip.kr": ["div.article-body", "div.post-content"],
    "korea.net": ["div.article_view", "div.content_view"],
    "nme.com": ["div.article-body", "div.content--article"],
    "cine21.com": ["div.article_content", "div.news_content"],
    "maxmovie.com": ["div.article-body", "div.view-content"],
    "hanteonews.com": ["div.article-body", "div.post-content"],
    "hancinema.net": ["div.article-body", "div.entry-content"],
    "kpopstarz.com": ["div.article-content", "div.entry-content"],
}
FALLBACK_SELECTORS = [
    "article .entry-content",
    "article .post-body",
    "article",
    "div.article-body",
    "div.entry-content",
    "div#content",
    "div.story",
    "main",
]


# ── 유틸리티 ──

def parse_date(date_string: str) -> datetime | None:
    if not date_string:
        return None
    try:
        return parsedate_to_datetime(date_string)
    except Exception:
        try:
            return datetime.fromisoformat(date_string.replace("Z", "+00:00"))
        except Exception:
            return None


def is_within_lookback(published_at: datetime | None) -> bool:
    if not published_at:
        return False
    if published_at.tzinfo is None:
        published_at = published_at.replace(tzinfo=timezone.utc)
    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
    return published_at >= cutoff


_NOISE_PATTERNS = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"Advertisement",
        r"Sponsored",
        r"©.*",
        r"All rights reserved.*",
        r"Reporter\s*:\s*.*",
        r"Contact\s*:\s*.*",
        r"Email\s*:\s*.*",
        r"RELATED ARTICLES.*",
        r"Read more.*",
        r"Sign up for.*newsletter.*",
        r"Subscribe\s*(to|for)?\s*.*",
        r"Follow us on.*",
        r"Share this article.*",
        r"Tags\s*:.*",
        r"Photo\s*credit\s*:.*",
        r"Source\s*:.*",
    ]
]


def clean_content(text: str) -> str:
    if not text:
        return text
    for pat in _NOISE_PATTERNS:
        text = pat.sub("", text)
    lines = [line.strip() for line in text.split("\n") if len(line.strip()) >= 20]
    return re.sub(r"\n{2,}", "\n", "\n".join(lines)).strip()


# ── RSS 크롤링 ──

def fetch_news_from_rss() -> list[dict]:
    all_news = []
    seen_urls = set()

    for _category, feeds in RSS_FEEDS.items():
        for feed_url in feeds:
            try:
                feed = feedparser.parse(feed_url)
                if feed.bozo and not feed.entries:
                    log.warning(f"[RSS] 파싱 실패: {feed_url}")
                    continue

                log.info(f"[RSS] {feed_url} → {len(feed.entries)}건")

                for entry in feed.entries:
                    url = entry.get("link", "")
                    if not url or url in seen_urls:
                        continue
                    seen_urls.add(url)

                    date_str = entry.get("published") or entry.get("updated", "")
                    published_at = parse_date(date_str)

                    if published_at and not is_within_lookback(published_at):
                        continue
                    if published_at and published_at.tzinfo is None:
                        published_at = published_at.replace(tzinfo=timezone.utc)

                    content = entry.get("summary", "") or ""
                    content = re.sub(r"<[^>]+>", " ", content).strip()

                    all_news.append(
                        {
                            "title": entry.get("title", ""),
                            "content": content,
                            "url": url,
                            "published_at": published_at or datetime.now(timezone.utc),
                            "crawled_at": datetime.now(),
                            "is_processed": False,
                        }
                    )
            except Exception as e:
                log.error(f"[RSS 오류] {feed_url}: {e}")

    log.info(f"[RSS 합계] {len(all_news)}건 수집")
    return all_news


# ── Playwright 본문 보충 ──

async def _extract_content(page, url: str):
    host = urlparse(url).netloc.lower()

    selectors = []
    for domain, sels in DOMAIN_SELECTORS.items():
        if domain in host:
            selectors = sels
            break
    selectors += FALLBACK_SELECTORS

    for sel in selectors:
        try:
            el = await page.query_selector(sel)
            if el:
                text = await el.inner_text()
                if len(text) > PW_MIN_CONTENT_LEN:
                    return text
        except Exception:
            pass

    try:
        await page.evaluate(
            """
            for (const sel of ['header', 'footer', 'nav', '.sidebar', '.ad',
                               '.advertisement', '.social-share', '.comments',
                               '.related-posts', '#cookie-notice']) {
                document.querySelectorAll(sel).forEach(el => el.remove());
            }
        """
        )
    except Exception:
        pass
    return await page.inner_text("body")


async def _fetch_many(urls: list[str]) -> list[dict]:
    sem = asyncio.Semaphore(5)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)

        async def _worker(url, retries=1):
            async with sem:
                for attempt in range(retries + 1):
                    page = await browser.new_page()
                    try:
                        await page.goto(url, timeout=30000)
                        try:
                            await page.wait_for_load_state("networkidle", timeout=5000)
                        except Exception:
                            pass
                        content = clean_content(await _extract_content(page, url))
                        return {"url": url, "content": content}
                    except Exception as e:
                        if attempt < retries:
                            log.debug(f"[PW 재시도] {url}: {e}")
                            await asyncio.sleep(1)
                        else:
                            log.warning(f"[PW 실패] {url}: {e}")
                            return {"url": url}
                    finally:
                        await page.close()

        results = await asyncio.gather(*[_worker(u) for u in urls])
        await browser.close()
        return results


def enrich_with_playwright(news_list: list[dict]) -> list[dict]:
    targets = [
        n
        for n in news_list
        if n.get("url") and len(n.get("content") or "") < PW_MIN_CONTENT_LEN
    ]
    if not targets:
        return news_list

    log.info(f"[Playwright] {len(targets)}건 본문 보충 시작")
    results = asyncio.run(_fetch_many([n["url"] for n in targets]))
    url_map = {r["url"]: r for r in results if "content" in r}

    enriched = 0
    for n in targets:
        r = url_map.get(n["url"])
        if r and len(r["content"]) > len(n.get("content") or ""):
            n["content"] = r["content"]
            enriched += 1

    log.info(f"[Playwright] {enriched}/{len(targets)}건 보충 완료")
    return news_list


# ── Tavily API 크롤링 ──

def fetch_news_from_tavily(
    query: str,
    *,
    max_results: int | None = None,
    include_domains: list[str] | None = None,
) -> list[dict]:
    from tavily import TavilyClient

    client = TavilyClient(api_key=os.getenv("TAVILY_API_KEY"))
    n = max_results or TAVILY_MAX_RESULTS

    response = None
    for attempt in range(TAVILY_RETRY + 1):
        try:
            response = client.search(
                query=query,
                search_depth="advanced",
                topic="news",
                max_results=n,
                include_raw_content=True,
                include_domains=include_domains or [],
            )
            break
        except Exception as e:
            if attempt < TAVILY_RETRY:
                log.warning(f"[Tavily 재시도 {attempt+1}] '{query[:30]}...': {e}")
                time.sleep(2)
            else:
                log.error(f"[Tavily 최종 실패] '{query[:30]}...': {e}")
                return []

    if not response:
        return []

    results = []
    domain_counts: dict[str, int] = {}

    for item in response.get("results", []):
        published_at = parse_date(item.get("published_date"))

        if published_at:
            if not is_within_lookback(published_at):
                continue
            if published_at.tzinfo is None:
                published_at = published_at.replace(tzinfo=timezone.utc)
        else:
            published_at = datetime.now(timezone.utc)

        url = item.get("url", "") or ""
        host = urlparse(url).netloc.lower()
        if host:
            domain_counts.setdefault(host, 0)
            if domain_counts[host] >= MAX_PER_DOMAIN:
                continue
            domain_counts[host] += 1

        raw_content = item.get("raw_content") or item.get("content", "") or ""
        if RAW_CONTENT_MAX_CHARS > 0:
            raw_content = raw_content[:RAW_CONTENT_MAX_CHARS]

        results.append(
            {
                "title": item.get("title", ""),
                "content": raw_content,
                "url": url,
                "published_at": published_at,
                "crawled_at": datetime.now(),
                "is_processed": False,
            }
        )

    return results


def fetch_news_multi_query(*, per_query_limit=5, include_domains=None) -> list[dict]:
    all_results = []
    for label, q in DEFAULT_QUERIES.items():
        results = fetch_news_from_tavily(
            q, max_results=per_query_limit, include_domains=include_domains
        )
        log.info(f"  [Tavily] {label}: {len(results)}건")
        all_results.extend(results)
    return all_results


# ── 중복 제거 / 저장 ──

def dedup_news(news_list: list[dict]) -> list[dict]:
    seen = set()
    return [
        n
        for n in news_list
        if n.get("url") and n["url"] not in seen and not seen.add(n["url"])
    ]


def limit_per_domain(news_list: list[dict]) -> list[dict]:
    counts: dict[str, int] = {}
    filtered = []

    for n in news_list:
        host = urlparse(n["url"]).netloc
        limit = 1 if "billboard.com" in host else 5
        if counts.get(host, 0) >= limit:
            continue
        counts[host] = counts.get(host, 0) + 1
        filtered.append(n)

    return filtered


def save_raw_news(session, news_list: list[dict], category: str) -> int:
    from database import RawNews

    if not news_list:
        return 0

    urls = [n["url"] for n in news_list if n.get("url")]
    titles = [n["title"] for n in news_list if n.get("title")]

    existing_urls = set()
    existing_titles = set()

    if urls:
        rows = session.query(RawNews.url).filter(RawNews.url.in_(urls)).all()
        existing_urls = {r[0] for r in rows}
    if titles:
        rows = session.query(RawNews.title).filter(RawNews.title.in_(titles)).all()
        existing_titles = {r[0] for r in rows}

    saved_count = 0
    for news in news_list:
        try:
            if news["url"] in existing_urls or news["title"] in existing_titles:
                continue

            raw = RawNews(
                title=news["title"],
                content=news["content"],
                url=news["url"],
                published_at=news["published_at"],
                crawled_at=news["crawled_at"],
                is_processed=news["is_processed"],
                category=category,
            )
            session.add(raw)
            session.commit()

            existing_urls.add(news["url"])
            existing_titles.add(news["title"])
            saved_count += 1
        except IntegrityError:
            session.rollback()
        except Exception as e:
            session.rollback()
            log.error(f"[저장 실패] {news.get('title', 'Unknown')[:40]}: {e}")

    return saved_count


def crawl_and_save(
    session, queries: dict = None, max_results: int | None = None
) -> int:
    """최신 뉴스 크롤링 파이프라인: RSS → Tavily → Playwright 보충 → DB 저장"""
    if queries is None:
        queries = DEFAULT_QUERIES

    total_saved = 0

    log.info("=" * 50)
    log.info("[1단계] RSS 피드 수집 시작")
    rss_news = fetch_news_from_rss()
    rss_news = dedup_news(rss_news)
    rss_news = enrich_with_playwright(rss_news)

    rss_saved = save_raw_news(session, rss_news, category="RSS")
    total_saved += rss_saved
    log.info(f"[RSS 완료] {len(rss_news)}건 수집, {rss_saved}건 신규 저장")

    log.info("=" * 50)
    log.info("[2단계] Tavily API 수집 시작")
    for group, cfg in SOURCE_QUOTA.items():
        domains = cfg.get("domains", [])
        limit = cfg.get("limit", 0)
        if not domains or limit <= 0:
            continue

        log.info(f"[Tavily] '{group}' domains={len(domains)} limit={limit}")

        news_list = fetch_news_multi_query(
            per_query_limit=limit, include_domains=domains
        )
        news_list = dedup_news(news_list)
        news_list = limit_per_domain(news_list)
        news_list = enrich_with_playwright(news_list)

        saved = save_raw_news(session, news_list, category=group)
        total_saved += saved
        log.info(f"  → {len(news_list)}건 수집, {saved}건 신규 저장")

    log.info("=" * 50)
    log.info(f"[크롤링 완료] 총 {total_saved}건 raw_news에 저장")
    return total_saved


# ═══════════════════════════════════════════════════
# Part 2: 과거 뉴스 크롤링 (Bing News + Playwright + LLM)
# ═══════════════════════════════════════════════════

MAX_RESULTS_PER_ARTIST = 5
PAST_DATE_FROM = "2022-01-01"
PAST_DATE_TO = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

_openrouter_client = OpenAI(
    api_key=os.getenv("OPENROUTER_API_KEY"),
    base_url="https://openrouter.ai/api/v1",
)

PAST_LLM_MODEL = "google/gemini-2.5-flash"
PAST_LLM_DELAY = 2

PAST_SYSTEM_PROMPT = """너는 K-엔터테인먼트 뉴스 분석 전문가야.
주어진 과거 뉴스 기사를 분석해서 아래 JSON 형식으로만 응답해. 다른 텍스트는 절대 포함하지 마.

{
  "summary": "핵심 내용 5줄 이내 요약",
  "category": "아이돌 | 드라마 | 영화 | 배우 | 글로벌 중 택1",
  "keywords": ["키워드1", "키워드2", "키워드3"],
  "sentiment": "positive | negative | neutral 중 택1",
  "sentiment_score": 0.82,
  "source_name": "출처 사이트명",
  "artist_type": "가수 | 배우 | 그룹 중 택1",
  "artist_agency": "소속사명 (모르면 빈 문자열)",
  "relevance_score": 0.85,
  "relation_type": "선행사건 | 후속보도 | 유사사건 | 배경정보 중 택1"
}

규칙:
- summary: 핵심 내용 3줄 이내 요약
- category: "아이돌", "드라마", "영화", "글로벌" 중 택1
- keywords: 기사 핵심 키워드 3~5개
- sentiment: 기사 논조
- sentiment_score: 0.0(매우 부정) ~ 1.0(매우 긍정)
- source_name: URL에서 출처명 추출
- artist_type: 해당 아티스트가 가수인지 배우인지 그룹인지
- artist_agency: 해당 아티스트의 소속사 (알고 있다면)
- relevance_score: 현재 뉴스와의 관련도 (0.0~1.0)
- relation_type: 현재 뉴스와의 관계 유��"""


def extract_unique_artists(session) -> dict[str, list[int]]:
    from database import ProcessedNews

    artist_map: dict[str, list[int]] = {}

    for p in session.query(ProcessedNews).all():
        tags = p.artist_tags
        if not tags:
            continue
        if isinstance(tags, str):
            try:
                tags = json.loads(tags)
            except Exception:
                continue

        for artist in tags:
            artist = artist.strip()
            if not artist:
                continue
            normalized = artist.upper()
            artist_map.setdefault(normalized, [])
            if p.id not in artist_map[normalized]:
                artist_map[normalized].append(p.id)

    return artist_map


def get_already_crawled_artists(session) -> set[str]:
    from database import PastNews
    return {name.upper() for (name,) in session.query(PastNews.artist_name).distinct().all()}


def _bing_news_search(artist_name: str, max_results: int) -> list[dict]:
    from playwright.sync_api import sync_playwright

    start_year = int(PAST_DATE_FROM[:4])
    end_year = int(PAST_DATE_TO[:4])
    years = [str(y) for y in range(start_year, end_year + 1)]
    per_year = max(1, max_results // len(years))

    results = []
    seen_urls = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            for year in years:
                query = quote_plus(f"{artist_name} {year}")
                url = f"https://www.bing.com/news/search?q={query}&first=1"

                page = browser.new_page()
                try:
                    page.goto(url, timeout=30000)
                    page.wait_for_timeout(2000)

                    count = 0
                    for card in page.query_selector_all("div.news-card"):
                        if count >= per_year:
                            break
                        try:
                            t_el = card.query_selector("div.t_t")
                            title = t_el.inner_text().strip() if t_el else ""

                            href = ""
                            for a_el in card.query_selector_all("a"):
                                h = a_el.get_attribute("href") or ""
                                if h.startswith("http"):
                                    href = h
                                    break

                            snip_el = card.query_selector("div.snippet")
                            snippet = (snip_el.inner_text().strip() if snip_el else "").replace("\xa0", " ")

                            if not href or not title or href in seen_urls:
                                continue

                            seen_urls.add(href)
                            results.append({
                                "title": title,
                                "content": snippet,
                                "url": href,
                                "published_at": None,
                            })
                            count += 1
                        except Exception:
                            continue
                except Exception as e:
                    print(f"  [Bing News {year} 검색 실패] '{artist_name}': {e}")
                finally:
                    page.close()
        finally:
            browser.close()

    return results


def _fetch_article_content(urls: list[str]) -> dict[str, str]:
    from playwright.sync_api import sync_playwright

    url_content = {}
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        for url in urls:
            page = browser.new_page()
            try:
                page.goto(url, timeout=30000)
                page.wait_for_load_state("domcontentloaded")
                text = page.inner_text("body").replace("\xa0", " ")
                if RAW_CONTENT_MAX_CHARS > 0:
                    text = text[:RAW_CONTENT_MAX_CHARS]
                url_content[url] = text
            except Exception:
                pass
            finally:
                page.close()
        browser.close()

    return url_content


def fetch_past_news(artist_name: str, max_results: int = MAX_RESULTS_PER_ARTIST) -> list[dict]:
    articles = _bing_news_search(artist_name, max_results)
    if not articles:
        return []

    content_map = _fetch_article_content([a["url"] for a in articles if a.get("url")])
    for article in articles:
        fetched = content_map.get(article["url"], "")
        if len(fetched) > len(article.get("content") or ""):
            article["content"] = fetched

    return articles


def process_past_article(article: dict, artist_name: str, current_summary: str) -> dict:
    content = (article.get("content") or "")[:3000]

    user_message = f"""아래는 '{artist_name}'의 과거 뉴스야. 분석해줘.

[현재 뉴스 요약 (비교 대상)]
{current_summary}

[과거 뉴스]
제목: {article.get('title', '')}
URL: {article.get('url', '')}
본문:
{content}"""

    try:
        response = _openrouter_client.chat.completions.create(
            model=PAST_LLM_MODEL,
            temperature=0.3,
            timeout=60,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": PAST_SYSTEM_PROMPT},
                {"role": "user", "content": user_message},
            ],
        )
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        print(f"  [LLM 오류] {article.get('title', '')[:40]}: {e}")
        return {}


def save_past_news(session, article: dict, llm_result: dict,
                   artist_name: str, processed_news_id: int) -> bool:
    from database import PastNews

    try:
        session.add(PastNews(
            processed_news_id=processed_news_id,
            artist_name=artist_name,
            artist_type=llm_result.get("artist_type", ""),
            artist_agency=llm_result.get("artist_agency", ""),
            title=article["title"],
            content=article.get("content", ""),
            url=article["url"],
            published_at=article.get("published_at"),
            summary=llm_result.get("summary", ""),
            category=llm_result.get("category", ""),
            keywords=llm_result.get("keywords", []),
            sentiment=llm_result.get("sentiment", ""),
            sentiment_score=llm_result.get("sentiment_score", 0.0),
            relevance_score=llm_result.get("relevance_score", 0.0),
            relation_type=llm_result.get("relation_type", ""),
            crawled_at=datetime.now(),
            source_name=llm_result.get("source_name", ""),
        ))
        session.commit()
        return True
    except IntegrityError:
        session.rollback()
        return False
    except Exception as e:
        session.rollback()
        print(f"  [저장 실패] {article.get('title', '')[:40]}: {e}")
        return False


def crawl_past_news(session) -> int:
    """과거 뉴스 크롤링 파이프라인: 아티스트 추출 → Bing 검색 → LLM 가공 → DB 저장"""
    from database import ProcessedNews

    artist_map = extract_unique_artists(session)
    if not artist_map:
        print("[과거뉴스] processed_news에 아티스트가 없습니다.")
        return 0

    already_done = get_already_crawled_artists(session)
    new_artists = {k: v for k, v in artist_map.items() if k not in already_done}

    if not new_artists:
        print("[과거뉴스] 모든 아티스트의 과거 뉴스가 이미 수집되었습니다.")
        return 0

    print(f"[과거뉴스] 총 {len(artist_map)}명 중 신규 {len(new_artists)}명 처리 예정")
    total_saved = 0

    for artist_name, processed_ids in new_artists.items():
        print(f"\n[과거뉴스] '{artist_name}' 과거 뉴스 검색 중...")

        current = session.query(ProcessedNews).filter(ProcessedNews.id == processed_ids[0]).first()
        current_summary = (current.summary or "") if current else ""

        articles = fetch_past_news(artist_name)
        if not articles:
            print(f"  → '{artist_name}' 과거 뉴스 없음")
            continue

        print(f"  → {len(articles)}건 수집")

        saved_count = 0
        for article in articles:
            llm_result = process_past_article(article, artist_name, current_summary)
            time.sleep(PAST_LLM_DELAY)
            if not llm_result:
                continue

            if save_past_news(session, article, llm_result, artist_name, processed_ids[0]):
                saved_count += 1

        total_saved += saved_count
        print(f"  → {saved_count}건 저장 완료")

    print(f"\n[과거뉴스 완료] 총 {total_saved}건 past_news에 저장")
    return total_saved
