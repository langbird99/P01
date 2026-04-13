# 뉴스 크롤링 파이프라인 가이드

---

## 1. 데이터 흐름

```
[Tavily 크롤링] → raw_news 테이블 (1차 저장, 대략적 스키마)
                        ↓
              [가공 모듈 (LLM)] → processed_news 테이블 (2차 저장, 디테일 스키마)
```

### 왜 2단계로 나누는가?

| | 1단계로 바로 가공 | 2단계 분리 (채택) |
|---|---|---|
| LLM 에러 시 | 원본 데이터 유실 | 원본은 안전하게 보존 |
| 스키마 변경 시 | Tavily 재크롤링 필요 (API 비용) | raw_news에서 재가공만 하면 됨 |
| 디버깅 | LLM 입력값 확인 불가 | raw_news에서 원본 확인 가능 |
| 부분 실패 | 전체 재실행 | 실패한 건만 재가공 |

---

## 2. DB 스키마 설계

하나의 SQLite 파일(`k_enter_news.db`) 안에 테이블 2개가 들어갑니다.

### 2-1. `raw_news` 테이블 (1차 — 원본 보관)

크롤링한 뉴스를 **가공 없이 그대로** 저장합니다.

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `id` | Integer, PK, Auto Increment | 고유 ID |
| `title` | String(500) | 기사 제목 | 
| `content` | Text | 기사 본문 전체 | 
| `url` | String(1000), **UNIQUE** | 원문 URL (중복 방지 기준) |
| `published_at` | DateTime, Nullable | 기사 발행일 (Tavily 제공 시) |
| `crawled_at` | DateTime, Default=now | 크롤링 시각 |
| `is_processed` | Boolean, Default=False | LLM 가공 완료 여부 |

- `url` UNIQUE → 같은 기사 중복 저장 방지
- `is_processed` → 미가공 기사만 필터링하여 LLM에 전달
- `content` → 원문 전체 보존, 스키마 변경 시 재가공 가능

### 2-2. `processed_news` 테이블 (2차 — LLM 가공 결과)

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `id` | Integer, PK, Auto Increment | 고유 ID |
| `raw_news_id` | Integer, **FK → raw_news.id** | 원본 기사 참조 |
| `category` | String(20) | K-Pop, K-Drama, Movie, Business, Scandal |
| `sub_category` | String(50) | 핵심 이벤트 요약 (1~2단어) |
| `summary` | Text | AI 3줄 요약 |
| `keywords` | JSON | 해시태그 배열 (예: `["BTS", "컴백"]`) |
| `sentiment` | String(10) | Positive / Negative / Neutral |
| `entities` | JSON | 인물/그룹/회사 배열 (아래 구조 참고) |
| `source_name` | String(100), Nullable | 언론사 이름 |
| `language` | String(5), Default='ko' | 기사 언어 |
| `processed_at` | DateTime, Default=now | 가공 시각 |

**`entities` JSON 구조:**

```json
[
  {
    "entity_name": "뉴진스",
    "entity_type": "Group",
    "is_main_subject": 1
  },
  {
    "entity_name": "하이브",
    "entity_type": "Agency",
    "is_main_subject": 0
  }
]
```

### 2-3. 테이블 관계

```
raw_news (1) ──── (1) processed_news
   │                       │
   │ id ◄───FK──── raw_news_id
   │                       │
   │ is_processed           │
   │ (가공 완료 시           │
   │  False → True)         │
```

---

## 3. 1단계: 크롤링 → raw_news 저장

### 3-1. Tavily 크롤링 함수 (`collector.py`)

```python
import os
from datetime import datetime
from tavily import TavilyClient

def fetch_news_from_tavily(query: str, max_results: int = 5) -> list[dict]:
    """
    Tavily API로 뉴스를 검색하여 원본 데이터를 반환합니다.

    Args:
        query: 검색 키워드 (예: "뉴진스 빌보드")
        max_results: 최대 검색 결과 수 (기본 5건)

    Returns:
        list[dict]: raw_news 테이블에 INSERT 가능한 딕셔너리 리스트
    """
    client = TavilyClient(api_key=os.getenv("TAVILY_API_KEY"))

    response = client.search(
        query=query,
        search_depth="advanced",   # "basic" (빠름) / "advanced" (정확)
        topic="news",              # 뉴스 검색에 특화
        max_results=max_results,
        include_raw_content=True,  # 기사 전문 포함
    )

    results = []
    for item in response.get("results", []):
        results.append({
            "title": item.get("title", ""),
            "content": item.get("raw_content") or item.get("content", ""),
            "url": item.get("url", ""),
            "published_at": item.get("published_date"),
            "crawled_at": datetime.now(),
            "is_processed": False,
        })

    return results
```

**Tavily `search()` 파라미터 참고:**

| 파라미터 | 타입 | 설명 |
|----------|------|------|
| `query` | str | 검색 키워드 |
| `search_depth` | str | `"basic"` / `"advanced"` |
| `topic` | str | `"general"` / `"news"` |
| `max_results` | int | 최대 결과 수 (1~10) |
| `include_raw_content` | bool | `True`면 기사 전문 포함 |
| `days` | int | 최근 N일 이내 결과만 |

**Tavily API 응답 예시:**

```json
{
  "results": [
    {
      "url": "https://example.com/news/123",
      "title": "뉴진스, 신곡으로 빌보드 핫100 진입",
      "content": "요약된 본문...",
      "raw_content": "기사 전문 텍스트...",
      "score": 0.95,
      "published_date": "2026-04-05T09:00:00Z"
    }
  ]
}
```

### 3-2. raw_news 저장 함수

```python
from sqlalchemy.exc import IntegrityError

def save_raw_news(session, news_list: list[dict]) -> int:
    """
    크롤링 결과를 raw_news에 저장합니다. url 중복 시 스킵.

    Returns:
        int: 신규 저장된 건수
    """
    saved_count = 0
    for news in news_list:
        try:
            raw = RawNews(**news)
            session.add(raw)
            session.commit()
            saved_count += 1
        except IntegrityError:
            session.rollback()  # url 중복 → 스킵
    return saved_count
```

---

## 4. 2단계: LLM 가공 → processed_news 저장

### 4-1. LLM 가공 함수 (`analyzer.py`)

```python
import json
from openai import OpenAI

def parse_article_to_json(raw_content: str) -> dict | None:
    """
    기사 본문을 LLM에 전달하여 정형화된 JSON으로 변환합니다.
    실패 시 None을 반환합니다 (다음 실행 때 자동 재시도).
    """
    client = OpenAI()  # 환경변수 OPENAI_API_KEY 자동 참조

    system_prompt = """
    너는 K-엔터 뉴스 전문 데이터 분석기야.
    제공된 뉴스 원문을 읽고, 반드시 아래 JSON 포맷에 맞추어 데이터를 추출해.

    [목표 JSON 구조]
    {
      "category": "string (K-Pop, K-Drama, Movie, Business, Scandal 중 택1)",
      "sub_category": "string (핵심 이벤트 1~2단어 요약)",
      "summary": "string (뉴스 보도체, 3문장 이내 요약)",
      "keywords": ["string", ...] (핵심 키워드 3~5개 배열),
      "sentiment": "string (Positive, Negative, Neutral)",
      "entities": [
        {
          "entity_name": "string (인물, 그룹, 회사명)",
          "entity_type": "string (Artist, Group, Agency, Broadcaster 중 택1)",
          "is_main_subject": integer (주인공이면 1, 단순 언급이면 0)
        }
      ],
      "source_name": "string (언론사 이름, 모르면 null)",
      "language": "string (ko, en 등)"
    }

    [규칙]
    - 반드시 위 구조를 지켜라. 키 이름을 변경하지 마라.
    - 모르는 정보는 null로 채워라.
    - entities는 기사에 등장하는 모든 주요 인물/단체를 배열로 나열해라.
    """

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            response_format={"type": "json_object"},  # JSON 모드 강제
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"다음 기사를 분석해서 JSON으로 변환해줘:\n\n{raw_content}"},
            ],
            temperature=0.1,  # 일관성 우선
        )
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        print(f"LLM 가공 실패: {e}")
        return None
```

**OpenAI API 핵심 설정:**

| 설정 | 값 | 이유 |
|------|----|------|
| `model` | `gpt-4o-mini` | 비용 효율 (입력 $0.15/1M, 출력 $0.60/1M) |
| `response_format` | `{"type": "json_object"}` | 유효한 JSON만 반환하도록 강제 |
| `temperature` | `0.1` | 창의성보다 일관성 (데이터 추출 용도) |

### 4-2. 미가공 기사 일괄 처리 함수

```python
def process_unprocessed_news(session) -> int:
    """
    raw_news에서 is_processed=False인 기사를 가져와
    LLM 가공 후 processed_news에 저장합니다.

    Returns:
        int: 가공 완료된 건수
    """
    unprocessed = session.query(RawNews).filter(
        RawNews.is_processed == False
    ).all()

    processed_count = 0
    for raw in unprocessed:
        result = parse_article_to_json(raw.content)

        if result is None:
            continue  # 실패 시 스킵 → is_processed=False 유지 → 다음에 재시도

        processed = ProcessedNews(
            raw_news_id=raw.id,
            category=result.get("category"),
            sub_category=result.get("sub_category"),
            summary=result.get("summary"),
            keywords=result.get("keywords", []),
            sentiment=result.get("sentiment"),
            entities=result.get("entities", []),
            source_name=result.get("source_name"),
            language=result.get("language", "ko"),
        )
        session.add(processed)

        raw.is_processed = True  # 가공 완료 표시
        session.commit()
        processed_count += 1

    return processed_count
```

---

## 5. 전체 파이프라인 실행 (`main.py`)

```python
from dotenv import load_dotenv
load_dotenv()

def main():
    # ===== 1단계: 크롤링 → raw_news 저장 =====
    queries = {
        "K-POP": "K-POP 아이돌 최신 뉴스",
        "K-DRAMA": "한국 드라마 최신 뉴스",
        "ACTOR": "한국 영화배우 최신 뉴스",
    }

    with get_session() as session:
        total_crawled = 0
        for category, query in queries.items():
            print(f"[1단계] '{query}' 크롤링 중...")
            news_list = fetch_news_from_tavily(query)
            saved = save_raw_news(session, news_list)
            total_crawled += saved
            print(f"  → {saved}건 신규 저장 (중복 제외)")

        print(f"\n[1단계 완료] 총 {total_crawled}건 raw_news에 저장")

        # ===== 2단계: 미가공 기사 → LLM 가공 → processed_news 저장 =====
        print("\n[2단계] 미가공 기사 LLM 분석 시작...")
        processed = process_unprocessed_news(session)
        print(f"[2단계 완료] 총 {processed}건 가공 완료")

if __name__ == "__main__":
    main()
```

---

## 6. 저장 결과 예시

### raw_news 테이블

| id | title | content | url | is_processed |
|----|-------|---------|-----|-------------|
| 1 | 뉴진스, 빌보드 핫100 진입 | 그룹 뉴진스(NewJeans)가... (전문) | https://example.com/123 | True |
| 2 | BTS 진, 솔로 앨범 발매 | 방탄소년단 진이... (전문) | https://example.com/456 | True |
| 3 | 이영애, 새 드라마 출연 확정 | 배우 이영애가... (전문) | https://example.com/789 | False |

### processed_news 테이블

| id | raw_news_id | category | summary | keywords | sentiment | entities |
|----|-------------|----------|---------|----------|-----------|----------|
| 1 | 1 | K-Pop | 뉴진스가 신곡으로 빌보드... | ["뉴진스","빌보드","핫100"] | Positive | [{"entity_name":"뉴진스",...}] |
| 2 | 2 | K-Pop | BTS 진이 첫 솔로 앨범을... | ["BTS","진","솔로"] | Positive | [{"entity_name":"진",...}] |

> id=3은 `is_processed=False` → 아직 가공 안 됨 (다음 실행 시 자동 처리)

---

## 7. 스키마 변경 시 재가공 방법

프롬프트나 스키마를 수정한 뒤, 기존 데이터를 재가공하려면:

```python
# 1. processed_news 비우기
session.query(ProcessedNews).delete()

# 2. raw_news 전체를 미가공 상태로 리셋
session.query(RawNews).update({"is_processed": False})
session.commit()

# 3. 재가공 실행
process_unprocessed_news(session)
```

**Tavily API를 다시 호출하지 않고** raw_news 원본으로 재가공합니다.
** 아마 랭체인을 토한 open ai 호출로 제가공 해야할거 같음...

---

## 8. 팀원별 수정 포인트

| 수정하고 싶은 것 | 수정할 위치 | 설명 |
|------------------|-------------|------|
| 검색 키워드 변경 | `main.py`의 `queries` 딕셔너리 | 카테고리/검색어 추가·삭제 |
| 검색 결과 수 조절 | `collector.py`의 `max_results` | 기본 5건, 최대 10건 |
| LLM 추출 구조 변경 | `analyzer.py`의 `system_prompt` | 필드 추가/삭제/이름 변경 |
| LLM 모델 변경 | `analyzer.py`의 `model` | `gpt-4o`, `gpt-4o-mini` 등 |
| DB 컬럼 추가 | `database.py` ORM 모델 | 테이블 구조 변경 |

---

## 9. 에러 대응 가이드

| 에러 | 원인 | 해결 |
|------|------|------|
| `AuthenticationError` | API 키 오류 | `.env` 파일 키 확인 |
| `RateLimitError` | API 호출 한도 초과 | 대기 후 재시도 또는 플랜 업그레이드 |
| `json.JSONDecodeError` | LLM 응답이 유효한 JSON 아님 | `response_format` 설정 확인 |
| `IntegrityError` | url 중복 저장 시도 | 정상 동작 (자동 스킵됨) |
| LLM 가공 실패 | OpenAI API 일시 장애 | `is_processed=False` 유지 → 다음 실행 시 자동 재시도 |
