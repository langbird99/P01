# 실시간 K-엔터 속보 대시보드 — 개발 프롬프트 v2

> **용도**: 이 문서를 AI 코딩 어시스턴트(Cursor, Copilot, Claude 등)에 그대로 붙여넣어 프로젝트 초기 뼈대 코드를 생성하는 프롬프트입니다.

---

너는 파이썬 기반의 데이터 엔지니어링 및 AI 웹 대시보드 아키텍트야.
아래의 기획서와 기술 스택을 바탕으로 **'실시간 K-엔터 속보 대시보드'** 프로젝트의 초기 뼈대 코드를 작성해 줘.

---

## 1. 프로젝트 개요 및 핵심 요구사항

- **목표**: K-팝, K-드라마, 영화배우 관련 최신 뉴스를 수집하고 AI로 요약하여 Streamlit 대시보드로 제공하는 실시간 서비스.
- **핵심 설계 원칙**:
  - 사용자가 추후 **'크롤링 세부 로직, LangChain 프롬프트, JSON 추출 스키마'를 직접 디테일하게 수정할 예정**이므로, 각 워크플로우 단위로 `.py` 파일을 철저하게 분리(모듈화)하여 작성할 것.
  - 모든 외부 API 호출은 실패를 전제로 설계하고, 재시도/폴백/로깅을 반드시 포함할 것.

---

## 2. 기술 스택

| 영역 | 기술 |
|------|------|
| 패키지 관리 | `pyproject.toml` + pip (Poetry 선택 사항) |
| 백엔드/스케줄러 | Python 3.11+, APScheduler |
| 수집 및 AI | Tavily API, LangChain, Pydantic, OpenAI API (또는 Anthropic API) |
| 데이터 전처리/저장 | Pandas, SQLAlchemy |
| 데이터베이스 | **기본: SQLite** (로컬 개발용), `DATABASE_URL`만 변경하면 MySQL/PostgreSQL 전환 가능하도록 설계 |
| 프론트엔드 | Streamlit, streamlit-autorefresh |

---

## 3. 디렉토리 구조 (워크플로우 모듈화)

```
k_enter_dashboard/
├── pyproject.toml
├── .env.example
├── config.py           # 환경변수 로드 및 설정
├── logger.py           # ★ 로깅 설정 (신규)
├── database.py         # SQLAlchemy 모델 및 DB 연결
├── schemas.py          # ★ 사용자가 수정할 Pydantic JSON 구조
├── prompts.py          # ★ 사용자가 수정할 LangChain 프롬프트 템플릿
├── collector.py        # ★ Tavily 뉴스 수집 전담 (services.py에서 분리)
├── analyzer.py         # ★ LangChain LLM 분석 전담 (services.py에서 분리)
├── services.py         # 오케스트레이션: collector → analyzer → DB 저장
├── scheduler.py        # APScheduler 실행 파일 (트랙 A)
└── app.py              # Streamlit 화면 (트랙 B)
```

> **분리 이유**: `services.py`에 수집·분석·저장을 모두 넣으면 어느 하나만 수정할 때도 전체를 건드려야 한다. `collector.py`(수집), `analyzer.py`(AI 분석), `services.py`(조합)로 분리하면 각각 독립적으로 수정·테스트 가능.

---

## 4. 데이터베이스 스키마 설계 (SQLAlchemy)

테이블명: `k_news_feed`

| 컬럼 | 타입 | 설명 |
|------|------|------|
| id | Integer, PK, Auto Increment | 고유 ID |
| category | String(20) | K-POP, K-DRAMA, ACTOR |
| title | String(500) | 기사 제목 |
| summary | Text | AI 3줄 요약 |
| keywords | JSON | 해시태그 배열 (예: `["BTS", "컴백", "빌보드"]`) |
| sentiment | Integer | 감성 점수 (1~10, 1=매우 부정, 10=매우 긍정) |
| source_url | String(1000), **UNIQUE** | 출처 링크 (**중복 방지 기준**) |
| raw_content | Text | ★ LLM에 넘긴 원문 텍스트 (재처리 대비 보존) |
| language | String(5), Default='ko' | 기사 언어 (ko, en 등) |
| published_at | DateTime | 원문 발행 시각 |
| created_at | DateTime, Default=now | DB INSERT 시각 |

> **중복 방지**: `source_url`에 UNIQUE 제약 조건을 걸고, INSERT 시 `IntegrityError`를 catch하여 스킵 처리.

---

## 5. 각 파일별 상세 구현 지시

### [.env.example]

```env
# === API Keys ===
TAVILY_API_KEY=tvly-xxxxxxxxxxxxx
OPENAI_API_KEY=sk-xxxxxxxxxxxxx

# === Database ===
# 기본값: SQLite (로컬 개발용). MySQL 전환 시 아래 주석 해제.
DATABASE_URL=sqlite:///k_enter_news.db
# DATABASE_URL=mysql+pymysql://user:password@localhost:3306/k_enter_db

# === Scheduler ===
FETCH_INTERVAL_MINUTES=10

# === Dashboard ===
AUTO_REFRESH_SECONDS=180
```

---

### [config.py]

- `python-dotenv`를 사용하여 `.env`의 환경변수를 로드하는 **Settings 클래스** 작성.
- 모든 설정값에 타입 힌트와 기본값을 부여할 것.
- `DATABASE_URL` 기본값은 `sqlite:///k_enter_news.db`로 설정.
- `FETCH_INTERVAL_MINUTES` (기본 10), `AUTO_REFRESH_SECONDS` (기본 180) 포함.

---

### [logger.py] ★ 신규

- Python 표준 `logging` 모듈 기반.
- 콘솔 출력 + 파일 로깅(`logs/app.log`) 동시 지원.
- 포맷: `[%(asctime)s] %(levelname)s %(name)s: %(message)s`
- `get_logger(name)` 함수 하나로 각 모듈에서 로거를 가져다 쓸 수 있게 구성.
- 로그 레벨은 `.env`의 `LOG_LEVEL`(기본 INFO)로 제어.

---

### [database.py]

- SQLAlchemy `create_engine` 및 `sessionmaker` 설정.
  - SQLite일 때는 `check_same_thread=False` 옵션 자동 적용.
  - MySQL/PostgreSQL일 때는 커넥션 풀 설정 (`pool_size=5`, `pool_recycle=3600`).
- `k_news_feed` 테이블의 **ORM 모델 클래스** 정의 (섹션 4의 스키마대로).
- `Base.metadata.create_all(engine)` 포함.
- DB 세션을 컨텍스트 매니저로 제공하는 `get_session()` 함수 작성.

---

### [schemas.py] ★ 핵심 분리 영역

- LangChain 구조화된 출력을 위한 **Pydantic** 클래스 `KEnterNewsSchema` 작성.
- 기본 속성:
  - `summary: str` — AI 3줄 요약
  - `keywords: list[str]` — 해시태그 (3~5개)
  - `sentiment: int` — 감성 점수 (1~10)
- 각 필드에 `Field(description=...)` 으로 LLM이 참고할 설명 첨부.
- 파일 상단에 아래와 같은 한글 주석을 명확히 남길 것:

```python
"""
★ [사용자 수정 영역] schemas.py
-------------------------------
이 파일은 LLM이 뉴스 기사에서 추출할 JSON 구조를 정의합니다.
필드를 추가/삭제/수정하여 원하는 데이터를 추출하세요.

예시: 아티스트 이름, 관련 그룹, 이벤트 날짜 등을 추가할 수 있습니다.
수정 후에는 database.py의 ORM 모델과 app.py의 표시 로직도 함께 수정하세요.
"""
```

---

### [prompts.py] ★ 핵심 분리 영역

- LangChain의 `PromptTemplate` 정의.
- 변수: `{format_instructions}`, `{article_text}`
- 프롬프트에 다음 규칙을 **명시적으로** 포함할 것:
  - 출력 언어: 한국어
  - sentiment 점수: 1~10 (1=매우 부정, 10=매우 긍정)
  - keywords: 3~5개의 한국어 해시태그
  - 요약 톤: 뉴스 보도체, 3문장 이내
- 파일 상단에 아래와 같은 한글 주석을 명확히 남길 것:

```python
"""
★ [사용자 수정 영역] prompts.py
-------------------------------
이 파일은 LLM에게 전달할 프롬프트 템플릿을 정의합니다.
요약 스타일, 톤, 추출 규칙 등을 자유롭게 수정하세요.

예시: '팬 커뮤니티 톤으로 작성해줘', '영어로도 병기해줘' 등
프롬프트를 수정하면 schemas.py의 필드와 일관성을 유지해야 합니다.
"""
```

---

### [collector.py] ★ 수집 전담 (services.py에서 분리)

- `fetch_news(query: str, max_results: int = 5) -> list[dict]` 함수 작성.
  - Tavily API(TavilyClient)를 호출하여 최신 뉴스 검색.
  - 반환값: `[{"title": ..., "content": ..., "url": ...}, ...]`
- **에러 처리**: API 실패 시 빈 리스트 반환 + 로그 경고.
- **재시도**: `tenacity` 라이브러리로 최대 3회 재시도 (exponential backoff).
- 검색 쿼리 목록은 함수 외부에 `DEFAULT_QUERIES` 딕셔너리로 정의:

```python
DEFAULT_QUERIES = {
    "K-POP": "K-POP 아이돌 최신 뉴스",
    "K-DRAMA": "한국 드라마 최신 뉴스",
    "ACTOR": "한국 영화배우 최신 뉴스",
}
# ★ 카테고리나 검색어를 자유롭게 추가/수정하세요.
```

---

### [analyzer.py] ★ LLM 분석 전담 (services.py에서 분리)

- `analyze_article(article_text: str) -> KEnterNewsSchema | None` 함수 작성.
  - `prompts.py`의 프롬프트 + `schemas.py`의 Pydantic 모델을 결합하여 LangChain 구동.
  - LangChain의 `with_structured_output()` 사용.
- **에러 처리/폴백**:
  - LLM 호출 실패 시 `None` 반환 + 로그 경고.
  - 파싱 실패 시에도 `None` 반환 (스킵 처리).
- **LLM 모델 설정**: `config.py`에서 모델명을 가져오되, 기본값은 `gpt-4o-mini`.

---

### [services.py] — 오케스트레이션

- `process_and_save_news() -> int` 함수 작성 (반환값: 신규 저장 건수).
  - **Step 1**: `collector.py`의 `fetch_news()`로 각 카테고리별 뉴스 수집.
  - **Step 2**: 각 기사에 대해 `analyzer.py`의 `analyze_article()` 실행.
  - **Step 3**: 분석 결과를 Pandas DataFrame으로 변환.
    - `None` 결과는 필터링(제거).
    - 결측치 처리: `keywords`가 빈 경우 빈 리스트 `[]` 할당, `sentiment`가 None이면 5(중립) 할당.
  - **Step 4**: `database.py`의 세션을 통해 DB INSERT.
    - `source_url` 중복 시 `IntegrityError` catch → 스킵 + 로그 info.
  - 전체 과정의 시작/종료/결과를 로그로 출력.

---

### [scheduler.py]

- `APScheduler`의 `BlockingScheduler`를 세팅.
- `config.py`의 `FETCH_INTERVAL_MINUTES` 주기로 `services.py`의 `process_and_save_news()`를 실행.
- 스케줄러 시작/종료/에러를 `logger.py`로 기록.
- `if __name__ == '__main__':` 블록 포함 — 터미널에서 단독 실행 가능.
- 시작 시 즉시 1회 실행 후 스케줄 시작 (`next_run_time=datetime.now()`).

---

### [app.py] — Streamlit 대시보드

- **페이지 설정**: `st.set_page_config(page_title="K-엔터 속보", layout="wide", page_icon="🎬")`
- **자동 새로고침**: `streamlit_autorefresh`로 `config.py`의 `AUTO_REFRESH_SECONDS`(기본 180초)마다 갱신.
- **데이터 로드**: `database.py`를 통해 최근 기사를 쿼리하여 DataFrame으로 변환.
  - 기본 조회: 최근 24시간 이내, 최대 100건.
- **좌측 사이드바**:
  - 카테고리 멀티셀렉트 필터 (K-POP, K-DRAMA, ACTOR).
  - 날짜 범위 선택기 (`st.date_input`).
  - 키워드 검색 입력창 (`st.text_input`).
- **상단 지표 영역**:
  - `st.metric`으로 전체 기사 수, 카테고리별 기사 수, 평균 감성 점수 표시.
  - 3~4개의 `st.columns` 사용.
- **메인 기사 카드**:
  - `st.container` + 반복문으로 기사 카드 렌더링.
  - 각 카드에 표시: 카테고리 배지, 제목, AI 요약, 해시태그 뱃지들, 감성 점수 바, 출처 링크, 발행 시각.
  - 감성 점수에 따라 색상 차별화 (1~3 빨강, 4~6 노랑, 7~10 초록).
- **페이지네이션**: 한 페이지에 10건씩 표시, 페이지 이동 버튼 포함.
- **빈 상태 처리**: 데이터가 없을 때 안내 메시지 + 이모지 표시.

---

## 6. pyproject.toml 주요 패키지

```toml
[project]
name = "k-enter-dashboard"
version = "0.1.0"
description = "실시간 K-엔터 속보 대시보드"
requires-python = ">=3.11"

dependencies = [
    # === Core ===
    "python-dotenv>=1.0.0",
    "pydantic>=2.0",
    
    # === Data & DB ===
    "pandas>=2.0",
    "sqlalchemy>=2.0",
    # "pymysql>=1.1.0",         # MySQL 사용 시 주석 해제
    
    # === AI & LangChain ===
    "langchain>=0.2",
    "langchain-openai>=0.1",
    # "langchain-anthropic>=0.1", # Anthropic API 사용 시 주석 해제
    "tavily-python>=0.3",
    
    # === Scheduler ===
    "apscheduler>=3.10",
    
    # === Web UI ===
    "streamlit>=1.30",
    "streamlit-autorefresh>=1.0",
    
    # === Resilience ===
    "tenacity>=8.0",
]
```

---

## 7. 작업 수행 규칙

1. 위 디렉토리 구조의 **모든 `.py` 파일 + `.env.example` + `pyproject.toml`**의 전체 코드를 하나씩 빠짐없이 출력해 줘.
2. 코드는 **당장 실행 가능한 수준**으로 완벽하게 작성할 것. import 누락, 미구현 함수 없이.
3. `schemas.py`와 `prompts.py`에는 **"★ [사용자 수정 영역]"** 한글 주석을 명확히 남길 것.
4. `collector.py`의 `DEFAULT_QUERIES`에도 수정 안내 주석을 남길 것.
5. 모든 외부 API 호출 함수에는 **try-except + 로깅 + 재시도(또는 폴백)** 로직을 포함할 것.
6. 모든 파일에서 `logger.py`의 `get_logger(__name__)`를 사용할 것.
7. 타입 힌트를 모든 함수 시그니처에 적용할 것.
