---
name: K-Enter News Dashboard Project
description: K-엔터 뉴스 크롤링 → LLM 가공 → 대시보드 프로젝트. 팀 8명(백3,프론트2,프롬프트3). 마감 2026-04-28.
type: project
---

K-엔터 뉴스 대시보드 프로젝트 (마감: 2026-04-28)

**Why:** K-POP/드라마/영화 등 K-엔터 뉴스를 크롤링하여 LLM으로 요약·가공한 뒤 Streamlit 대시보드로 시각화하는 서비스

**팀 구성:** 백엔드 3명, 프론트엔드 2명, 프롬프트 엔지니어 3명 (총 8명)

**기술 스택:** Tavily 크롤링, SQLAlchemy + Pydantic, LangChain, ChromaDB(RAG), Streamlit, TTS

**핵심 결정사항 (2026-04-06 회의):**
- 개발 순서: SQL+Pydantic 데이터 파이프라인(백엔드) 먼저 → ChromaDB+RAG 연동
- 형태소 분석 직접 코딩 X → LangChain+ChromaDB 자체 구조 활용
- 메인 UI: 대시보드 형태 확정
- TTS 뉴스 읽기 기능 확정
- PDF 보고서, 이미지/웹툰 생성은 보류(여유 시 진행)

**How to apply:** 백엔드 파이프라인 우선, RAG는 뼈대 완성 후 붙이기. 주말 최대한 쉬는 일정으로 계획.
