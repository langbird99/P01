"""
main.py — 전체 파이프라인 실행 (진입점)

실행 순서:
  1단계: 최신 뉴스 크롤링 (RSS + Tavily) → raw_news
  2단계: LLM 가공 → processed_news
  3단계: 과거 뉴스 크롤링 → past_news
  4단계: 이미지 수집 → thumbnail_url
"""

from dotenv import load_dotenv
from database import get_session

load_dotenv()


def main():
    import crawler
    import processor

    with get_session() as session:
        # 1단계: 최신 뉴스 크롤링 → raw_news
        saved = crawler.crawl_and_save(session)
        print(f"\n총 {saved}건 저장 완료")

        # 2단계: LLM 가공 → processed_news
        processed = processor.process_and_save(session)
        print(f"총 {processed}건 가공 완료")

        # 3단계: 과거 뉴스 크롤링 → past_news
        past_saved = crawler.crawl_past_news(session)
        print(f"총 {past_saved}건 아티스트에 대한 추가 정보 저장완료!!")

    # 4단계: 이미지 수집
    processor.fetch_all_images()
    print("이미지 수집 완료")


if __name__ == "__main__":
    main()
