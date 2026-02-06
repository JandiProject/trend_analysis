from typing import List
from datetime import datetime
import logging
from common.models.rss_schema import ArticleSchema


logger = logging.getLogger(__name__)

class RSSParser():
    platform: str = ""


    def __init__(self, platform: str, url: str) -> None:
        self.platform = platform
        self.url = url

    def normalize(self, entry) -> ArticleSchema:
        """
        RSS 엔트리를 ArticleSchema로 변환

        Args:
            entry: feedparser entry object

        Returns:
            ArticleSchema instance
        """
        from bs4 import BeautifulSoup
        import hashlib

        parsed_content = ""

        if hasattr(entry, 'content'):
            parsed_content = str(entry.content[0].value)
        elif hasattr(entry, 'summary'):
            parsed_content = str(entry.summary)
        elif hasattr(entry, 'description'):
            parsed_content = str(entry.description)


        parsed_content = BeautifulSoup(parsed_content, "html.parser").get_text()
        article = ArticleSchema(
            source=self.platform,
            title=entry.get("title", ""),
            url=entry.get("link", ""),
            rss_content= parsed_content,
            content="",
            published_at=entry.get("published", ""),
            created_at=datetime.now().isoformat(),
            encoded_url= hashlib.md5(entry.get("link", "").encode('utf-8')).hexdigest(),
        )
        return article

    def parse(self) -> List[ArticleSchema]:
        """
        RSS 피드를 파싱하여 ArticleSchema 리스트 반환 (공통 로직)

        Returns:
            List of ArticleSchema
        """
        import feedparser
        try:
            rss_url = self.url
            logger.info(f"Fetching RSS from: {rss_url}")

            # feedparser로 파싱
            feed = feedparser.parse(rss_url)

            if not feed.entries:
                logger.warning(f"No entries found in RSS feed: {rss_url}")
                return []

            # 각 엔트리를 ArticleSchema로 변환
            articles = []
            for entry in feed.entries:
                try:
                    article = self.normalize(entry)
                    articles.append(article)
                except Exception as e:
                    logger.error(f"Failed to normalize entry: {e}")
                    continue

            logger.info(f"Parsed {len(articles)} articles from {rss_url}")
            return articles

        except Exception as e:
            logger.error(f"Unexpected error parsing RSS from {self.url}: {e}")
            return []