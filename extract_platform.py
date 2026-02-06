import base64
import json
from bs4 import BeautifulSoup

urls = rss_feeds = {
    "무신사(MUSINSA)": "https://medium.com/feed/musinsa-tech",
    "네이버 D2": "https://d2.naver.com/d2.atom",
    "마켓컬리": "https://helloworld.kurly.com/feed.xml",
    "우아한형제들": "https://techblog.woowahan.com/feed",
    "카카오엔터프라이즈": "https://tech.kakaoenterprise.com/feed",
    "데브시스터즈": "https://tech.devsisters.com/rss.xml",
    "라인(LINE)": "https://engineering.linecorp.com/ko/feed/index.html",
    "쿠팡(Coupang)": "https://medium.com/feed/coupang-engineering",
    "당근마켓": "https://medium.com/feed/daangn",
    "토스(Toss)": "https://toss.tech/rss.xml",
    "직방": "https://medium.com/feed/zigbang",
    "왓챠(Watcha)": "https://medium.com/feed/watcha",
    "뱅크샐러드(banksalad)": "https://blog.banksalad.com/rss.xml",
    "Hyperconnect": "https://hyperconnect.github.io/feed.xml",
    "요기요(yogiyo)": "https://techblog.yogiyo.co.kr/feed",
    "쏘카(Socar)": "https://tech.socarcorp.kr/feed",
    "리디(RIDI)": "https://www.ridicorp.com/feed",
    "NHN Toast": "https://meetup.toast.com/rss",
    "Engineer at Meta": "https://code.facebook.com/posts/rss/",
    "Reddit": "https://reddit.com/r/ExperiencedDevs.rss",
    "Netflix TechBlog": "https://medium.com/feed/netflix-techblog",
    "Amazon": "https://aws.amazon.com/ko/blogs/tech/feed"
}
import feedparser

only_rss = {}

for platform, url in urls.items():
    print(platform+"-------------------------")
    print(url)
    feed = feedparser.parse(url)
    total_chars = 0
    for entry in feed.entries:
        content_body = ""
        if hasattr(entry, 'content'):
            content_body = str(entry.content[0].value)
        elif hasattr(entry, 'summary'):
            content_body = str(entry.summary)
        elif hasattr(entry, 'description'):
            content_body = str(entry.description)
        
        # 2. HTML 태그를 제거하고 순수 텍스트 길이만 측정
        clean_text = BeautifulSoup(content_body, "html.parser").get_text()
        total_chars += len(clean_text)
    print(f"평균 글자 수: {total_chars/len(feed.entries)}")
    if total_chars>1000:
        platform = base64.b64encode(platform.encode('utf-8')).decode('utf-8')
        only_rss[platform] = url
target_platform = json.dumps(only_rss, ensure_ascii=False)
with open("/Users/jeongdaegyun/airflow_dc/config/platforms.json", "w", encoding='utf-8') as f:
    f.write(target_platform)