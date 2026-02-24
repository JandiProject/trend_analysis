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
    "NHN Toast": "https://meetup.toast.com/rss",
    "Engineer at Meta": "https://code.facebook.com/posts/rss/",
    "Reddit": "https://reddit.com/r/ExperiencedDevs.rss",
    "Netflix TechBlog": "https://medium.com/feed/netflix-techblog",
    "Amazon": "https://aws.amazon.com/ko/blogs/tech/feed",
    "Stack Overflow": "https://stackoverflow.blog/feed",
    "DEV Community": "https://dev.to/feed",
    "freeCodeCamp": "https://freecodecamp.org/news/rss",
    "A List Apart": "https://alistapart.com/main/feed",
    "DZone": "https://feeds.dzone.com/home",
    "Smashing Magazine": "https://smashingmagazine.com/feed",
    "Shaaf's Blog": "https://shaaf.dev/index.xml",
    "Google Developers Blog": "https://feeds.feedburner.com/GDBcode",
    "Site Point Blog": "https://sitepoint.com/sitepoint.rss",
    "Codrops": "https://tympanus.net/codrops/feed",
    "SCAND Blog": "https://scand.com/company/blog/feed",
    "SD Times": "https://sdtimes.com/feed",
    "Code Simplicity": "https://codesimplicity.com/feed",
    "Jon Skeet's Coding Blog": "https://codeblog.jonskeet.uk/feed",
    "Codemotion Magazine": "https://codemotion.com/magazine/feed",
    "Simple Programmer": "https://simpleprogrammer.com/feed",
    "Smashing Hub": "https://smashinghub.com/feed",
    "WebAppers": "https://feeds.feedburner.com/Webappers",
    "Pontikis.net Blog": "https://pontikis.net/feed"
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
    mean_chars = total_chars / len(feed.entries) if feed.entries else 0
    print(f"평균 글자 수: {mean_chars}")
    if mean_chars > 5000:
        platform = base64.b64encode(platform.encode('utf-8')).decode('utf-8')
        only_rss[platform] = url
target_platform = json.dumps(only_rss, ensure_ascii=False)
with open("/Users/jeongdaegyun/airflow_dc/config/platforms.json", "w", encoding='utf-8') as f:
    f.write(target_platform)