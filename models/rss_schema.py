class ArticleSchema:
    '''
    rss로 추출한 한 개의 post를 나타냅니다.
    source: 자료의 출처 이름을 base64로 인코딩한 문자열입니다.
    title: 글 제목
    url: 글 URL
    rss_content: RSS 피드에서 추출한 원본 콘텐츠 (HTML 태그 제거)
    content: 본문 내용 (개별 크롤링을 통해 얻은 내용)
    published_at: 글이 게시된 날짜 (ISO 8601 형식)
    created_at: 글이 크롤링된 날짜 (ISO 8601 형식)
    encoded_url: 글 URL의 MD5 해시값 (자료 중복 저장 방지)
    '''
    source: str
    title: str
    url: str
    rss_content: str
    content: str
    published_at:str
    created_at: str
    encoded_url:str
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)