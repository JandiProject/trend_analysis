import re

def _get_shortest_portion(text, ratio=0.5)->list[str]:
    sentences = [s.strip() for s in re.split(r'(?<=[.?!])\s+', text) if s.strip()]
    # 길이에 따라 정렬
    sentences.sort(key=len)
    
    # 하위(짧은 쪽) n%만 추출
    num_to_keep = int(len(sentences) * ratio)
    return sentences[:num_to_keep]

def _clean_text(text):
    if not text:
        return ""

    # 1. 이전 단계: URL, 특수문자 정제
    text = re.sub(r'https?://\S+|www\.\S+', '', text)
    text = re.sub(r'[^가-힣a-zA-Z0-9\s.?!]', ' ', text)
    
    # 2. 불용어 리스트 정의 (기술 블로그 및 한국어 특성 반영)
    stop_words = {
        "내용", "이번", "경우", "통해", "대한", "위해", "관련", "정도", "이후", "사실",
        "생각", "사용", "진행", "확인", "작업", "부분", "기존", "기능", "방식", "설정",
        "방법", "하나", "단어", "단락", "문장", "블로그", "포스팅", "주소", "댓글",
        "감사합니다", "참고", "링크", "아래", "다음", "그리고", "하지만", "또한", "매우",
        "그냥", "어떤", "지금", "오늘", "매일", "진짜", "역시", "항상", "종종", "한번"
    }

    # 3. 단어 단위로 쪼개어 필터링
    # split()은 공백을 기준으로 단어를 나눕니다.
    words = text.split()
    
    # 불용어에 포함되지 않고, 길이가 2글자 이상인 단어만 추출
    clean_words = [word for word in words if word not in stop_words and len(word) >= 2]

    # 4. 다시 문장으로 합침
    result_text = " ".join(clean_words)
    
    # 5. 다중 공백 제거
    result_text = re.sub(r'\s+', ' ', result_text).strip()


    
    return result_text

def process_text(text:str) -> list[str]:
    sentences = _get_shortest_portion(text=text)
    sentences = list(map(_clean_text, sentences))
    return sentences