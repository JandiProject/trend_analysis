# To run this code you need to install the following dependencies:
# pip install google-genai

import os
from typing import Literal
from google import genai
from google.genai import types
from pydantic import BaseModel

category_to_field = {
    # Tech
    "IT": "Tech", "Electronics": "Tech", "Backend": "Tech", "Frontend": "Tech",
    "Infra": "Tech", "Security": "Tech", "Mobile": "Tech", "Embedded": "Tech",
    "Game": "Tech", "Robot": "Tech",
    # AI/Data
    "CV": "AI/Data", "NLP": "AI/Data", "DA": "AI/Data", "DE": "AI/Data",
    # Industry
    "Bio": "Industry", "Electronics": "Industry",
    # Daily
    "Daily": "Daily", "Review": "Daily", "Cook": "Daily", "Trip": "Daily",
    "Beauty": "Daily", "Movie": "Daily", "Fashion": "Daily", "Career": "Daily",
    # Humanities
    "Literature": "Humanities", "History": "Humanities", "Linguistics": "Humanities",
    # Social
    "Economics": "Social", "Geography": "Social", "Social": "Social", "Other": "Other"
}

categories = [
    "IT", "Electronics", "Backend", "Frontend", "Infra", "Security", "Mobile", "Embedded", "Game", "Robot",
    "CV", "NLP", "DA", "DE",
    "Bio", "Electronics",
    "Daily", "Review", "Cook", "Trip", "Beauty", "Movie", "Fashion", "Career",
    "Literature", "History", "Linguistics",
    "Economics", "Geography", "Social", "Other"
]
Categories = Literal [
    "IT", "Electronics", "Backend", "Frontend", "Infra", "Security", "Mobile", "Embedded", "Game", "Robot",
    "CV", "NLP", "DA", "DE",
    "Bio", "Electronics",
    "Daily", "Review", "Cook", "Trip", "Beauty", "Movie", "Fashion", "Career",
    "Literature", "History", "Linguistics",
    "Economics", "Geography", "Social", "Other"
]

class ArticleClassification(BaseModel):
    keywords: list[str]
    category: Categories
    summary: str

def generate(text: list[str]) -> str:
    client = genai.Client(
        api_key=os.getenv('GEMINI_API_KEY'),
    )

    model = "gemini-2.5-flash-lite"
    contents = [
        types.Content(
            role="user",
            parts=[
                types.Part.from_text(text="""
                                     Role: You are a Senior Editor and Content Analyst. Your mission is to parse articles across various domains, identify their core subjects, and maintain a high-quality content classification database. Valid Categories: {category_pool} STRICT RULE: You MUST only select a category that exists in Valid Categories above. Do NOT invent, infer, or approximate new categories. Any output containing a category outside the list is considered invalid. Task Instructions: Analysis (thought_process): Before deciding on a keyword, briefly analyze the article's context. Determine: "Is the primary subject a specific concept, methodology, or theory? Or is it an entity (company, person, organization) or an event (conference, festival, talk)?" If the article describes something associated with a specific entity, focus only on the core subject or underlying concept, not the entity itself. Keyword Selection (keyword): Select exactly one noun that represents the core subject of the article. PRIORITY: Specific concepts, methodologies, tools, frameworks, theories, or practices. STRICT PROHIBITION: Do not use company names, person names, organization names, or event titles. Bad: Naver, Toss, Kakao Enterprise, UNESCO, AWS re:Invent, Slash24. Good: Vector Database, Microservices, Fermentation, Renaissance, Behavioral Economics. Category Selection (category): Select exactly one category from Valid Categories that most precisely describes the core subject of the article. VALIDATION: Confirm the selected category exists verbatim in Valid Categories. If not, reselect. If no category fits perfectly, select the closest one and note it in thought_process. Summary (summary): Provide a concise, one-sentence overview of the article. LANGUAGE REQUIREMENT: This field MUST be written in Korean. Few-Shot Examples: Content: "How we optimized our search engine at Naver using Elasticsearch." Keyword: Elasticsearch (Correct) | Keyword: Naver (Wrong) Content: "Introducing the new feature of our service announced at Toss Slash 23." Keyword: the specific tech mentioned (Correct) | Keyword: Toss Slash 23 (Wrong) Content: "A deep dive into the history of the Roman Empire's expansion." Keyword: Roman Empire (Correct) | Keyword: History Channel (Wrong) Content: "UNESCO's report on the endangered languages of Southeast Asia." Keyword: Endangered Languages (Correct) | Keyword: UNESCO (Wrong) Content: "Our journey of migrating to a Distributed Database system." Keyword: Distributed Database (Correct)
                                     """.format(category_pool= ", ".join(categories))),
                types.Part.from_text(text=" ".join(text)),
            ],
        ),
    ]
    generate_content_config = types.GenerateContentConfig(
        response_mime_type="application/json",
        response_schema=ArticleClassification.model_json_schema(),
    )
    result = ""

    for chunk in client.models.generate_content_stream(
        model=model,
        contents=contents,
        config=generate_content_config,
    ):
        result += chunk.text if chunk.text else ""

    return result


if __name__=="__main__":
    import dotenv
    dotenv.load_dotenv("/Users/jeongdaegyun/airflow_dc/.env")
    print(generate(["안녕하세요. 오늘은 Docker에 대해 알아보겠습니다. Docker는 컨테이너 기반의 오픈소스 가상화 플랫폼으로, 애플리케이션을 신속하게 배포하고 확장할 수 있도록 도와줍니다. Docker를 사용하면 개발 환경과 운영 환경 간의 일관성을 유지할 수 있어, DevOps 문화에서 매우 중요한 역할을 합니다."]))