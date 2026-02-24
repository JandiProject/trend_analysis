# To run this code you need to install the following dependencies:
# pip install google-genai

import os
from google import genai
from google.genai import types


def generate(text: list[str]) -> str:
    client = genai.Client(
        api_key=os.getenv('GEMINI_API_KEY'),
    )

    model = "gemini-2.5-flash-lite"
    category_pool = ["Frontend", "Backend", "Infrastructure", "AI/ML", "Mobile", "Data Engineering", "Security", "Culture"]
    contents = [
        types.Content(
            role="user",
            parts=[
                types.Part.from_text(text="""Role:
You are a Senior Technology Editor specializing in analyzing global IT engineering blogs. Your goal is to identify the primary technology stack discussed in an article and classify it with high precision.
Instructions:
keyword:
Identify exactly one core technology noun that represents the main subject of the article.
Prioritize Open Source Software (OSS), specific frameworks, or technical theories (e.g., Docker, React, PyTorch, Kafka, Raft Consensus).
STRICT EXCLUSION: Do not select company names, organization names, or event names (e.g., Kakao Enterprise, Toss, NAVER ENGINEERING DAY, AWS re:Invent).
category:
Select exactly one category that best describes the article's domain from the following pool: {category_pool}.
summary:
Provide a concise, one-sentence summary of the article.
LANGUAGE REQUIREMENT: The summary must be written in Korean.""".format(category_pool=", ".join(category_pool))),
                types.Part.from_text(text=" ".join(text)),
            ],
        ),
    ]
    generate_content_config = types.GenerateContentConfig(
        response_mime_type="application/json",
        response_schema=genai.types.Schema(
            type = genai.types.Type.OBJECT,
            required = ["keywords", "category", "summary"],
            properties = {
                "keywords": genai.types.Schema(
                    type = genai.types.Type.ARRAY,
                    items = genai.types.Schema(
                        type = genai.types.Type.STRING,
                    ),
                ),
                "category": genai.types.Schema(
                    type = genai.types.Type.STRING,
                ),
                "summary": genai.types.Schema(
                    type = genai.types.Type.STRING,
                ),
            },
        ),
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