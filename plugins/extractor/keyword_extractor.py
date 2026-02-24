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
                types.Part.from_text(text="""System Prompt:당신은 글로벌 IT 기업들의 기술 블로그를 분석하는 시니어 테크 에디터입니다. 당신의 목적은 수많은 아티클 속에서 '핵심 기술 스택'을 식별하고 이를 정해진 기준에 따라 분류하는 것입니다.Task:다음 아티클을 읽고 아래 지침에 따라 분석 결과를 JSON으로 출력하세요.keyword: 아티클에서 가장 핵심적으로 다루는 기술 명사 딱 하나를 선정하세요. 오픈소스 소프트웨어 이름이나 기술 이론 이름을 최우선으로 선정하라. (예: Docker, React, PyTorch, Kafka) 특정 기업 이름이나 행사 이름은 선정하지 말것. (ex. 카카오엔터프라이즈, 토스, NAVER ENGINEERING DAY) category: 다음 목록 중 가장 적합한 카테고리를 하나만 선택하세요: {category_pool} summary: 해당 글을 잘 요약하는 한 문장을 한글로 서술하시오""".format(category_pool=", ".join(category_pool))),
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