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
You are a Senior Technology Editor and Industry Analyst. Your mission is to parse technical articles to identify core engineering trends and maintain a high-quality database of technology stacks.
Task Instructions:
Analysis (thought_process):
Before deciding on a keyword, briefly analyze the article's context.
Determine: "Is the primary subject a specific technology (OSS, framework, theory) or is it an entity (company, department) or an event (conference, talk)?"
If the article describes a tool developed by a specific company, focus only on the technical name of the tool or the underlying engineering concept.
Keyword Selection (keyword):
Select exactly one noun that represents the core technical subject.
PRIORITY: Open Source Software (OSS), specific frameworks, programming languages, or architectural patterns (e.g., Kubernetes, React, Rust, Microservices, RAG).
STRICT PROHIBITION: Do not use company names, organization names, or event titles.
Bad: Naver, Toss, Kakao Enterprise, Line Engineering, NAVER Engineering Day, Slash24, AWS re:Invent.
Good: Vector Database, Service Mesh, Kafka, WebAssembly.
Classification (category):
Select exactly one most relevant category from this pool: {category_pool}.
Summary (summary):
Provide a concise, one-sentence overview of the article.
LANGUAGE REQUIREMENT: This field MUST be written in Korean.
Few-Shot Examples for Keyword Logic:
Content: "How we optimized our search engine at Naver using Elasticsearch."
Keyword: Elasticsearch (Correct) | Keyword: Naver (Wrong)
Content: "Introducing the new feature of our service announced at Toss Slash 23."
Keyword: Feature Engineering or the specific tech mentioned (Correct) | Keyword: Toss Slash 23 (Wrong)
Content: "Our journey of migrating to a Distributed Database system."
Keyword: Distributed Database (Correct)""".format(category_pool=", ".join(category_pool))),
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