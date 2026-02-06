# 의존성 추가를 위해 기존 이미지에서 Apache Airflow 3.0.1 버전을 기반으로 새로운 이미지를 생성합니다.
FROM apache/airflow:3.0.1

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt