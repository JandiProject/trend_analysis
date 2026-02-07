# dags/extract_post.py
 
from airflow.sdk import DAG, task
import pendulum
import os
import json
from common.parser import RSSParser

with DAG(
    dag_id="extract_post",
    schedule="@weekly",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["example", "python"],
    max_active_tasks=3,
) as dag:

    @task(task_id="check_s3")
    def check_s3():
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        hook = S3Hook('datalake')
        bucket_name = 'jandi-post-bucket'
        exists = hook.check_for_bucket(bucket_name)

        if not exists:
            raise ValueError(f"S3 bucket {bucket_name} does not exist.")
        print(f"S3 bucket {bucket_name} exists.")
        return


    @task(task_id="load_config")
    def load_config():
        import os
        import json
        import base64
        base_path = os.path.dirname(os.path.dirname(__file__)) 
        config_path = os.path.join(base_path, "config", "platforms.json")
        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)
        return [{"platform": key, "url": config[key], "decoded_platform": base64.b64decode(key).decode('utf-8')} for key in config]

    @task(map_index_template="{{ task.op_kwargs['config']['decoded_platform'] }}", task_id="collect_platform_data", retries=2, retry_delay=pendulum.duration(minutes=5))
    def collect_platform_data(config):
        import pandas as pd
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        import base64

        # rss 파서 초기화 및 데이터 수집
        platform = config["platform"]
        url = config["url"]
        parser = RSSParser(platform, url)
        articles = parser.parse()
        articles_json = [article.__dict__ for article in articles]
        n = len(articles)
        del articles

        # 로컬에 json 파일로 저장
        base_path = os.path.dirname(os.path.dirname(__file__)) 
        decoded_platform = config["decoded_platform"]
        with open(os.path.join(base_path, "data", f"{decoded_platform}_articles.json"), "w", encoding="utf-8") as f:
            json.dump(articles_json, f, ensure_ascii=False, indent=4)
AWS_ACCESS_KEY_ID
        # S3에 Parquet 파일로 저장
        df = pd.DataFrame(articles_json)
        articles_parquet = df.to_parquet()

        hook = S3Hook('datalake')
        bucket_name = 'jandi-post-bucket'

        now = pendulum.now('Asia/Seoul')
        partition_date = now.to_date_string()        # 2025-02-05
        timestamp = now.format('HHmmss')             # 203015 (시분초)

        object_key = f"blog-data/raw/dt={partition_date}/{platform}_{timestamp}.parquet"
        hook.load_bytes(
            bytes_data=articles_parquet,
            key=object_key,
            bucket_name=bucket_name,
            replace=True
        )

        print(f"Collected {n} articles from {platform}")
        return
    
    configs = load_config()
    check_s3() >> configs # pyright: ignore[reportUnusedExpression]
    collect_platform_data.expand(config=configs)