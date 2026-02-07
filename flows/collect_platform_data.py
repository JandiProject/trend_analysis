from prefect import flow, task


@task(name="load_config")
def load_config():
    import os
    import json
    import base64
    base_path = os.path.dirname(os.path.dirname(__file__)) 
    config_path = os.path.join(base_path, "config", "platforms.json")
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)
    return [{"platform": key, "url": config[key], "decoded_platform": base64.b64decode(key).decode('utf-8')} for key in config]

@task(name="collect_platform_data", task_run_name="{config['decoded_platform']}_data_collection")
def collect_platform_data(config):
    import pandas as pd
    from parser.parser import RSSParser
    parser = RSSParser(config['platform'], config['url'])
    articles = parser.parse()
    articles = [article.__dict__ for article in articles]
    n = len(articles)
    df = pd.DataFrame(articles)
    return {'data': df, 'platform': config['decoded_platform'], 'num_articles': n}

@task(name="save_to_sqlite")
def save_to_sqlite(data):
    import os
    import sqlite3
    base_path = os.path.dirname(os.path.dirname(__file__))
    db_path = os.path.join(base_path, "blog_data", "articles.db")
    conn = sqlite3.connect(db_path)
    platform = data['platform']
    df = data['data']
    table_name = platform.replace(" ", "_").lower() + "_articles"
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()
    print(f"Saved {data['num_articles']} articles to table {table_name} in {db_path}")

@task(name="save_to_s3")
def save_to_s3(data):
    import boto3
    import io
    import pendulum
    import os
    s3_client = boto3.client('s3')

    # 2. 메모리 버퍼에 Parquet 쓰기
    buffer = io.BytesIO()
    data['data'].to_parquet(buffer, index=False, engine='pyarrow', compression='snappy')
    buffer.seek(0)

    now = pendulum.now('Asia/Seoul')
    partition_date = now.to_date_string()        # 2025-02-05
    timestamp = now.format('HHmmss')             # 203015 (시분초)

    object_key = f"blog-data/raw/dt={partition_date}/{data['platform']}_{timestamp}.parquet"
    bucket_name = os.getenv('S3_BUCKET_NAME')
    try:
        s3_client.upload_fileobj(buffer, bucket_name, object_key)
        print(f"S3 업로드 성공: s3://{bucket_name}/{object_key}")
    except Exception as e:
        print(f"S3 업로드 실패: {e}")
        raise
    


@flow(name="RSS Insight Pipeline", log_prints=True)
def rss_flow():
    configs = load_config()
    collected_data = collect_platform_data.map(configs)
    save_to_sqlite.map(collected_data)
    save_to_s3.map(collected_data)

if __name__ == "__main__":
    rss_flow()