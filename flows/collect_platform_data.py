from prefect import flow, task, get_run_logger
from prefect.task_runners import ThreadPoolTaskRunner



@task(name="load_config")
def load_config():
    """
    config/platforms.json 파일을 읽어서 플랫폼 이름과 RSS URL을 반환하는 함수
    """

    import os
    import json
    import base64

    base_path = os.path.dirname(os.path.dirname(__file__)) 
    config_path = os.path.join(base_path, "config", "platforms.json")
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    return [{"platform": key, "url": config[key], "decoded_platform": base64.b64decode(key).decode('utf-8')} for key in config]

def _undup_url(url_hash_list:list[str], db) -> list[str]:
    """
    수집된 url 중에서 이미 DB에 존재하는 url을 제거하여 반환
    """
    from models.db_models import ExternalPost

    existing_urls = db.query(ExternalPost.id).filter(ExternalPost.id.in_(url_hash_list)).all()
    existing_urls = [url[0] for url in existing_urls]

    return [url for url in url_hash_list if url not in existing_urls]

@task(name="collect_platform_data")
def collect_platform_data(config):
    """
    특정 플랫폼의 RSS 피드를 파싱하고 중복 제거 후 DataFrame으로 반환
    """
    import pandas as pd
    from utils.parser import RSSParser
    from services.service_db import DbSession
    db = DbSession()
    
    # RSS 파싱
    parser = RSSParser(config['platform'], config['url'])
    articles = parser.parse()

    # 중복 제거
    hash_list = [article.encoded_url for article in articles]
    undup_hash_list = _undup_url(hash_list, db)
    articles = [article for article in articles if article.encoded_url in undup_hash_list]

    # DataFrame 변환
    articles = [article.__dict__ for article in articles]
    n = len(articles)
    df = pd.DataFrame(articles)
    return {'data': df, 'platform': config['decoded_platform'], 'num_articles': n}

@task(name="save_to_sqlite")
def save_to_sqlite(data):
    """
    수집된 데이터를 로컬 SQLite 데이터베이스에 저장
    """
    import os
    import sqlite3
    base_path = "/opt/prefect/blog_data"
    db_path = os.path.join(base_path, "articles.db")
    conn = sqlite3.connect(db_path)
    platform = data['platform']
    df = data['data']
    table_name = platform.replace(" ", "_").lower() + "_articles"
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()
    logger = get_run_logger()
    logger.info(f"Saved {data['num_articles']} articles to table {table_name} in {db_path}")

@task(name="save_to_s3")
def save_to_s3(data):
    """
    수집된 데이터를 Parquet 형식으로 S3에 저장
    """

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
        logger = get_run_logger()
        logger.info(f"S3 업로드 성공: s3://{bucket_name}/{object_key}")
        return object_key
    except Exception as e:
        logger = get_run_logger()
        logger.error(f"S3 업로드 실패: {e}")
        raise

def _parse_rss_date(raw_date_str):
    """
    RSS 피드에서 제공하는 날짜 문자열을 Pendulum datetime 객체로 변환
    """

    import pendulum
    from email.utils import parsedate_to_datetime
    from datetime import datetime

    if not raw_date_str:
        return pendulum.now('Asia/Seoul')

    try:
        # 시도 1: 가장 확실한 email.utils 방식
        dt_native = parsedate_to_datetime(raw_date_str)
        return pendulum.instance(dt_native).in_timezone('Asia/Seoul')
    except Exception:
        try:
            # 시도 2: 일반적인 ISO 형식일 경우 Pendulum 자동 파싱
            return pendulum.parse(raw_date_str)
        except Exception as e:
            # 모두 실패 시 현재 시간으로 대체하고 로그 남김
            logger = get_run_logger()
            logger.warning(f"날짜 파싱 실패: {raw_date_str}. 현재 시간으로 대체합니다.")
            return pendulum.now('Asia/Seoul')

@task(name="insert_to_postgres")
def insert_to_postgres(data):
    """
    수집된 데이터를 PostgreSQL 데이터베이스에 저장
    """
    from services.service_db import DbSession
    from models.db_models import ExternalPost
    import pandas as pd
    from datetime import datetime
    from sqlalchemy.dialects.postgresql import insert

    db = DbSession()
    logger = get_run_logger()

    dfs = [d['data'] for d in data if d is not None]
    df = pd.concat(dfs, ignore_index=True)
    df = df.to_dict(orient='records')

    try:
        data_to_insert = []
        for row in df:
            logger = get_run_logger()
            logger.info(f"Inserting post with URL hash: {row['encoded_url']}")
            logger.info(f"Published at: {row['published_at']}, Created at: {row['created_at']}")
            post = ({
                'id': row['encoded_url'],
                'source': row['source'],
                'title': row['title'],
                'url': row['url'],
                'published_at': _parse_rss_date(row['published_at']),
                'collected_at': _parse_rss_date(row['created_at']) if row['created_at'] else datetime.now(),
            })
            data_to_insert.append(post)

        stmt = insert(ExternalPost).values(data_to_insert)
        stmt = stmt.on_conflict_do_nothing(index_elements=['id'])
        db.execute(stmt)

        logger.info(f"PostgreSQL 삽입 성공: {len(data_to_insert)} 개의 포스트 삽입 시도")
        db.commit()
    except Exception as e:
        logger.error(f"PostgreSQL 삽입 실패: {e}")
        db.rollback()
        raise
    finally:
        db.commit()
        db.close()


@flow(task_runner=ThreadPoolTaskRunner(max_workers=4), name="Data Collection Flow", log_prints=True) # type: ignore
def data_collection_flow():
    configs = load_config()
    collected_futures = collect_platform_data.map(configs)
    collected_results = []
    logger = get_run_logger()

    for future in collected_futures:
        res = future.result()
        if res and res['num_articles'] > 0:
            collected_results.append(res)
    logger.info(f"총 수집된 플랫폼 수: {len(collected_results)}")
    if len(collected_results) == 0:
        logger.warning("수집된 데이터가 없습니다.")
        return []
    #sqlite_futures = save_to_sqlite.map(collected_results)
    s3_futures = save_to_s3.map(collected_results)
    #[f.result() for f in sqlite_futures]
    obj_keys = [f.result() for f in s3_futures]

    insert_to_postgres(collected_results)
    
    return obj_keys


if __name__ == "__main__":
    data_collection_flow()