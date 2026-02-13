from prefect import flow, task, get_run_logger, unmapped
from prefect.task_runners import ThreadPoolTaskRunner

@task(name="load_unanalized_data")
def load_unanalized_data():
    """
    db에서 분석되지 않은 데이터를 로드하는 함수
    """
    from services.service_db import DbSession
    from models.db_models import ExternalPost
    db = DbSession()
    existing_urls = db.query(ExternalPost.id).filter(ExternalPost.is_analyzed == False).all()
    existing_urls = [url[0] for url in existing_urls]
    return existing_urls

@task(name="load_data_from_s3")
def load_data_from_s3(obj_key: str, target_ids: list[str]):

    import boto3
    import pandas as pd
    from io import BytesIO

    # --- Configuration ---
    BUCKET_NAME = 'jandi-post-bucket'
    OBJECT_KEY = obj_key
    logger = get_run_logger()

    s3_client = boto3.client('s3')
    s3_response_object = s3_client.get_object(Bucket=BUCKET_NAME, Key=OBJECT_KEY)
    df_bytes = s3_response_object['Body'].read()
    df = pd.read_parquet(BytesIO(df_bytes))
    logger.info(f"Loaded DataFrame from S3 with columns: {df.columns.tolist()} and {len(df)} rows.")
    res = df[["encoded_url", "rss_content"]].to_dict(orient='records')
    res = [item for item in res if item['encoded_url'] in target_ids]
    return res

@task(name="extract_keywords", retries=5, retry_delay_seconds=[2, 4, 8, 16, 32])
def extract_keywords(s3_data:dict):
    logger = get_run_logger()

    from plugins.preprocessor.raw_text_preprocessor import process_text
    from plugins.extractor.keyword_extractor import generate
    import json

    result = generate(process_text(s3_data['rss_content']))

    logger.info(f"Extracted keywords for {len(result)} articles.")
    result = json.loads(result) | {'id': s3_data['encoded_url']}
    logger.info(result)
    return result

@task(name="upload_results_to_db")
def upload_results_to_db(results):
    from services.service_db import DbSession
    from models.db_models import ExternalPost, Keyword, PostKeywordMapping
    from sqlalchemy.dialects.postgresql import insert

    db = DbSession()
    logger = get_run_logger()

    keywords = [result['keywords'] for result in results if 'keywords' in result]
    keywords = list(set(sum(keywords, [])))
    keyword_to_id = {}

    try:

        # 키워드 테이블에 키워드 삽입
        keyword_data = [{'keyword': kw} for kw in keywords]
        stmt = insert(Keyword).values(keyword_data)
        stmt = stmt.on_conflict_do_nothing(index_elements=['keyword'])
        db.execute(stmt)
        db.commit()
        logger.info(f"PostgreSQL 키워드 삽입 성공: {len(keyword_data)} 개의 키워드 삽입 시도")

        # 키워드-id 매핑
        existing_keywords = db.query(Keyword).filter(Keyword.keyword.in_(keywords)).all()
        for kw in existing_keywords:
            keyword_to_id[kw.keyword] = kw.id
        print(keyword_to_id)

        # is_analyzed, category 업데이트
        data_to_update = []
        for result in results:
            if 'category' in result:
                data_to_update.append({
                    'id': result['id'],
                    'category': result['category'],
                    'is_analyzed': True,
                })
        db.bulk_update_mappings(ExternalPost, data_to_update) # type: ignore
        logger.info(f"PostgreSQL 포스트 업데이트 성공: {len(data_to_update)} 개의 포스트 업데이트 시도")

        # Post-Keyword 매핑 삽입
        post_keyword_mappings = []
        for result in results:
            if 'keywords' in result:
                for kw in result['keywords']:
                    if kw in keyword_to_id:
                        post_keyword_mappings.append({
                            'post_id': result['id'],
                            'keyword_id': keyword_to_id[kw],
                        })
        stmt = insert(PostKeywordMapping).values(post_keyword_mappings)
        stmt = stmt.on_conflict_do_nothing()
        db.execute(stmt)
        db.commit()
    except Exception as e:
        logger.error(f"PostgreSQL 키워드 삽입 실패: {e}")
        db.rollback()
        raise
    

@task(name="backup_results_to_s3")
def backup_results_to_s3(results):
    pass

@flow(name="Extract keywords flow", task_runner=ThreadPoolTaskRunner(max_workers=4), log_prints=True) # type: ignore
def extract_keywords_flow(collected_obj_keys: list[str]):
    logger = get_run_logger()

    logger.info("Starting keyword extraction process...")
    if len(collected_obj_keys) == 0:
        logger.warning("No collected object keys provided for keyword extraction.")
        return
    
    unanalized_data_ids = load_unanalized_data()
    s3_data_futures = load_data_from_s3.map(collected_obj_keys, target_ids = unmapped(unanalized_data_ids))
    s3_data = [f.result() for f in s3_data_futures]
    s3_data = sum(s3_data, [])
    logger.info(f"Loaded data from {len(s3_data)} S3 objects.")
    if len(s3_data) == 0:
        logger.warning("No unanalized data found in the loaded S3 objects.")
        return

    results = extract_keywords.map(s3_data)
    results = [r.result() for r in results]
    
    logger.info(f"Extracted keywords results: {len(results)} items.")

    upload_results_to_db(results)
    backup_results_to_s3(results)
    logger.info("Keyword extraction process completed.")