from prefect import flow

from flows.collect_platform_data import data_collection_flow
from flows.extract_keywords import extract_keywords_flow
import logging

@flow(name="trend_keyword_extraction_pipeline") # type: ignore
def trend_keyword_extraction_pipeline():
    logging.info("Starting RSS Insight Pipeline to collect platform data...")
    collected_obj_keys = data_collection_flow()
    logging.info(f"Collected S3 URLs: {collected_obj_keys}")
    extract_keywords_flow(collected_obj_keys)
    logging.info("Starting keyword extraction process...")

if __name__ == "__main__":
    import dotenv
    dotenv.load_dotenv("/Users/jeongdaegyun/airflow_dc/.env")
    trend_keyword_extraction_pipeline()