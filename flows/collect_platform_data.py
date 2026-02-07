from prefect import flow, task
from datetime import timedelta


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

@task(name="collect_platform_data")
def collect_platform_data(config):
    # 수집이 끝나면 실행될 분석 로직
    print(f"분석 중: {config['platform']}")
    return "분석 완료"

# 2. 흐름 정의 (Airflow의 DAG 역할)
@flow(name="RSS Insight Pipeline", log_prints=True)
def rss_flow():
    configs = load_config()
    collect_platform_data.map(configs)
    

if __name__ == "__main__":
    rss_flow()