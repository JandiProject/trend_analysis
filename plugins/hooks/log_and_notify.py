from prefect import Flow, get_client
from prefect.client.schemas import FlowRun
from prefect.client.schemas.filters import LogFilter, LogFilterFlowRunId
from uuid import UUID
import boto3
import os
import logging


async def upload_logs_to_s3_and_notify(flow: Flow, flow_run: FlowRun, state):
    """Flow 완료 시 로그를 S3에 올리고 디스코드 알림을 보냄"""

    from discord_webhook import DiscordWebhook, DiscordEmbed
    from prefect.logging import get_run_logger

    flow_run_id = str(flow_run.id)
    file_name = f"logs/{flow_run_id}.log"
    
    # 1. Prefect API에서 로그 가져오기
    async with get_client() as client:
        logs = await client.read_logs(log_filter=LogFilter(flow_run_id=LogFilterFlowRunId(any_=[UUID(flow_run_id)])))
        full_log_text = "\n".join([f"[{l.timestamp}] {l.level}: {l.message}" for l in logs])

    # 2. S3에 업로드 (boto3)
    s3 = boto3.client('s3')
    try:
        s3.put_object(
            Bucket=os.getenv('S3_BUCKET_NAME'),
            Key=file_name,
            Body=full_log_text,
            ContentType='text/plain'
        )
        # S3 URL 생성 (Public 읽기가 허용된 경우 혹은 Pre-signed URL 사용 가능)
        s3_url = s3.generate_presigned_url(
            ClientMethod='get_object',
            Params={'Bucket': os.getenv('S3_BUCKET_NAME'), 'Key': file_name},
            ExpiresIn=3600 * 24 * 7  # URL 유효 기간 
        )
    except Exception as e:
        s3_url = f"S3 업로드 실패: {e}"

    # 3. 디스코드 웹훅 메시지 구성
    color = 3066993 if state.is_completed() else 15158528
    status_emoji = "✅" if state.is_completed() else "❌"

    payload = {
        "username": "Prefect S3 Logger",
        "embeds": [{
            "title": f"{status_emoji} Flow: {flow.name}",
            "description": f"**State:** {state.type}\n**Run Name:** {flow_run.name}",
            "color": color,
            "fields": [
                {
                    "name": "Full Logs (S3)",
                    "value": f"[🔗 로그 파일 보기]({s3_url})",
                    "inline": False
                },
                {
                    "name": "Prefect",
                    "value": f"[🔎 UI에서 보기](http://raspberrypi.taila2f7bd.ts.net:4200/runs/flow-run/{flow_run_id})",
                    "inline": False
                }
            ],
            "footer": {"text": f"Run ID: {flow_run_id}"}
        }]
    }
    webhook_url = os.getenv('DISCORD_WEBHOOK_URL') if os.getenv('DISCORD_WEBHOOK_URL') else ""
    try:
        webhook = DiscordWebhook(url=str(webhook_url), username="Prefect S3 Logger", content="")
        for embed in payload["embeds"]:
            discord_embed = DiscordEmbed(
                title=embed["title"],
                description=embed["description"],
                color=embed["color"]
            )
            for field in embed["fields"]:
                discord_embed.add_embed_field(name=field["name"], value=field["value"], inline=field["inline"])
            if "footer" in embed:
                discord_embed.set_footer(text=embed["footer"]["text"])
            webhook.add_embed(discord_embed)

        response = webhook.execute()
        if response.status_code != 200:
            logging.error(f"디스코드 웹훅 전송 실패: {response.status_code} - {response.content}")
    except Exception as e:
        logging.error(f"디스코드 웹훅 전송 중 오류 발생: {e}")