"""
슬랙 Interactive Message 처리
- 승인 요청 메시지 전송
- 버튼 클릭 응답 처리
"""
import json
import logging
from typing import Optional, Callable
from datetime import datetime
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

import sys
sys.path.append('..')
from config import config

logger = logging.getLogger(__name__)

# 슬랙 클라이언트
_client: Optional[WebClient] = None


def get_client() -> WebClient:
    """슬랙 클라이언트 싱글톤"""
    global _client
    if _client is None:
        _client = WebClient(token=config.slack.bot_token)
    return _client


def send_approval_request(
    request_id: str,
    zone_name: str,
    action_type: str,  # "exclude" or "reduce"
    reason: str,
    original_message: str,
    source: str,  # "slack" or "email"
    requester: str,
    details: Optional[dict] = None,
) -> Optional[str]:
    """
    승인 요청 메시지를 슬랙에 전송

    Returns:
        메시지 ts (성공 시) 또는 None (실패 시)
    """
    client = get_client()

    action_type_kr = "제외" if action_type == "exclude" else "축소"
    source_kr = "슬랙" if source == "slack" else "이메일"

    # Block Kit 메시지 구성
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"🚨 반납구역 {action_type_kr} 요청",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*요청 ID:*\n`{request_id}`",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*요청 출처:*\n{source_kr}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*대상 구역:*\n{zone_name}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*요청 유형:*\n{action_type_kr}",
                },
            ],
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*요청 사유:*\n{reason}",
            },
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*원본 메시지:*\n```{original_message[:500]}{'...' if len(original_message) > 500 else ''}```",
            },
        },
    ]

    # 상세 정보가 있으면 추가
    if details:
        details_text = "\n".join([f"• {k}: {v}" for k, v in details.items()])
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*AI 분석 결과:*\n{details_text}",
            },
        })

    # 버튼 추가
    blocks.extend([
        {"type": "divider"},
        {
            "type": "actions",
            "block_id": f"approval_actions_{request_id}",
            "elements": [
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "✅ 허용",
                        "emoji": True,
                    },
                    "style": "primary",
                    "action_id": "approve_request",
                    "value": json.dumps({
                        "request_id": request_id,
                        "action": "approve",
                    }),
                },
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "❌ 거부",
                        "emoji": True,
                    },
                    "style": "danger",
                    "action_id": "reject_request",
                    "value": json.dumps({
                        "request_id": request_id,
                        "action": "reject",
                    }),
                },
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "🔍 상세 보기",
                        "emoji": True,
                    },
                    "action_id": "view_details",
                    "value": json.dumps({
                        "request_id": request_id,
                        "action": "view",
                    }),
                },
            ],
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"요청자: {requester} | 수신 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                },
            ],
        },
    ])

    try:
        # 멘션 텍스트
        mention = ""
        if config.slack.notify_user_id:
            mention = f"<@{config.slack.notify_user_id}> "

        response = client.chat_postMessage(
            channel=config.slack.approval_channel,
            text=f"{mention}반납구역 {action_type_kr} 요청이 도착했습니다.",
            blocks=blocks,
        )

        logger.info(f"승인 요청 메시지 전송 완료: {response['ts']}")
        return response["ts"]

    except SlackApiError as e:
        logger.error(f"슬랙 메시지 전송 실패: {e.response['error']}")
        return None


def update_approval_message(
    channel: str,
    ts: str,
    request_id: str,
    status: str,  # "approved", "rejected", "processing", "completed", "failed"
    updated_by: str,
    note: Optional[str] = None,
):
    """
    승인 메시지 상태 업데이트 (버튼 제거 및 결과 표시)
    """
    client = get_client()

    status_emoji = {
        "approved": "✅",
        "rejected": "❌",
        "processing": "⏳",
        "completed": "🎉",
        "failed": "⚠️",
    }

    status_text = {
        "approved": "허용됨",
        "rejected": "거부됨",
        "processing": "처리 중...",
        "completed": "완료",
        "failed": "실패",
    }

    emoji = status_emoji.get(status, "❓")
    text = status_text.get(status, status)

    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"{emoji} *요청 ID `{request_id}`가 {text}되었습니다.*",
            },
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"처리자: <@{updated_by}> | 처리 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                },
            ],
        },
    ]

    if note:
        blocks.insert(1, {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*비고:* {note}",
            },
        })

    try:
        client.chat_update(
            channel=channel,
            ts=ts,
            text=f"요청 {request_id}: {text}",
            blocks=blocks,
        )
        logger.info(f"승인 메시지 업데이트 완료: {ts} -> {status}")

    except SlackApiError as e:
        logger.error(f"슬랙 메시지 업데이트 실패: {e.response['error']}")


def send_completion_notification(
    request_id: str,
    zone_name: str,
    action_type: str,
    success: bool,
    screenshot_url: Optional[str] = None,
    error_message: Optional[str] = None,
):
    """작업 완료 알림 전송"""
    client = get_client()

    action_type_kr = "제외" if action_type == "exclude" else "축소"

    if success:
        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"🎉 *반납구역 {action_type_kr} 완료*\n\n구역 `{zone_name}`이(가) 성공적으로 {action_type_kr}되었습니다.",
                },
            },
        ]

        if screenshot_url:
            blocks.append({
                "type": "image",
                "title": {
                    "type": "plain_text",
                    "text": "작업 완료 스크린샷",
                },
                "image_url": screenshot_url,
                "alt_text": "작업 완료 스크린샷",
            })
    else:
        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"⚠️ *반납구역 {action_type_kr} 실패*\n\n구역 `{zone_name}` 처리 중 오류가 발생했습니다.",
                },
            },
        ]

        if error_message:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*오류 내용:*\n```{error_message}```",
                },
            })

    blocks.append({
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": f"요청 ID: `{request_id}` | 완료 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            },
        ],
    })

    try:
        mention = ""
        if config.slack.notify_user_id:
            mention = f"<@{config.slack.notify_user_id}> "

        client.chat_postMessage(
            channel=config.slack.approval_channel,
            text=f"{mention}반납구역 {action_type_kr} {'완료' if success else '실패'}",
            blocks=blocks,
        )

    except SlackApiError as e:
        logger.error(f"완료 알림 전송 실패: {e.response['error']}")


def handle_approval_response(payload: dict, on_approve: Callable, on_reject: Callable):
    """
    버튼 클릭 응답 처리

    Args:
        payload: 슬랙에서 받은 interaction payload
        on_approve: 승인 시 호출할 콜백 (request_id) -> None
        on_reject: 거부 시 호출할 콜백 (request_id) -> None
    """
    action = payload.get("actions", [{}])[0]
    action_id = action.get("action_id", "")
    value = json.loads(action.get("value", "{}"))

    request_id = value.get("request_id")
    user_id = payload.get("user", {}).get("id", "unknown")
    channel = payload.get("channel", {}).get("id", "")
    message_ts = payload.get("message", {}).get("ts", "")

    if action_id == "approve_request":
        logger.info(f"요청 승인: {request_id} by {user_id}")
        update_approval_message(channel, message_ts, request_id, "processing", user_id)
        on_approve(request_id)

    elif action_id == "reject_request":
        logger.info(f"요청 거부: {request_id} by {user_id}")
        update_approval_message(channel, message_ts, request_id, "rejected", user_id)
        on_reject(request_id)

    elif action_id == "view_details":
        logger.info(f"상세 보기: {request_id}")
        # TODO: 모달로 상세 정보 표시
