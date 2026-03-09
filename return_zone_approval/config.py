"""
반납구역 승인 워크플로우 설정
"""
import os
from pathlib import Path
from dotenv import load_dotenv
from pydantic import BaseModel
from typing import Optional

# .env 파일 로드
load_dotenv()

class SlackConfig(BaseModel):
    """슬랙 설정"""
    bot_token: str = os.getenv("SLACK_BOT_TOKEN", "")
    app_token: str = os.getenv("SLACK_APP_TOKEN", "")  # Socket Mode용
    signing_secret: str = os.getenv("SLACK_SIGNING_SECRET", "")

    # 모니터링할 채널 (쉼표로 구분)
    monitor_channels: list[str] = os.getenv("SLACK_MONITOR_CHANNELS", "").split(",")

    # 승인 알림 보낼 채널 또는 사용자 ID
    approval_channel: str = os.getenv("SLACK_APPROVAL_CHANNEL", "")

    # 알림 받을 사용자 ID (멘션용)
    notify_user_id: str = os.getenv("SLACK_NOTIFY_USER_ID", "")


class EmailConfig(BaseModel):
    """이메일 (Gmail) 설정"""
    credentials_path: str = os.getenv("GMAIL_CREDENTIALS_PATH", "credentials.json")
    token_path: str = os.getenv("GMAIL_TOKEN_PATH", "token.json")

    # 모니터링할 이메일 라벨/필터
    monitor_label: str = os.getenv("GMAIL_MONITOR_LABEL", "INBOX")

    # 특정 발신자만 처리 (쉼표로 구분, 비어있으면 모두 처리)
    allowed_senders: list[str] = [
        s.strip() for s in os.getenv("GMAIL_ALLOWED_SENDERS", "").split(",") if s.strip()
    ]

    # 폴링 간격 (초)
    poll_interval: int = int(os.getenv("GMAIL_POLL_INTERVAL", "60"))


class AnthropicConfig(BaseModel):
    """Claude API 설정"""
    api_key: str = os.getenv("ANTHROPIC_API_KEY", "")
    model: str = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-20250514")


class AdminConfig(BaseModel):
    """BikeShare admin web 설정"""
    base_url: str = os.getenv("BIKESHARE_BASE_URL", "https://admin.example.com")
    username: str = os.getenv("BIKESHARE_USERNAME", "")
    password: str = os.getenv("BIKESHARE_PASSWORD", "")

    # 브라우저 headless 모드
    headless: bool = os.getenv("BIKESHARE_HEADLESS", "true").lower() == "true"

    # 스크린샷 저장 경로
    screenshot_dir: str = os.getenv("BIKESHARE_SCREENSHOT_DIR", "./screenshots")


class AppConfig(BaseModel):
    """앱 전체 설정"""
    slack: SlackConfig = SlackConfig()
    email: EmailConfig = EmailConfig()
    anthropic: AnthropicConfig = AnthropicConfig()
    admin_web: AdminConfig = AdminConfig()

    # 데이터베이스 경로 (TinyDB)
    db_path: str = os.getenv("DB_PATH", "./data/approval_queue.json")

    # 로그 레벨
    log_level: str = os.getenv("LOG_LEVEL", "INFO")


# 전역 설정 인스턴스
config = AppConfig()


# 프로젝트 루트 경로
PROJECT_ROOT = Path(__file__).parent


def validate_config() -> list[str]:
    """설정 유효성 검사. 누락된 필수 설정 목록 반환."""
    missing = []

    if not config.slack.bot_token:
        missing.append("SLACK_BOT_TOKEN")
    if not config.slack.app_token:
        missing.append("SLACK_APP_TOKEN")
    if not config.slack.approval_channel:
        missing.append("SLACK_APPROVAL_CHANNEL")
    if not config.anthropic.api_key:
        missing.append("ANTHROPIC_API_KEY")
    if not config.admin_web.username:
        missing.append("BIKESHARE_USERNAME")
    if not config.admin_web.password:
        missing.append("BIKESHARE_PASSWORD")

    return missing
