"""
슬랙 메시지 리스너
- 특정 채널에서 반납구역 관련 메시지를 감지
"""
import logging
import re
from typing import Callable, Optional
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

import sys
sys.path.append('..')
from config import config

logger = logging.getLogger(__name__)


class SlackListener:
    """슬랙 메시지 리스너"""

    def __init__(self, on_message_callback: Optional[Callable] = None):
        """
        Args:
            on_message_callback: 메시지 수신 시 호출할 콜백 함수
                                 (message: dict, channel: str, user: str) -> None
        """
        self.app = App(
            token=config.slack.bot_token,
            signing_secret=config.slack.signing_secret,
        )
        self.on_message_callback = on_message_callback
        self._setup_handlers()

    def _setup_handlers(self):
        """이벤트 핸들러 설정"""

        @self.app.event("message")
        def handle_message(event, say, client):
            """메시지 이벤트 핸들러"""
            # 봇 자신의 메시지는 무시
            if event.get("bot_id"):
                return

            channel = event.get("channel", "")
            user = event.get("user", "")
            text = event.get("text", "")
            ts = event.get("ts", "")

            # 모니터링 채널 확인
            if config.slack.monitor_channels and channel not in config.slack.monitor_channels:
                return

            # 반납구역 관련 키워드 필터
            if not self._is_return_zone_message(text):
                logger.debug(f"키워드 불일치, 무시: {text[:50]}...")
                return

            logger.info(f"반납구역 요청 감지 - Channel: {channel}, User: {user}")
            logger.info(f"메시지: {text}")

            # 콜백 호출
            if self.on_message_callback:
                self.on_message_callback(
                    message={
                        "text": text,
                        "ts": ts,
                        "channel": channel,
                        "user": user,
                        "source": "slack",
                    },
                    channel=channel,
                    user=user,
                )

    def _is_return_zone_message(self, text: str) -> bool:
        """반납구역 관련 메시지인지 확인"""
        keywords = [
            "반납구역",
            "반납 구역",
            "반납존",
            "반납 존",
            "제외",
            "축소",
            "확대",
            "추가",
            "삭제",
            "zone",
            "return zone",
        ]

        text_lower = text.lower()
        return any(kw in text_lower for kw in keywords)

    def start(self):
        """슬랙 봇 시작 (Socket Mode)"""
        logger.info("슬랙 봇 시작...")

        handler = SocketModeHandler(
            app=self.app,
            app_token=config.slack.app_token,
        )
        handler.start()

    def start_async(self):
        """비동기로 슬랙 봇 시작"""
        import threading

        thread = threading.Thread(target=self.start, daemon=True)
        thread.start()
        logger.info("슬랙 봇이 백그라운드에서 실행 중...")
        return thread


# 테스트용
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    def test_callback(message, channel, user):
        print(f"[TEST] Message received: {message}")

    listener = SlackListener(on_message_callback=test_callback)
    listener.start()
