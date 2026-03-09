#!/usr/bin/env python3
"""
반납구역 승인 워크플로우 시스템
메인 실행 파일

실행 방법:
    python main.py

환경 변수:
    .env 파일에 필요한 설정을 입력하세요.
    .env.example 파일을 참고하세요.
"""
import logging
import sys
import signal
import json
from typing import Optional

from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

from config import config, validate_config
from slack_bot.listener import SlackListener
from slack_bot.interactive import (
    send_approval_request,
    update_approval_message,
    send_completion_notification,
)
from email_monitor.gmail import GmailMonitor
from parser.ai_parser import parse_return_zone_request, format_parsed_request_for_display
from workflow.approval import ApprovalWorkflow, ApprovalRequest, AutomationResult

# 로깅 설정
logging.basicConfig(
    level=getattr(logging, config.log_level),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("approval_workflow.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


class ReturnZoneApprovalSystem:
    """반납구역 승인 워크플로우 시스템"""

    def __init__(self):
        self.workflow: Optional[ApprovalWorkflow] = None
        self.slack_app: Optional[App] = None
        self.slack_handler: Optional[SocketModeHandler] = None
        self.email_monitor: Optional[GmailMonitor] = None

    def setup(self) -> bool:
        """시스템 초기화"""
        # 설정 검증
        missing = validate_config()
        if missing:
            logger.error(f"필수 설정 누락: {', '.join(missing)}")
            logger.error(".env 파일을 확인하세요. .env.example을 참고하세요.")
            return False

        # 워크플로우 초기화
        self.workflow = ApprovalWorkflow(
            on_approval_complete=self._on_approval_complete
        )

        # 슬랙 앱 초기화
        self._setup_slack_app()

        # 이메일 모니터 초기화
        self.email_monitor = GmailMonitor(
            on_email_callback=self._on_message_received
        )

        logger.info("시스템 초기화 완료")
        return True

    def _setup_slack_app(self):
        """슬랙 앱 설정"""
        self.slack_app = App(
            token=config.slack.bot_token,
            signing_secret=config.slack.signing_secret,
        )

        # 메시지 이벤트 핸들러
        @self.slack_app.event("message")
        def handle_message(event, say, client):
            # 봇 자신의 메시지는 무시
            if event.get("bot_id"):
                return

            channel = event.get("channel", "")
            user = event.get("user", "")
            text = event.get("text", "")

            # 모니터링 채널 확인
            if config.slack.monitor_channels:
                if channel not in config.slack.monitor_channels:
                    return

            # 반납구역 관련 키워드 필터
            if not self._is_return_zone_message(text):
                return

            logger.info(f"슬랙 메시지 감지: {text[:50]}...")

            self._on_message_received({
                "text": text,
                "channel": channel,
                "user": user,
                "source": "slack",
            })

        # 버튼 클릭 핸들러: 승인
        @self.slack_app.action("approve_request")
        def handle_approve(ack, body, client):
            ack()

            action = body.get("actions", [{}])[0]
            value = json.loads(action.get("value", "{}"))
            request_id = value.get("request_id")
            user_id = body.get("user", {}).get("id", "unknown")
            channel = body.get("channel", {}).get("id", "")
            message_ts = body.get("message", {}).get("ts", "")

            logger.info(f"승인 버튼 클릭: {request_id} by {user_id}")

            # 메시지 업데이트: 처리 중
            update_approval_message(
                channel, message_ts, request_id, "processing", user_id
            )

            # 승인 처리 (자동화 실행)
            result = self.workflow.approve_request(request_id, user_id)

            # 결과에 따라 메시지 업데이트
            if result:
                status = "completed" if result.success else "failed"
                update_approval_message(
                    channel, message_ts, request_id, status, user_id,
                    note=result.message
                )

        # 버튼 클릭 핸들러: 거부
        @self.slack_app.action("reject_request")
        def handle_reject(ack, body, client):
            ack()

            action = body.get("actions", [{}])[0]
            value = json.loads(action.get("value", "{}"))
            request_id = value.get("request_id")
            user_id = body.get("user", {}).get("id", "unknown")
            channel = body.get("channel", {}).get("id", "")
            message_ts = body.get("message", {}).get("ts", "")

            logger.info(f"거부 버튼 클릭: {request_id} by {user_id}")

            # 거부 처리
            self.workflow.reject_request(request_id, user_id)

            # 메시지 업데이트
            update_approval_message(
                channel, message_ts, request_id, "rejected", user_id
            )

        # 버튼 클릭 핸들러: 상세 보기
        @self.slack_app.action("view_details")
        def handle_view_details(ack, body, client):
            ack()

            action = body.get("actions", [{}])[0]
            value = json.loads(action.get("value", "{}"))
            request_id = value.get("request_id")

            summary = self.workflow.get_request_summary(request_id)
            if summary:
                client.chat_postEphemeral(
                    channel=body.get("channel", {}).get("id", ""),
                    user=body.get("user", {}).get("id", ""),
                    text=f"```{summary}```",
                )

    def _is_return_zone_message(self, text: str) -> bool:
        """반납구역 관련 메시지인지 확인"""
        keywords = [
            "반납구역", "반납 구역", "반납존", "반납 존",
            "제외", "축소", "확대", "추가", "삭제",
            "zone", "return zone",
        ]
        text_lower = text.lower()
        return any(kw in text_lower for kw in keywords)

    def _on_message_received(self, message: dict):
        """메시지 수신 처리"""
        text = message.get("text", "")
        source = message.get("source", "unknown")
        requester = message.get("user", message.get("sender", "unknown"))

        logger.info(f"메시지 처리 시작: {source} from {requester}")

        # AI 파싱
        parsed = parse_return_zone_request(text)
        if not parsed:
            logger.warning("메시지 파싱 실패")
            return

        logger.info(f"파싱 결과:\n{format_parsed_request_for_display(parsed)}")

        # 신뢰도가 너무 낮으면 스킵
        if parsed.confidence < 0.5:
            logger.warning(f"신뢰도가 너무 낮음: {parsed.confidence}")
            return

        # 승인 요청 생성
        request = self.workflow.create_request(
            source=source,
            requester=requester,
            original_message=text,
            parsed=parsed,
        )

        # 슬랙으로 승인 요청 전송
        message_ts = send_approval_request(
            request_id=request.id,
            zone_name=parsed.zone_name,
            action_type=parsed.action_type,
            reason=parsed.reason,
            original_message=text,
            source=source,
            requester=requester,
            details={
                "지역": parsed.region or "미상",
                "긴급도": parsed.urgency,
                "신뢰도": f"{parsed.confidence * 100:.0f}%",
            },
        )

        if message_ts:
            self.workflow.set_slack_message_ts(request.id, message_ts)
            logger.info(f"승인 요청 전송 완료: {request.id}")

    def _on_approval_complete(self, request: ApprovalRequest, result: AutomationResult):
        """승인 처리 완료 콜백"""
        parsed = request.parsed

        send_completion_notification(
            request_id=request.id,
            zone_name=parsed.get("zone_name", "알 수 없음"),
            action_type=parsed.get("action_type", "exclude"),
            success=result.success,
            screenshot_url=None,  # 로컬 파일이라 URL로 공유 어려움
            error_message=result.error if not result.success else None,
        )

    def run(self):
        """시스템 실행"""
        if not self.setup():
            logger.error("시스템 초기화 실패")
            return

        # 종료 시그널 핸들러
        def signal_handler(sig, frame):
            logger.info("종료 신호 수신, 시스템 종료 중...")
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        logger.info("=" * 50)
        logger.info("반납구역 승인 워크플로우 시스템 시작")
        logger.info("=" * 50)

        # 이메일 모니터링 시작 (백그라운드)
        if self.email_monitor:
            try:
                if self.email_monitor.authenticate():
                    self.email_monitor.start_polling_async()
                    logger.info("이메일 모니터링 시작됨")
                else:
                    logger.warning("이메일 모니터링 비활성화 (인증 실패)")
            except Exception as e:
                logger.warning(f"이메일 모니터링 비활성화: {e}")

        # 슬랙 봇 시작 (블로킹)
        logger.info("슬랙 봇 시작...")
        self.slack_handler = SocketModeHandler(
            app=self.slack_app,
            app_token=config.slack.app_token,
        )
        self.slack_handler.start()


def main():
    """메인 함수"""
    print("""
╔══════════════════════════════════════════════════════════════╗
║          반납구역 승인 워크플로우 시스템 v1.0                ║
║                                                              ║
║  기능:                                                       ║
║  - 슬랙/이메일에서 반납구역 요청 자동 감지                   ║
║  - AI를 이용한 비정형 메시지 파싱                            ║
║  - 슬랙 버튼으로 승인/거부                                   ║
║  - 승인 시 admin web 자동화 실행                                ║
╚══════════════════════════════════════════════════════════════╝
    """)

    system = ReturnZoneApprovalSystem()
    system.run()


if __name__ == "__main__":
    main()
