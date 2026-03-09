"""
Gmail 이메일 모니터링
- Gmail API를 사용하여 새 이메일 감지
- 반납구역 관련 이메일 필터링
"""
import os
import base64
import logging
import time
import re
from typing import Callable, Optional, List
from datetime import datetime
from email.mime.text import MIMEText

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import sys
sys.path.append('..')
from config import config

logger = logging.getLogger(__name__)

# Gmail API 스코프
SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.modify",  # 읽음 처리용
]


class GmailMonitor:
    """Gmail 이메일 모니터링"""

    def __init__(self, on_email_callback: Optional[Callable] = None):
        """
        Args:
            on_email_callback: 이메일 수신 시 호출할 콜백 함수
                               (email_data: dict) -> None
        """
        self.on_email_callback = on_email_callback
        self.service = None
        self._last_check_time = None
        self._processed_ids = set()

    def authenticate(self) -> bool:
        """Gmail API 인증"""
        creds = None

        # 저장된 토큰 확인
        if os.path.exists(config.email.token_path):
            try:
                creds = Credentials.from_authorized_user_file(
                    config.email.token_path, SCOPES
                )
            except Exception as e:
                logger.warning(f"토큰 로드 실패: {e}")

        # 토큰 갱신 또는 새로 발급
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                except Exception as e:
                    logger.warning(f"토큰 갱신 실패: {e}")
                    creds = None

            if not creds:
                if not os.path.exists(config.email.credentials_path):
                    logger.error(
                        f"credentials.json 파일이 없습니다: {config.email.credentials_path}"
                    )
                    logger.error(
                        "Google Cloud Console에서 OAuth 2.0 클라이언트 ID를 생성하고 "
                        "credentials.json을 다운로드하세요."
                    )
                    return False

                flow = InstalledAppFlow.from_client_secrets_file(
                    config.email.credentials_path, SCOPES
                )
                creds = flow.run_local_server(port=0)

            # 토큰 저장
            with open(config.email.token_path, "w") as token:
                token.write(creds.to_json())

        try:
            self.service = build("gmail", "v1", credentials=creds)
            logger.info("Gmail API 인증 성공")
            return True
        except Exception as e:
            logger.error(f"Gmail API 초기화 실패: {e}")
            return False

    def _get_unread_emails(self) -> List[dict]:
        """읽지 않은 이메일 목록 가져오기"""
        if not self.service:
            logger.error("Gmail API가 초기화되지 않았습니다.")
            return []

        try:
            # 검색 쿼리 구성
            query_parts = ["is:unread"]

            if config.email.monitor_label != "INBOX":
                query_parts.append(f"label:{config.email.monitor_label}")

            # 허용된 발신자 필터
            if config.email.allowed_senders:
                sender_query = " OR ".join(
                    [f"from:{sender}" for sender in config.email.allowed_senders]
                )
                query_parts.append(f"({sender_query})")

            query = " ".join(query_parts)

            results = self.service.users().messages().list(
                userId="me",
                q=query,
                maxResults=20,
            ).execute()

            messages = results.get("messages", [])
            return messages

        except HttpError as e:
            logger.error(f"이메일 목록 가져오기 실패: {e}")
            return []

    def _get_email_content(self, message_id: str) -> Optional[dict]:
        """이메일 내용 가져오기"""
        try:
            message = self.service.users().messages().get(
                userId="me",
                id=message_id,
                format="full",
            ).execute()

            # 헤더에서 정보 추출
            headers = message.get("payload", {}).get("headers", [])
            header_dict = {h["name"].lower(): h["value"] for h in headers}

            subject = header_dict.get("subject", "")
            sender = header_dict.get("from", "")
            date = header_dict.get("date", "")

            # 본문 추출
            body = self._extract_body(message.get("payload", {}))

            return {
                "id": message_id,
                "subject": subject,
                "sender": sender,
                "date": date,
                "body": body,
                "snippet": message.get("snippet", ""),
            }

        except HttpError as e:
            logger.error(f"이메일 내용 가져오기 실패: {e}")
            return None

    def _extract_body(self, payload: dict) -> str:
        """이메일 본문 추출"""
        body = ""

        if "body" in payload and payload["body"].get("data"):
            body = base64.urlsafe_b64decode(payload["body"]["data"]).decode("utf-8")
        elif "parts" in payload:
            for part in payload["parts"]:
                mime_type = part.get("mimeType", "")
                if mime_type == "text/plain":
                    if part.get("body", {}).get("data"):
                        body = base64.urlsafe_b64decode(
                            part["body"]["data"]
                        ).decode("utf-8")
                        break
                elif mime_type.startswith("multipart/"):
                    body = self._extract_body(part)
                    if body:
                        break

        # HTML 태그 제거 (간단한 처리)
        body = re.sub(r"<[^>]+>", "", body)
        body = re.sub(r"\s+", " ", body).strip()

        return body

    def _mark_as_read(self, message_id: str):
        """이메일을 읽음으로 표시"""
        try:
            self.service.users().messages().modify(
                userId="me",
                id=message_id,
                body={"removeLabelIds": ["UNREAD"]},
            ).execute()
        except HttpError as e:
            logger.warning(f"읽음 표시 실패: {e}")

    def _is_return_zone_email(self, email_data: dict) -> bool:
        """반납구역 관련 이메일인지 확인"""
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

        text = f"{email_data.get('subject', '')} {email_data.get('body', '')}".lower()
        return any(kw in text for kw in keywords)

    def check_new_emails(self):
        """새 이메일 확인 및 처리"""
        unread_emails = self._get_unread_emails()

        for msg in unread_emails:
            msg_id = msg["id"]

            # 이미 처리한 이메일 스킵
            if msg_id in self._processed_ids:
                continue

            email_data = self._get_email_content(msg_id)
            if not email_data:
                continue

            # 반납구역 관련 이메일 필터
            if not self._is_return_zone_email(email_data):
                logger.debug(f"키워드 불일치, 무시: {email_data['subject']}")
                self._processed_ids.add(msg_id)
                continue

            logger.info(f"반납구역 요청 이메일 감지: {email_data['subject']}")
            logger.info(f"발신자: {email_data['sender']}")

            # 처리 완료 표시
            self._processed_ids.add(msg_id)
            self._mark_as_read(msg_id)

            # 콜백 호출
            if self.on_email_callback:
                self.on_email_callback({
                    "text": f"[제목] {email_data['subject']}\n\n{email_data['body']}",
                    "subject": email_data["subject"],
                    "sender": email_data["sender"],
                    "source": "email",
                    "email_id": msg_id,
                })

    def start_polling(self):
        """폴링 시작 (블로킹)"""
        logger.info(f"이메일 모니터링 시작 (간격: {config.email.poll_interval}초)")

        if not self.service and not self.authenticate():
            logger.error("Gmail 인증 실패, 모니터링 중단")
            return

        while True:
            try:
                self.check_new_emails()
            except Exception as e:
                logger.error(f"이메일 확인 중 오류: {e}")

            time.sleep(config.email.poll_interval)

    def start_polling_async(self):
        """비동기로 폴링 시작"""
        import threading

        thread = threading.Thread(target=self.start_polling, daemon=True)
        thread.start()
        logger.info("이메일 모니터링이 백그라운드에서 실행 중...")
        return thread


# 테스트용
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    def test_callback(email_data):
        print(f"[TEST] Email received: {email_data}")

    monitor = GmailMonitor(on_email_callback=test_callback)
    if monitor.authenticate():
        monitor.start_polling()
