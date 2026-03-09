"""
승인 워크플로우 관리
- 요청 대기열 관리
- 승인/거부 처리
- 자동화 실행 트리거
"""
import uuid
import logging
from datetime import datetime
from enum import Enum
from typing import Optional, List, Callable
from dataclasses import dataclass, field, asdict
from pathlib import Path

from tinydb import TinyDB, Query

import sys
sys.path.append('..')
from config import config
from parser.ai_parser import ParsedRequest
from automation.admin_web import AdminWebAutomation, AutomationResult

logger = logging.getLogger(__name__)


class ApprovalStatus(Enum):
    """승인 상태"""
    PENDING = "pending"  # 대기 중
    APPROVED = "approved"  # 승인됨
    REJECTED = "rejected"  # 거부됨
    PROCESSING = "processing"  # 처리 중
    COMPLETED = "completed"  # 완료
    FAILED = "failed"  # 실패


@dataclass
class ApprovalRequest:
    """승인 요청"""
    id: str
    source: str  # "slack" or "email"
    requester: str  # 요청자 (슬랙 user ID 또는 이메일 주소)
    original_message: str  # 원본 메시지
    parsed: dict  # ParsedRequest를 dict로 변환한 것
    status: str = ApprovalStatus.PENDING.value
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat())
    slack_message_ts: Optional[str] = None  # 슬랙 메시지 타임스탬프
    approved_by: Optional[str] = None  # 승인/거부자
    result: Optional[dict] = None  # 자동화 실행 결과
    notes: Optional[str] = None  # 메모

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "ApprovalRequest":
        return cls(**data)


class ApprovalWorkflow:
    """승인 워크플로우 관리자"""

    def __init__(
        self,
        on_approval_complete: Optional[Callable[[ApprovalRequest, AutomationResult], None]] = None
    ):
        """
        Args:
            on_approval_complete: 승인 처리 완료 시 콜백
        """
        # 데이터베이스 초기화
        db_path = Path(config.db_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db = TinyDB(str(db_path))
        self.requests = self.db.table("requests")

        self.on_approval_complete = on_approval_complete
        self.automation = AdminWebAutomation()

    def create_request(
        self,
        source: str,
        requester: str,
        original_message: str,
        parsed: ParsedRequest,
    ) -> ApprovalRequest:
        """새 승인 요청 생성"""
        request_id = str(uuid.uuid4())[:8]  # 짧은 ID

        request = ApprovalRequest(
            id=request_id,
            source=source,
            requester=requester,
            original_message=original_message,
            parsed=parsed.to_dict(),
        )

        # DB에 저장
        self.requests.insert(request.to_dict())
        logger.info(f"새 요청 생성: {request_id}")

        return request

    def get_request(self, request_id: str) -> Optional[ApprovalRequest]:
        """요청 조회"""
        RequestQuery = Query()
        result = self.requests.search(RequestQuery.id == request_id)

        if result:
            return ApprovalRequest.from_dict(result[0])
        return None

    def get_pending_requests(self) -> List[ApprovalRequest]:
        """대기 중인 요청 목록"""
        RequestQuery = Query()
        results = self.requests.search(
            RequestQuery.status == ApprovalStatus.PENDING.value
        )
        return [ApprovalRequest.from_dict(r) for r in results]

    def update_request(self, request_id: str, **updates) -> bool:
        """요청 업데이트"""
        RequestQuery = Query()
        updates["updated_at"] = datetime.now().isoformat()

        count = self.requests.update(updates, RequestQuery.id == request_id)
        return count > 0

    def set_slack_message_ts(self, request_id: str, message_ts: str):
        """슬랙 메시지 타임스탬프 저장"""
        self.update_request(request_id, slack_message_ts=message_ts)

    def approve_request(self, request_id: str, approved_by: str) -> Optional[AutomationResult]:
        """
        요청 승인 및 자동화 실행

        Args:
            request_id: 요청 ID
            approved_by: 승인자 (슬랙 user ID)

        Returns:
            AutomationResult 또는 None
        """
        request = self.get_request(request_id)
        if not request:
            logger.error(f"요청을 찾을 수 없음: {request_id}")
            return None

        if request.status != ApprovalStatus.PENDING.value:
            logger.warning(f"이미 처리된 요청: {request_id} ({request.status})")
            return None

        # 상태 업데이트: 처리 중
        self.update_request(
            request_id,
            status=ApprovalStatus.PROCESSING.value,
            approved_by=approved_by,
        )

        logger.info(f"요청 승인: {request_id} by {approved_by}")

        # 자동화 실행
        parsed = request.parsed
        result = self.automation.execute_action(
            action_type=parsed.get("action_type", "exclude"),
            zone_name=parsed.get("zone_name", ""),
            reason=parsed.get("reason", ""),
        )

        # 결과에 따라 상태 업데이트
        final_status = ApprovalStatus.COMPLETED if result.success else ApprovalStatus.FAILED

        self.update_request(
            request_id,
            status=final_status.value,
            result=asdict(result) if hasattr(result, '__dataclass_fields__') else {
                "success": result.success,
                "message": result.message,
                "screenshot_path": result.screenshot_path,
                "error": result.error,
            },
        )

        logger.info(f"요청 처리 완료: {request_id} -> {final_status.value}")

        # 콜백 호출
        if self.on_approval_complete:
            updated_request = self.get_request(request_id)
            self.on_approval_complete(updated_request, result)

        return result

    def reject_request(self, request_id: str, rejected_by: str, reason: Optional[str] = None):
        """요청 거부"""
        request = self.get_request(request_id)
        if not request:
            logger.error(f"요청을 찾을 수 없음: {request_id}")
            return

        if request.status != ApprovalStatus.PENDING.value:
            logger.warning(f"이미 처리된 요청: {request_id} ({request.status})")
            return

        self.update_request(
            request_id,
            status=ApprovalStatus.REJECTED.value,
            approved_by=rejected_by,  # 거부자도 같은 필드 사용
            notes=reason,
        )

        logger.info(f"요청 거부: {request_id} by {rejected_by}")

    def get_request_summary(self, request_id: str) -> Optional[str]:
        """요청 요약 정보 반환"""
        request = self.get_request(request_id)
        if not request:
            return None

        parsed = request.parsed
        action_type_kr = {
            "exclude": "제외",
            "reduce": "축소",
            "expand": "확대",
            "add": "추가",
        }

        status_kr = {
            "pending": "대기 중",
            "approved": "승인됨",
            "rejected": "거부됨",
            "processing": "처리 중",
            "completed": "완료",
            "failed": "실패",
        }

        lines = [
            f"요청 ID: {request.id}",
            f"상태: {status_kr.get(request.status, request.status)}",
            f"구역: {parsed.get('zone_name', '알 수 없음')}",
            f"유형: {action_type_kr.get(parsed.get('action_type'), parsed.get('action_type'))}",
            f"사유: {parsed.get('reason', '없음')}",
            f"요청자: {request.requester}",
            f"출처: {request.source}",
            f"생성: {request.created_at}",
        ]

        if request.approved_by:
            lines.append(f"처리자: {request.approved_by}")

        return "\n".join(lines)


# 테스트용
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    workflow = ApprovalWorkflow()

    # 테스트 요청 생성
    from parser.ai_parser import ParsedRequest

    test_parsed = ParsedRequest(
        action_type="exclude",
        zone_name="동탄역 앞",
        region="화성시 동탄",
        reason="민원 발생",
        time_range=None,
        duration=None,
        coordinates=None,
        urgency="high",
        confidence=0.85,
        raw_text="동탄역 앞 반납존 좀 빼주세요",
        notes=None,
    )

    request = workflow.create_request(
        source="slack",
        requester="U12345678",
        original_message="동탄역 앞 반납존 좀 빼주세요",
        parsed=test_parsed,
    )

    print(f"생성된 요청:\n{workflow.get_request_summary(request.id)}")
