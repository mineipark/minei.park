# 반납구역 승인 워크플로우 시스템

슬랙/이메일로 들어오는 반납구역 제외/축소 요청을 자동으로 감지하고, 승인 워크플로우를 통해 admin web 작업을 자동화하는 시스템입니다.

## 시스템 흐름

```
┌─────────────────┐     ┌─────────────────┐
│   슬랙 채널     │     │     이메일      │
│  (요청 수신)    │     │   (Gmail)       │
└────────┬────────┘     └────────┬────────┘
         │                       │
         └───────────┬───────────┘
                     ▼
         ┌───────────────────────┐
         │   AI 메시지 파서      │
         │ (비정형 → 정형 변환)  │
         │     (Claude API)      │
         └───────────┬───────────┘
                     ▼
         ┌───────────────────────┐
         │   슬랙 승인 요청      │
         │  [허용] [거부] 버튼   │
         └───────────┬───────────┘
                     ▼
         ┌───────────────────────┐
         │  admin web 자동화 실행   │
         │    (Playwright)       │
         └───────────────────────┘
```

## 주요 기능

- **다채널 요청 수신**: 슬랙 채널과 이메일(Gmail) 모니터링
- **AI 메시지 파싱**: 비정형 메시지를 Claude API로 분석하여 구조화
- **슬랙 승인 워크플로우**: 버튼 클릭으로 승인/거부
- **admin web 자동화**: Playwright로 BikeShare admin web 자동 조작
- **이력 관리**: TinyDB로 모든 요청 이력 저장

## 설치

### 1. 의존성 설치

```bash
cd return_zone_approval
pip install -r requirements.txt
playwright install chromium
```

### 2. 환경 설정

```bash
cp .env.example .env
# .env 파일을 편집하여 필요한 값 입력
```

## 설정 가이드

### Slack 앱 설정

1. [Slack API](https://api.slack.com/apps)에서 새 앱 생성
2. **OAuth & Permissions**에서 Bot Token Scopes 추가:
   - `channels:history` - 채널 메시지 읽기
   - `chat:write` - 메시지 전송
   - `users:read` - 사용자 정보 조회
3. **Socket Mode** 활성화
4. **Event Subscriptions** 활성화 후 `message.channels` 이벤트 구독
5. 워크스페이스에 앱 설치
6. 토큰 복사:
   - Bot User OAuth Token → `SLACK_BOT_TOKEN`
   - App-Level Token (connections:write) → `SLACK_APP_TOKEN`
   - Signing Secret → `SLACK_SIGNING_SECRET`

### Gmail API 설정

1. [Google Cloud Console](https://console.cloud.google.com/)에서 프로젝트 생성
2. Gmail API 활성화
3. OAuth 2.0 클라이언트 ID 생성 (데스크톱 앱)
4. `credentials.json` 다운로드 후 프로젝트 폴더에 저장
5. 최초 실행 시 브라우저에서 인증 진행

### Anthropic API 설정

1. [Anthropic Console](https://console.anthropic.com/)에서 API 키 발급
2. `ANTHROPIC_API_KEY` 환경 변수 설정

### admin web 자동화 설정

`automation/admin_web.py` 파일에서 실제 BikeShare admin web UI에 맞게 선택자(selector)를 수정해야 합니다.

```python
# 예시: 로그인 선택자 수정
username_selector = "input[id='your-actual-id']"
password_selector = "input[name='your-actual-name']"
```

## 실행

```bash
python main.py
```

## 사용 예시

### 슬랙에서 요청

모니터링 채널에 다음과 같은 메시지를 보내면:

```
동탄역 앞 반납존 좀 빼주세요. 민원 들어왔어요
```

시스템이 자동으로:
1. 메시지를 감지
2. AI가 파싱 (구역: 동탄역 앞, 유형: 제외, 사유: 민원)
3. 지정된 채널로 승인 요청 전송

```
🚨 반납구역 제외 요청

요청 ID: a1b2c3d4
요청 출처: 슬랙
대상 구역: 동탄역 앞
요청 유형: 제외
요청 사유: 민원 발생

[✅ 허용] [❌ 거부] [🔍 상세 보기]
```

### 승인 처리

"허용" 버튼 클릭 시:
1. admin web에 자동 로그인
2. 해당 구역 검색
3. 제외 처리 실행
4. 완료 알림 전송

## 프로젝트 구조

```
return_zone_approval/
├── main.py                 # 메인 실행 파일
├── config.py               # 설정 관리
├── requirements.txt        # Python 의존성
├── .env.example            # 환경 변수 템플릿
├── README.md               # 이 문서
│
├── slack_bot/              # 슬랙 봇 모듈
│   ├── __init__.py
│   ├── listener.py         # 메시지 리스너
│   └── interactive.py      # Interactive Message 처리
│
├── email_monitor/          # 이메일 모니터링 모듈
│   ├── __init__.py
│   └── gmail.py            # Gmail API 연동
│
├── parser/                 # 메시지 파서 모듈
│   ├── __init__.py
│   └── ai_parser.py        # Claude API 파싱
│
├── automation/             # admin web 자동화 모듈
│   ├── __init__.py
│   └── admin_web.py       # Playwright 자동화
│
└── workflow/               # 워크플로우 모듈
    ├── __init__.py
    └── approval.py         # 승인 로직 및 DB 관리
```

## 환경 변수

| 변수명 | 설명 | 필수 |
|--------|------|------|
| `SLACK_BOT_TOKEN` | 슬랙 봇 토큰 (xoxb-) | O |
| `SLACK_APP_TOKEN` | 슬랙 앱 토큰 (xapp-) | O |
| `SLACK_SIGNING_SECRET` | 슬랙 서명 시크릿 | O |
| `SLACK_MONITOR_CHANNELS` | 모니터링할 채널 ID (쉼표 구분) | - |
| `SLACK_APPROVAL_CHANNEL` | 승인 알림 채널 ID | O |
| `SLACK_NOTIFY_USER_ID` | 멘션할 사용자 ID | - |
| `GMAIL_CREDENTIALS_PATH` | OAuth credentials.json 경로 | - |
| `GMAIL_ALLOWED_SENDERS` | 허용할 발신자 (쉼표 구분) | - |
| `ANTHROPIC_API_KEY` | Claude API 키 | O |
| `BIKESHARE_BASE_URL` | admin web URL | O |
| `BIKESHARE_USERNAME` | admin web 로그인 ID | O |
| `BIKESHARE_PASSWORD` | admin web 로그인 비밀번호 | O |

## 커스터마이징

### 키워드 수정

`main.py`의 `_is_return_zone_message()` 메서드에서 감지 키워드 수정:

```python
keywords = [
    "반납구역", "반납 구역", "반납존",
    "제외", "축소", "확대",
    # 추가 키워드...
]
```

### AI 파싱 프롬프트 수정

`parser/ai_parser.py`의 `PARSING_PROMPT` 수정

### admin web 자동화 로직 수정

`automation/admin_web.py`에서 실제 UI에 맞게 수정

## 문제 해결

### 슬랙 연결 실패

- `SLACK_BOT_TOKEN`과 `SLACK_APP_TOKEN` 확인
- Socket Mode가 활성화되어 있는지 확인
- 앱이 워크스페이스에 설치되었는지 확인

### 이메일 인증 실패

- `credentials.json` 파일 위치 확인
- OAuth 동의 화면이 구성되었는지 확인
- 테스트 사용자에 본인 이메일이 추가되었는지 확인

### admin web 자동화 실패

- 선택자(selector)가 실제 UI와 일치하는지 확인
- `BIKESHARE_HEADLESS=false`로 설정하여 브라우저 동작 확인
- 스크린샷 폴더에서 오류 시점 캡처 확인

## 라이선스

Internal Use Only
