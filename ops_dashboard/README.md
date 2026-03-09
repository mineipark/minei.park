# 운영 대시보드 (Ops Dashboard)

Streamlit 기반 BikeShare 운영 현황 모니터링 대시보드

## 개요

센터별/직원별 운영 현황을 실시간으로 모니터링하고 분석할 수 있는 멀티페이지 대시보드입니다.

## 페이지 구성

| 페이지 | 파일 | 설명 | 접근 권한 |
|--------|------|------|----------|
| 전센터 대시보드 | `0_전센터_대시보드.py` | 전체 센터 종합 현황 | 관리자 전용 |
| 센터별 대시보드 | `1_센터별_대시보드.py` | 개별 센터 상세 현황 | 해당 센터 |
| 직원별 동선 | `2_직원별_동선.py` | 핸들러 이동 경로 추적 | 해당 센터 |
| 인원별 월간 | `3_인원별_월간.py` | 월간 개인 실적 분석 | 해당 센터 |
| 유지보수 성과 | `4_유지보수_성과.py` | 유지보수 KPI 분석 | 관리자 전용 |
| 관리 | `5_관리.py` | 시스템 관리 기능 | 관리자 전용 |
| 경쟁사 비교 | `6_경쟁사_라이딩_비교.py` | 경쟁사 라이딩 데이터 비교 | 전체 |

## 실행

```bash
cd ops_dashboard
streamlit run ops_worker_dashboard.py
```

## 프로젝트 구조

```
ops_dashboard/
├── ops_worker_dashboard.py    # 메인 엔트리포인트 (로그인)
├── pages/                     # 멀티페이지 구성
│   ├── 0_전센터_대시보드.py
│   ├── 1_센터별_대시보드.py
│   ├── 2_직원별_동선.py
│   ├── 3_인원별_월간.py
│   ├── 4_유지보수_성과.py
│   ├── 5_관리.py
│   └── 6_경쟁사_라이딩_비교.py
├── utils/                     # 유틸리티 모듈
│   ├── auth.py               # 인증 처리
│   ├── bigquery.py           # BigQuery 연동
│   ├── sheets.py             # Google Sheets 연동
│   ├── calc.py               # 계산 유틸리티
│   └── sidebar_style.py      # UI 스타일링
└── requirements.txt          # 의존성 목록
```

## 인증 시스템

- 이름 + 비밀번호 기반 로그인
- 센터별 접근 권한 관리
- 관리자는 전체 센터 접근 가능

### 권한 구조
```python
# 센터 담당자: 해당 센터만 접근
"Staff_D1": {"password": "****", "centers": ["Center_East"]}

# CJ 관리자: CJ 계열 센터 접근
"Staff_Admin1": {"password": "****", "centers": ["Partner_Seoul", "Partner_Gwacheon", ...]}

# 전체 관리자: 모든 센터 접근
"관리자": {"password": "****", "centers": ["전체"]}
```

## 데이터 소스

### BigQuery
- `bikeshare.service.rides` - 라이딩 데이터
- `bikeshare.service.service_center` - 센터 정보
- `bikeshare.service.geo_area` - 권역 정보

### Google Sheets
- 운영 관련 외부 데이터 연동

## 주요 기능

### 센터별 대시보드
- 일별 라이딩 현황
- 시간대별 이용 패턴
- 권역별 분포

### 직원별 동선
- Folium 지도 기반 이동 경로 시각화
- 작업 시간대별 위치 추적

### 유지보수 성과
- 핸들러별 작업량 분석
- 유지보수 효율 지표

## 환경 설정

### 필수 환경 변수
```bash
GOOGLE_APPLICATION_CREDENTIALS=path/to/credentials.json
```

### 의존성 설치
```bash
pip install -r requirements.txt
```

### requirements.txt 주요 패키지
- `streamlit>=1.30.0` - 대시보드 프레임워크
- `google-cloud-bigquery` - BigQuery 연동
- `folium`, `streamlit-folium` - 지도 시각화
- `plotly`, `altair` - 차트 라이브러리
- `pandas`, `numpy` - 데이터 처리

## 배포

Streamlit Cloud 또는 자체 서버에서 실행

```bash
# 포트 지정 실행
streamlit run ops_worker_dashboard.py --server.port 8501
```

## 대상 센터

Center_North, Center_West, Center_South, Center_Gimpo, Center_East, Center_Central, Partner_Gwacheon, Partner_Ansan, Partner_Seoul, Partner_Daejeon
