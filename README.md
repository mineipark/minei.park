# 박민이 | AI 기반 운영 기획 포트폴리오

> 모빌리티 서비스 런칭부터 약 5년간, 현장의 문제를 데이터로 정의하고 AI/ML로 해결해온 운영 기획자입니다.

![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)
![BigQuery](https://img.shields.io/badge/BigQuery-669DF6?style=flat-square&logo=googlebigquery&logoColor=white)
![LightGBM](https://img.shields.io/badge/LightGBM-02569B?style=flat-square&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=flat-square&logo=streamlit&logoColor=white)
![Claude API](https://img.shields.io/badge/Claude_API-191919?style=flat-square&logo=anthropic&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-2088FF?style=flat-square&logo=githubactions&logoColor=white)
![Google Sheets](https://img.shields.io/badge/Google_Sheets-34A853?style=flat-square&logo=googlesheets&logoColor=white)
![Slack](https://img.shields.io/badge/Slack_Bot-4A154B?style=flat-square&logo=slack&logoColor=white)

---

## 핵심 역량

| ML 예측 모델링 | 데이터 기반 실험 설계 | 업무 자동화 파이프라인 |
|:---:|:---:|:---:|
| LightGBM 앙상블 모델로<br>일별 수요를 예측하고<br>MAPE 11%를 달성 | DiD, ROI 분석 등<br>실험 설계로 현장 작업의<br>효과를 정량적으로 검증 | 19개 자동화 도구를<br>설계·운영하며<br>6개 업무 영역을 커버 |

---

## Projects

| # | 프로젝트 | 한줄 요약 | 핵심 기술 |
|:-:|---------|----------|----------|
| 1 | [**ML 기반 수요 예측 시스템**](./projects/demand-forecast/) | 2-Model Ensemble로 일별 수요 예측, MAPE 11% | LightGBM · BigQuery · GitHub Actions |
| 2 | [**현장 작업 ROI & DiD 실험**](./projects/experiment-did-roi/) | 현장 작업 효과를 ROI·DiD로 정량 검증 | DiD / ROI Analysis · BigQuery |
| 3 | [**운영팀 Task 보드**](./projects/task-board/) | 5개 뷰 통합 프로젝트 매니지먼트 웹앱 | Firebase · Firestore · JS |
| 4 | [**기술소견서 & 자산 리포트 자동화**](./projects/automation-report/) | 사고 접수→소견서 자동 생성, 월간 자산 리포트 | Slack Bot · Apps Script · BigQuery |
| 5 | [**자동화 카탈로그**](./projects/automation-catalog/) | 19개 자동화 도구 체계적 관리 | Google Sheets · Process Mgmt |

> 각 프로젝트를 클릭하면 **Problem → Approach → Architecture → Results** 상세 페이지로 이동합니다.

---

## 전체 시스템 아키텍처

```mermaid
flowchart TB
    subgraph 데이터 소스
        APP[앱 오픈 이벤트]
        RIDE[라이딩 기록]
        SNAP[기기 스냅샷]
        WEATHER[기상청 API]
        CS_DATA[CS 사고 접수]
    end

    subgraph 데이터 레이크
        BQ[(BigQuery)]
    end

    subgraph AI_ML
        FORECAST[수요 예측 모델<br>LightGBM Ensemble]
        ANALYSIS[실험 분석<br>DiD / ROI]
    end

    subgraph 자동화 파이프라인
        PIPELINE[일일 예측 파이프라인]
        TECH_DOC[기술소견서 자동화]
        ASSET[월간 자산 리포트]
    end

    subgraph 아웃풋
        SHEETS[Google Sheets<br>예측 결과 / 분석 데이터]
        SLACK[Slack 봇<br>알림 / 리포트]
        DASH[Streamlit 대시보드<br>실시간 모니터링]
        TASK[Task 보드<br>팀 업무 관리]
    end

    subgraph 인프라
        GA[GitHub Actions<br>스케줄러]
    end

    APP & RIDE & SNAP --> BQ
    WEATHER --> BQ
    CS_DATA --> TECH_DOC

    BQ --> FORECAST
    BQ --> ANALYSIS
    BQ --> ASSET

    FORECAST --> PIPELINE --> SHEETS --> SLACK
    ANALYSIS --> SHEETS
    TECH_DOC --> SLACK
    ASSET --> SLACK

    BQ --> DASH
    GA -.->|매일 09:00| PIPELINE
    GA -.->|매월 1일| ASSET
```

---

## 의사결정 프레임워크

```
매출 = (앱 오픈 × 접근성 × 전환율) × 건당 매출
비용 = 현장 운영비 (정비 + 재배치 + 배터리)
EBITDA = 매출 - 비용
```

| 레버 | 프로젝트 | 기대 효과 |
|------|---------|----------|
| **접근성 개선** | 수요 예측 → 재배치 최적화 | 앱 오픈 시 100m 내 바이크 확률 증가 |
| **전환율 개선** | ROI 분석 → 품질 우선순위화 | 접근 가능 사용자의 실제 라이딩 비율 증가 |
| **비용 절감** | 동선 최적화, 자동화 | 현장 작업 효율 향상, 수동 작업 제거 |

---

## 기술 스택

| 분류 | 기술 |
|------|------|
| **데이터** | BigQuery, Google Sheets API, Amplitude |
| **ML** | LightGBM, scikit-learn, SciPy |
| **대시보드** | Streamlit, Plotly, Folium |
| **자동화** | Slack Bolt, Apps Script, GitHub Actions |
| **AI** | Claude API (Claude Code, Codex) |
| **공간 분석** | H3 Hexagon, Shapely, GeoJSON |
| **인프라** | GitHub Actions (CI/CD), Firebase |

---

> 본 포트폴리오는 실제 운영 환경에서 설계·구축한 시스템을 기반으로 합니다.
> 회사 고유 데이터, 인증 정보, 식별 가능한 정보는 모두 제거 또는 일반화되었습니다.
