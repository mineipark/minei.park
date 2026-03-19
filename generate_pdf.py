#!/usr/bin/env python3
"""포트폴리오 PDF 생성 - 디자인 강화 버전
Safari에서 열고 Cmd+P → PDF로 저장"""

import os
import base64

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ASSETS_DIR = os.path.join(BASE_DIR, "assets")
OUTPUT_PATH = os.path.join(BASE_DIR, "portfolio_print.html")


def img_to_base64(filename):
    path = os.path.join(ASSETS_DIR, filename)
    if os.path.exists(path):
        with open(path, "rb") as f:
            data = base64.b64encode(f.read()).decode()
        ext = filename.rsplit(".", 1)[-1].lower()
        if ext == "jpg":
            ext = "jpeg"
        return f"data:image/{ext};base64,{data}"
    return ""


def build_html():
    imgs = {}
    for name in [
        "demand_forecast_slack_report.png",
        "monthly_asset_report.png",
        "tech_report_slack.png",
        "ops_dashboard_center.png",
        "ops_dashboard_route.png",
        "task_board_backlog.png",
        "task_board_personal.png",
        "data_alert.jpeg",
        "data_resolved.jpeg",
        "data_daily_report.jpeg",
    ]:
        imgs[name] = img_to_base64(name)

    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<title>박민이 포트폴리오</title>
<style>
/* ===== 인쇄 설정 ===== */
@page {{
    size: A4;
    margin: 18mm 16mm 16mm 16mm;
}}
@media print {{
    body {{ -webkit-print-color-adjust: exact; print-color-adjust: exact; }}
    .no-print {{ display: none; }}
    .page-break {{ page-break-before: always; }}
}}

/* ===== 기본 ===== */
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{
    font-family: -apple-system, 'Apple SD Gothic Neo', 'Noto Sans KR', 'Helvetica Neue', sans-serif;
    font-size: 9pt;
    color: #1a1a2e;
    line-height: 1.65;
    background: #fff;
}}

/* ===== 컬러 시스템 ===== */
:root {{
    --primary: #0f4c81;
    --accent: #e63946;
    --bg-light: #f7f9fc;
    --bg-card: #ffffff;
    --border: #e2e8f0;
    --text-main: #1a1a2e;
    --text-sub: #64748b;
    --text-muted: #94a3b8;
}}

/* ===== 커버 페이지 ===== */
.cover-page {{
    height: 100vh;
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    text-align: center;
    position: relative;
}}
.cover-page::before {{
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 6px;
    background: linear-gradient(90deg, var(--primary) 0%, var(--accent) 100%);
}}
.cover-name {{
    font-size: 38pt;
    font-weight: 800;
    color: var(--primary);
    letter-spacing: -1px;
    margin-bottom: 6px;
}}
.cover-subtitle {{
    font-size: 12pt;
    color: var(--text-sub);
    font-weight: 400;
    letter-spacing: 4px;
    text-transform: uppercase;
    margin-bottom: 30px;
}}
.cover-divider {{
    width: 50px;
    height: 3px;
    background: var(--accent);
    margin: 0 auto 30px;
}}
.cover-contact {{
    font-size: 8.5pt;
    color: var(--text-muted);
    line-height: 1.8;
}}
.cover-contact a {{ color: var(--primary); text-decoration: none; }}

/* ===== 섹션 타이틀 ===== */
.section-title {{
    font-size: 13pt;
    font-weight: 700;
    color: var(--primary);
    padding-bottom: 5px;
    border-bottom: 2px solid var(--primary);
    margin: 18px 0 10px;
    letter-spacing: -0.3px;
}}

/* ===== 역량 카드 ===== */
.competency-grid {{
    display: grid;
    grid-template-columns: 1fr 1fr 1fr;
    gap: 10px;
    margin: 12px 0;
}}
.comp-card {{
    background: var(--bg-light);
    border-radius: 6px;
    padding: 12px 14px;
    border-top: 3px solid var(--primary);
}}
.comp-card .label {{
    font-size: 8pt;
    font-weight: 700;
    color: var(--primary);
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-bottom: 4px;
}}
.comp-card .value {{
    font-size: 8.5pt;
    color: var(--text-main);
    line-height: 1.5;
}}

/* ===== 기술 스택 ===== */
.stack-grid {{
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 4px 20px;
    margin: 8px 0;
}}
.stack-item {{
    display: flex;
    padding: 3px 0;
    border-bottom: 1px solid #f1f5f9;
}}
.stack-label {{
    width: 62px;
    font-weight: 700;
    color: var(--primary);
    font-size: 8pt;
    flex-shrink: 0;
}}
.stack-value {{ font-size: 8pt; color: var(--text-main); }}

/* ===== 프로젝트 목록 테이블 ===== */
table {{ width: 100%; border-collapse: collapse; font-size: 8pt; margin: 8px 0; }}
thead th {{
    background: var(--primary);
    color: #fff;
    padding: 7px 8px;
    text-align: left;
    font-weight: 600;
    font-size: 7.5pt;
    text-transform: uppercase;
    letter-spacing: 0.3px;
}}
tbody td {{
    padding: 7px 8px;
    border-bottom: 1px solid var(--border);
    vertical-align: top;
}}
tbody tr:nth-child(even) td {{ background: #fafbfd; }}
tbody tr:hover td {{ background: #f0f4ff; }}

/* ===== 프로젝트 상세 ===== */
.project-header {{
    display: flex;
    align-items: baseline;
    gap: 10px;
    margin-bottom: 4px;
}}
.project-num {{
    font-size: 9pt;
    font-weight: 800;
    color: var(--accent);
    letter-spacing: 1px;
    white-space: nowrap;
}}
.project-title {{
    font-size: 15pt;
    font-weight: 700;
    color: var(--text-main);
    letter-spacing: -0.5px;
}}
.project-desc {{
    font-size: 8.5pt;
    color: var(--text-sub);
    margin-bottom: 12px;
    padding-left: 2px;
}}

h3 {{
    font-size: 9.5pt;
    font-weight: 700;
    color: var(--primary);
    margin: 10px 0 4px;
    padding-left: 8px;
    border-left: 3px solid var(--accent);
}}

ul {{ padding-left: 20px; margin: 3px 0 8px; }}
li {{ margin-bottom: 2px; font-size: 8.5pt; }}
li strong {{ color: var(--primary); }}

/* ===== 태그 ===== */
.tags {{ margin-top: 10px; display: flex; flex-wrap: wrap; gap: 4px; }}
.tag {{
    display: inline-block;
    background: #eef2ff;
    color: var(--primary);
    font-size: 7pt;
    padding: 2px 8px;
    border-radius: 10px;
    font-weight: 600;
    letter-spacing: 0.2px;
}}

/* ===== 수식 / 강조 박스 ===== */
.highlight-box {{
    background: var(--bg-light);
    border-left: 3px solid var(--primary);
    padding: 10px 14px;
    margin: 8px 0;
    border-radius: 0 4px 4px 0;
    font-size: 8.5pt;
}}
.highlight-box .hl {{ color: var(--primary); font-weight: 700; }}

.code-box {{
    background: #1e293b;
    color: #e2e8f0;
    border-radius: 6px;
    padding: 10px 14px;
    font-family: 'SF Mono', 'Fira Code', 'Consolas', monospace;
    font-size: 7.5pt;
    line-height: 1.6;
    white-space: pre;
    margin: 8px 0;
    overflow: hidden;
}}
.code-box .comment {{ color: #64748b; }}
.code-box .keyword {{ color: #7dd3fc; }}
.code-box .string {{ color: #86efac; }}

.formula-inline {{
    background: #f8fafc;
    padding: 4px 10px;
    font-family: monospace;
    font-size: 8.5pt;
    border: 1px solid var(--border);
    border-radius: 4px;
    display: inline-block;
    margin: 4px 0;
}}

/* ===== 이미지 ===== */
.img-grid {{
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 10px;
    margin: 10px 0;
}}
.img-grid-3 {{
    display: grid;
    grid-template-columns: 1fr 1fr 1fr;
    gap: 8px;
    margin: 10px 0;
}}
.img-item {{ text-align: center; }}
.img-item img {{
    max-width: 100%;
    border: 1px solid var(--border);
    border-radius: 6px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.08);
}}
.img-caption {{
    font-size: 7pt;
    color: var(--text-muted);
    margin-top: 4px;
    font-weight: 500;
}}

/* ===== 프레임워크 ===== */
.framework-table {{ margin: 8px 0; }}
.framework-table td:first-child {{ font-weight: 700; color: var(--primary); width: 100px; }}

/* ===== 아키텍처 ===== */
.arch-box {{
    background: #f8fafc;
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 12px 16px;
    font-family: monospace;
    font-size: 7pt;
    line-height: 1.6;
    white-space: pre;
    color: var(--text-main);
}}

/* ===== 푸터 ===== */
.footer {{
    text-align: center;
    margin-top: 30px;
    padding-top: 12px;
    border-top: 1px solid var(--border);
}}
.footer p {{ font-size: 7.5pt; color: var(--text-muted); }}
.footer a {{ color: var(--primary); text-decoration: none; }}

/* ===== 페이지 번호 (인쇄시) ===== */
.page-num {{
    position: fixed;
    bottom: 10mm;
    right: 16mm;
    font-size: 7pt;
    color: var(--text-muted);
}}
</style>
</head>
<body>

<!-- ===== 커버 ===== -->
<div class="cover-page">
    <div class="cover-name">박민이</div>
    <div class="cover-subtitle">Portfolio</div>
    <div class="cover-divider"></div>
    <div class="cover-contact">
        minei.park@gmail.com<br>
        <a href="https://github.com/mineipark/minei.park">github.com/mineipark/minei.park</a>
    </div>
</div>

<!-- ===== 프로필 & 역량 ===== -->
<div class="page-break"></div>

<div class="section-title">Core Competencies</div>
<div class="competency-grid">
    <div class="comp-card">
        <div class="label">ML Forecasting</div>
        <div class="value">LightGBM 앙상블 모델로<br>일별 수요 예측<br><strong>MAPE 20%</strong> 달성</div>
    </div>
    <div class="comp-card">
        <div class="label">Experiment Design</div>
        <div class="value">DiD, ROI 분석 등<br>실험 설계로 현장 작업의<br>효과를 <strong>정량적으로 검증</strong></div>
    </div>
    <div class="comp-card">
        <div class="label">Automation Pipeline</div>
        <div class="value"><strong>19개 자동화</strong> 도구를<br>설계·운영하며<br>6개 업무 영역 커버</div>
    </div>
</div>

<div class="section-title">Tech Stack</div>
<div class="stack-grid">
    <div class="stack-item"><span class="stack-label">Data</span><span class="stack-value">BigQuery, Google Sheets API, Amplitude</span></div>
    <div class="stack-item"><span class="stack-label">ML</span><span class="stack-value">LightGBM, scikit-learn, SciPy</span></div>
    <div class="stack-item"><span class="stack-label">Dashboard</span><span class="stack-value">Streamlit, Plotly, Folium</span></div>
    <div class="stack-item"><span class="stack-label">Automation</span><span class="stack-value">Slack Bolt, Apps Script, GitHub Actions</span></div>
    <div class="stack-item"><span class="stack-label">AI</span><span class="stack-value">Claude API (Claude Code)</span></div>
    <div class="stack-item"><span class="stack-label">Spatial</span><span class="stack-value">H3 Hexagon, Shapely, GeoJSON</span></div>
</div>

<div class="section-title">Projects Overview</div>
<table>
    <thead>
        <tr><th>#</th><th>프로젝트</th><th>한줄 요약</th><th>핵심 기술</th><th>주요 성과</th></tr>
    </thead>
    <tbody>
        <tr><td>1</td><td>ML 기반 수요 예측</td><td>2-Model Ensemble 일별 수요 예측</td><td>LightGBM / BigQuery</td><td>MAPE 20%, 일일 자동화</td></tr>
        <tr><td>2</td><td>현장 작업 ROI & DiD</td><td>현장 작업 효과 정량 검증</td><td>DiD / ROI Analysis</td><td>전환율 정량화, 실험 체계</td></tr>
        <tr><td>3</td><td>운영팀 Task 보드</td><td>5개 뷰 통합 PM 웹앱</td><td>Firebase / Firestore</td><td>팀 업무 가시성, AI 코칭</td></tr>
        <tr><td>4</td><td>기술소견서 & 자산 리포트</td><td>사고접수 자동화, 월간 자산 리포트</td><td>Slack Bot / Apps Script</td><td>수동 작업 완전 제거</td></tr>
        <tr><td>5</td><td>자동화 카탈로그</td><td>19개 자동화 도구 체계적 관리</td><td>Google Sheets</td><td>6개 영역, 비개발자 협업</td></tr>
        <tr><td>6</td><td>운영 대시보드</td><td>7페이지 멀티페이지 모니터링</td><td>Streamlit / Folium</td><td>RBAC, GPS 동선 시각화</td></tr>
        <tr><td>7</td><td>Data 파이프라인 모니터링</td><td>6-Layer 파이프라인 감시 시스템</td><td>BigQuery / Slack / EC2</td><td>387 테이블 자동 감시</td></tr>
    </tbody>
</table>

<!-- ===== Project 1 ===== -->
<div class="page-break"></div>
<div class="project-header">
    <div class="project-num">PROJECT 01</div>
    <div class="project-title">ML 기반 수요 예측 시스템</div>
</div>
<div class="project-desc">경험적 어림 예측을 ML 모델로 대체하여, 일별 권역/구역 단위 이용량을 자동으로 예측</div>

<h3>Problem</h3>
<ul>
    <li>계절성, 날씨, 이벤트 변동을 반영하기 어려운 경험적 수요 예측</li>
    <li>인력/재배치 스케줄링에 정량적 근거 부재</li>
</ul>

<h3>Approach</h3>
<p><strong>2-Model Ensemble</strong>: 앱 오픈 수(Opens)와 전환율(RPO)을 분리 예측</p>
<div class="formula-inline">predicted_rides = predicted_app_opens × predicted_RPO</div>
<ul>
    <li><strong>Opens Model</strong>: LightGBM GBDT (MSE) — 순수 수요 시그널</li>
    <li><strong>RPO Model</strong>: LightGBM GBDT (Huber) — 공급 상태 영향 전환 지표</li>
    <li><strong>77개 피처</strong>: 시계열 Rolling(14) + Lag(8) + 캘린더(12) + 날씨(10) + 공간(15) + 인터랙션(18)</li>
    <li><strong>후처리</strong>: RPO Shrinkage, 소규모 구역 클리핑, 구역/요일/공휴일 보정</li>
</ul>

<h3>Results</h3>
<ul>
    <li><strong>MAPE 20%</strong> 달성 — 기존 단순 평균 대비 예측 오차 약 40% 감소</li>
    <li>GitHub Actions 기반 <strong>일일 자동 파이프라인</strong> (매일 09:00 KST)</li>
    <li>Slack 봇 매일 자동 성과 리포트: 추세 진단, 편향 분석, 보정 알림 (114개 권역)</li>
</ul>

{"<div class='img-grid'><div class='img-item'><img src='" + imgs['demand_forecast_slack_report.png'] + "'/><div class='img-caption'>일일 예측 리포트 — Slack 자동 발송</div></div></div>" if imgs.get('demand_forecast_slack_report.png') else ""}

<div class="tags">
    <span class="tag">Python</span><span class="tag">LightGBM</span><span class="tag">BigQuery</span>
    <span class="tag">GitHub Actions</span><span class="tag">Google Sheets API</span><span class="tag">Slack Webhook</span>
</div>

<!-- ===== Project 2 ===== -->
<div class="page-break"></div>
<div class="project-header">
    <div class="project-num">PROJECT 02</div>
    <div class="project-title">현장 작업 ROI 분석 & DiD 실험 설계</div>
</div>
<div class="project-desc">현장 작업의 실제 효과를 정량 측정하고, 동선 개선의 인과 효과를 DiD로 검증</div>

<h3>Problem</h3>
<ul>
    <li>현장 작업(수거, 배터리교체) 후 실제 라이딩 발생 여부 알 수 없음</li>
    <li>동선 개선 시범 운영 효과를 데이터로 검증할 방법 부재</li>
</ul>

<h3>Approach: ROI 분석</h3>
<ul>
    <li><strong>매칭 키</strong>: bike_id + 작업 완료 시각 기준 24h 윈도우</li>
    <li><strong>산출 지표</strong>: 24h 전환율, 건당 매출 기여, 기여율</li>
    <li><strong>비교 축</strong>: 조치유형(고장수거 vs 배터리교체), 시간대, 권역</li>
</ul>

<h3>Approach: DiD 실험</h3>
<div class="formula-inline">DiD 효과 = (실험군_사후 − 실험군_사전) − (대조군_사후 − 대조군_사전)</div>
<ul>
    <li>5개 지표 동시 추적: 라이딩 건수, 매출, 현장조치율, 접근성, 전환율</li>
    <li>시간에 따른 자연적 변화를 대조군으로 제거 → 순수 인과 효과 추출</li>
</ul>

<h3>Results</h3>
<ul>
    <li>조치유형별 24시간 내 <strong>전환율 차이 정량화</strong></li>
    <li>DiD를 통해 동선 개선의 <strong>순수 효과를 인과적으로 측정</strong></li>
    <li><strong>실험 기반 의사결정 체계</strong> 구축: 시범 운영 → 효과 검증 → 전사 확대</li>
</ul>

<div class="tags">
    <span class="tag">BigQuery</span><span class="tag">DiD</span><span class="tag">ROI Analysis</span><span class="tag">A/B Test Design</span>
</div>

<!-- ===== Project 3 ===== -->
<div class="page-break"></div>
<div class="project-header">
    <div class="project-num">PROJECT 03</div>
    <div class="project-title">운영팀 Task 보드</div>
</div>
<div class="project-desc">분산된 팀 업무를 하나의 보드에서 관리하는 프로젝트 매니지먼트 웹앱</div>

<h3>Problem</h3>
<ul>
    <li>팀 내 프로젝트/개인 업무가 여러 도구에 분산되어 진행률 파악 어려움</li>
    <li>프로젝트/개인/주간 단위 스케줄을 동시에 볼 수 없음</li>
</ul>

<h3>Approach</h3>
<p>Firebase 기반 웹앱, 하나의 데이터소스(Firestore)에서 <strong>5가지 뷰</strong> 통합:</p>
<ul>
    <li><strong>대시보드</strong>: 주간 달성률, 프로젝트별 진행률, 오늘 마감 Task</li>
    <li><strong>개인 페이지</strong>: AI 코칭, D-day, 마감 지난 Task 경고</li>
    <li><strong>Weekly</strong>: 간트차트, 프로젝트별 Task 배치</li>
    <li><strong>프로젝트</strong>: 칸반 보드 (To do / Doing / Done)</li>
    <li><strong>백로그</strong>: 미배정 Task 풀, 프로젝트 이동</li>
</ul>

<h3>Results</h3>
<ul>
    <li>5개 뷰 통합으로 팀 업무 <strong>가시성 확보</strong></li>
    <li>프로젝트별 진행률 <strong>실시간 추적</strong></li>
</ul>

<div class="img-grid">
    {"<div class='img-item'><img src='" + imgs['task_board_backlog.png'] + "'/><div class='img-caption'>백로그</div></div>" if imgs.get('task_board_backlog.png') else ""}
    {"<div class='img-item'><img src='" + imgs['task_board_personal.png'] + "'/><div class='img-caption'>개인 페이지</div></div>" if imgs.get('task_board_personal.png') else ""}
</div>

<div class="tags">
    <span class="tag">Firebase</span><span class="tag">Firestore</span><span class="tag">Authentication</span><span class="tag">JavaScript</span>
</div>

<!-- ===== Project 4 ===== -->
<div class="page-break"></div>
<div class="project-header">
    <div class="project-num">PROJECT 04</div>
    <div class="project-title">기술소견서 & 자산 리포트 자동화</div>
</div>
<div class="project-desc">사고기기 접수 시 기술소견서 자동 생성, 월별 자산 현황 자동 집계/리포트</div>

<h3>Problem</h3>
<ul>
    <li>사고 접수마다 기술소견서를 수동 작성 → 반복 작업, 누락 리스크</li>
    <li>월별 자산 현황(보유/가용/유형변경) 수동 집계 → 시간 소모</li>
</ul>

<h3>Approach</h3>
<ul>
    <li><strong>기술소견서</strong>: CS 사고 접수 → Slack 알림 → Google Sheets 소견서 자동 생성 → 담당자 배정</li>
    <li><strong>월간 자산 리포트</strong>: 매월 1일 GitHub Actions 자동 실행 → BigQuery device_snapshot (가칭) 월평균 집계 → Slack Block Kit 자동 발송</li>
</ul>

<h3>Results</h3>
<ul>
    <li><strong>기술소견서</strong>: 수동 문서 작성 완전 제거, 5분 이내 자동 처리</li>
    <li><strong>월간 자산 리포트</strong>: 완전 자동화 (매월 1일 09:00)</li>
</ul>

<div class="img-grid">
    {"<div class='img-item'><img src='" + imgs['monthly_asset_report.png'] + "'/><div class='img-caption'>월간 자산 리포트</div></div>" if imgs.get('monthly_asset_report.png') else ""}
    {"<div class='img-item'><img src='" + imgs['tech_report_slack.png'] + "'/><div class='img-caption'>기술소견서 Slack 알림</div></div>" if imgs.get('tech_report_slack.png') else ""}
</div>

<div class="tags">
    <span class="tag">BigQuery</span><span class="tag">Google Sheets API</span><span class="tag">Apps Script</span>
    <span class="tag">Slack Webhook</span><span class="tag">GitHub Actions</span>
</div>

<!-- ===== Project 5 ===== -->
<div class="page-break"></div>
<div class="project-header">
    <div class="project-num">PROJECT 05</div>
    <div class="project-title">자동화 카탈로그 & 운영 체계</div>
</div>
<div class="project-desc">19개 자동화 도구를 체계적으로 관리하고, 비개발자도 접근할 수 있는 문서 체계</div>

<h3>6개 업무 영역별 커버리지</h3>
<table>
    <thead><tr><th>업무 영역</th><th>도구 수</th><th>주요 도구</th><th>사용 기술</th></tr></thead>
    <tbody>
        <tr><td>바이크 정비</td><td>3</td><td>일일 정비 알림, 정비 대시보드, 기술소견서</td><td>BigQuery, Slack, Streamlit</td></tr>
        <tr><td>유저 분석</td><td>2</td><td>유저 패널 대시보드, Amplitude 이벤트 분석</td><td>Streamlit, Amplitude</td></tr>
        <tr><td>현장 운영</td><td>5</td><td>태스크 관리앱, 주소 검색, 자산 추적</td><td>Firebase, Apps Script</td></tr>
        <tr><td>프랜차이즈</td><td>2</td><td>EBITDA 시뮬레이션, 계약구조 시뮬레이터</td><td>BigQuery, HTML</td></tr>
        <tr><td>수요/재배치</td><td>4</td><td>수요 예측, 날씨 수집, Sheets 동기화</td><td>BigQuery, GitHub Actions</td></tr>
        <tr><td>팀 관리</td><td>3</td><td>컨디션 트래커, HR 태스크 보드</td><td>Google Sheets, Firebase</td></tr>
        <tr style="font-weight:700;background:#f0f4ff;"><td>합계</td><td>19</td><td></td><td></td></tr>
    </tbody>
</table>

<div class="tags">
    <span class="tag">Google Sheets</span><span class="tag">Documentation</span><span class="tag">Process Management</span>
</div>

<!-- ===== Project 6 ===== -->
<div class="page-break"></div>
<div class="project-header">
    <div class="project-num">PROJECT 06</div>
    <div class="project-title">운영 대시보드</div>
</div>
<div class="project-desc">센터별 실시간 운영 현황 모니터링, 기기 관리/직원 동선/유지보수 성과 추적</div>

<h3>Approach: Streamlit 7개 페이지</h3>
<ul>
    <li><strong>전센터 대시보드</strong> — 가동률, 수리율, 현장조치율 KPI</li>
    <li><strong>센터별 대시보드</strong> — 시간대별 패턴, 센터 간 비교</li>
    <li><strong>직원별 동선</strong> — GPS 기반 Folium 지도 시각화</li>
    <li><strong>인원별 월간</strong> — 월간 처리량, 효율 비교</li>
    <li><strong>유지보수 성과</strong> — 수리 효율, 비용 분석</li>
    <li><strong>관리</strong> — 실종 위기 기기 추적</li>
    <li><strong>경쟁사 비교</strong> — 경쟁사 라이딩 데이터 벤치마킹</li>
</ul>
<p style="font-size:8.5pt;">역할 기반 접근 제어(RBAC)로 센터 직원/관리자별 뷰 분리</p>

<h3>Results</h3>
<ul>
    <li>7개 페이지로 운영 전 영역 <strong>모니터링 통합</strong></li>
    <li>실종 위기 기기 자동 필터링 → 수색 프로세스 체계화</li>
</ul>

<div class="img-grid">
    {"<div class='img-item'><img src='" + imgs['ops_dashboard_center.png'] + "'/><div class='img-caption'>센터별 대시보드</div></div>" if imgs.get('ops_dashboard_center.png') else ""}
    {"<div class='img-item'><img src='" + imgs['ops_dashboard_route.png'] + "'/><div class='img-caption'>직원별 작업 동선</div></div>" if imgs.get('ops_dashboard_route.png') else ""}
</div>

<div class="tags">
    <span class="tag">Streamlit</span><span class="tag">BigQuery</span><span class="tag">Folium</span>
    <span class="tag">Plotly</span><span class="tag">Google Sheets API</span><span class="tag">RBAC</span>
</div>

<!-- ===== Project 7 ===== -->
<div class="page-break"></div>
<div class="project-header">
    <div class="project-num">PROJECT 07</div>
    <div class="project-title">Data 파이프라인 모니터링 시스템</div>
</div>
<div class="project-desc">387개 테이블, 56개 예약쿼리, EC2 Inspector를 6-Layer 구조로 자동 감시</div>

<h3>Problem</h3>
<ul>
    <li>BigQuery 예약쿼리 실패, 테이블 freshness 지연 등 <strong>파이프라인 장애를 수동으로 발견</strong></li>
    <li>EC2 Inspector(billing_sync, device_snapshot (가칭) 등)가 죽어도 <strong>알 수 없음</strong></li>
    <li>스키마 변경이 <strong>하위 테이블에 연쇄 영향</strong>을 미쳐도 사전 감지 불가</li>
</ul>

<h3>Approach: 6-Layer Architecture</h3>
<table>
    <thead><tr><th>Layer</th><th>구성</th><th>역할</th></tr></thead>
    <tbody>
        <tr><td>Trigger</td><td>data_main (2~3시간), data_all (매일 09:00), /data (온디맨드)</td><td>감시 트리거</td></tr>
        <tr><td>Scan Engine</td><td>scan_table_meta, scan_scheduled_queries, scan_ec2_cron, scan_views, scan_schemas</td><td>5개 스캔 모듈</td></tr>
        <tr><td>data_main</td><td>Critical 노드 5개 + 핵심 예약쿼리 ~15개</td><td>긴급 감시</td></tr>
        <tr><td>data_all</td><td>전체 387개 테이블 + 56개 예약쿼리</td><td>종합 점검</td></tr>
        <tr><td>EC2 Inspectors</td><td>billing_sync, device_snapshot, dedup_checker (가칭) 등 5개</td><td>데이터 정합성</td></tr>
        <tr><td>Slack Output</td><td>#data_ops (가칭)</td><td>알림 + 리포트</td></tr>
    </tbody>
</table>

<h3>BigQuery 라이브러리 활용</h3>
<div class="code-box"><span class="keyword">from</span> google.cloud <span class="keyword">import</span> bigquery
<span class="keyword">from</span> google.cloud <span class="keyword">import</span> bigquery_datatransfer_v1

<span class="comment"># scan_table_meta: __TABLES__로 freshness & row_count 조회</span>
<span class="comment"># scan_scheduled_queries: DataTransferServiceClient로 예약쿼리 상태 확인</span>
<span class="comment"># scan_schemas: client.get_table() → 스키마 비교 (추가/삭제/타입변경)</span>
<span class="comment"># scan_views: dry_run=True로 뷰 SQL 유효성 검증</span></div>

<h3>핵심 설계 원칙</h3>
<div class="highlight-box">
    <span class="hl">Data</span> = 파이프라인 인프라 감시 (돌아가고 있는가?)<br>
    <span class="hl">Inspector</span> = 데이터 정합성 점검 (데이터가 맞는가?)<br>
    <span class="hl">Data → Inspector</span> = Inspector 생존 감시 (Inspector가 살아있는가?)
</div>

<h3>Results</h3>
<ul>
    <li><strong>387개 테이블 + 56개 예약쿼리</strong> 자동 감시 체계 구축</li>
    <li>data_main: 2~3시간 주기로 <strong>Critical 노드 장애 즉시 감지</strong></li>
    <li>스키마 변경 감지 + 의존성 체인 분석 → <strong>연쇄 장애 사전 차단</strong></li>
</ul>

<div class="img-grid-3">
    {"<div class='img-item'><img src='" + imgs['data_alert.jpeg'] + "'/><div class='img-caption'>이상 감지 알림</div></div>" if imgs.get('data_alert.jpeg') else ""}
    {"<div class='img-item'><img src='" + imgs['data_resolved.jpeg'] + "'/><div class='img-caption'>장애 해결 완료</div></div>" if imgs.get('data_resolved.jpeg') else ""}
    {"<div class='img-item'><img src='" + imgs['data_daily_report.jpeg'] + "'/><div class='img-caption'>일일 점검 리포트</div></div>" if imgs.get('data_daily_report.jpeg') else ""}
</div>

<div class="tags">
    <span class="tag">BigQuery</span><span class="tag">bigquery_datatransfer</span><span class="tag">Python</span>
    <span class="tag">Slack Bot</span><span class="tag">EC2</span><span class="tag">Pipeline Monitoring</span>
</div>

<!-- ===== 마지막: 의사결정 프레임워크 ===== -->
<div class="page-break"></div>
<div class="section-title">Decision Framework</div>

<div class="highlight-box">
    매출 = (앱 오픈 × 접근성 × 전환율) × 건당 매출<br>
    비용 = 현장 운영비 (정비 + 재배치 + 배터리)<br>
    <span class="hl">EBITDA = 매출 − 비용</span>
</div>

<table class="framework-table">
    <thead><tr><th>레버</th><th>프로젝트</th><th>기대 효과</th></tr></thead>
    <tbody>
        <tr><td>접근성 개선</td><td>수요 예측 → 재배치 최적화</td><td>앱 오픈 시 100m 내 바이크 확률 증가</td></tr>
        <tr><td>전환율 개선</td><td>ROI 분석 → 품질 우선순위화</td><td>접근 가능 사용자의 실제 라이딩 비율 증가</td></tr>
        <tr><td>비용 절감</td><td>동선 최적화, 자동화</td><td>현장 작업 효율 향상, 수동 작업 제거</td></tr>
        <tr><td>데이터 신뢰도</td><td>Data 파이프라인 모니터링</td><td>장애 즉시 감지, 의사결정 안정성 확보</td></tr>
    </tbody>
</table>

<div class="section-title" style="margin-top:24px;">System Architecture</div>
<div class="arch-box">[데이터 소스]                    [데이터 레이크]             [AI/ML]
앱 오픈 이벤트  ───┐
라이딩 기록     ───┤         BigQuery      ──→  수요 예측 모델
기기 스냅샷     ───┤                        ──→  실험 분석 (DiD/ROI)
기상청 API      ───┘
CS 사고 접수    ──────────→ 기술소견서 자동화

[자동화 파이프라인]                              [아웃풋]
일일 예측 파이프라인  ──→  Google Sheets  ──→   Slack 봇 (알림/리포트)
월간 자산 리포트      ──→  Slack 봇
                                                Streamlit 대시보드
[파이프라인 감시]                                Task 보드 (Firebase)
Data  ──→  387 테이블 + 56 예약쿼리 감시
      ──→  EC2 Inspector 생존 감시  ──→  Slack 장애 알림

[인프라]
GitHub Actions (매일 09:00 / 매월 1일 / Data 2~3시간)</div>

<div class="footer">
    <p>본 포트폴리오는 실제 운영 환경에서 설계·구축한 시스템을 기반으로 합니다.</p>
    <p>회사 고유 데이터, 인증 정보, 식별 가능한 정보는 모두 제거 또는 일반화되었습니다.</p>
    <p style="margin-top:8px;"><a href="https://github.com/mineipark/minei.park">github.com/mineipark/minei.park</a></p>
</div>

</body>
</html>"""
    return html


def main():
    html_content = build_html()
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(html_content)
    print(f"디자인 강화 HTML 생성 완료: {OUTPUT_PATH}")
    print("Safari에서 열고 Cmd+P → PDF로 저장하세요")
    os.system(f"open '{OUTPUT_PATH}'")


if __name__ == "__main__":
    main()
