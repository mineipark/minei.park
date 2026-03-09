#!/usr/bin/env python
"""
월간 기기 현황 리포트 - BigQuery 조회 후 Slack 전송

매월 1일 실행 → 전월 데이터 조회 → Slack 메시지 전송

사용법:
    python bike_stats_report.py

필요 환경변수:
    - SLACK_BOT_TOKEN: Slack Bot Token (xoxb-)
    - BIKE_STATS_SLACK_CHANNEL: 리포트 전송할 채널 ID
"""
import os
import json
from datetime import datetime, timedelta
from calendar import monthrange

from google.oauth2 import service_account
from google.cloud import bigquery
import requests

# ── 설정 ──────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CREDENTIALS_PATH = os.path.join(SCRIPT_DIR, 'credentials', 'service-account.json')

SLACK_BOT_TOKEN = os.environ.get('SLACK_BOT_TOKEN', '')
SLACK_CHANNEL = os.environ.get('BIKE_STATS_SLACK_CHANNEL', 'YOUR_SLACK_CHANNEL_ID')

SCOPES = [
    'https://www.googleapis.com/auth/bigquery',
]


def get_previous_month_range():
    """전월의 시작일과 종료일 반환"""
    today = datetime.now()
    first_of_this_month = today.replace(day=1)
    last_of_prev_month = first_of_this_month - timedelta(days=1)

    year = last_of_prev_month.year
    month = last_of_prev_month.month
    _, last_day = monthrange(year, month)

    start_date = f"{year}-{month:02d}-01"
    end_date = f"{year}-{month:02d}-{last_day:02d}"

    return start_date, end_date, year, month


def query_bike_stats(client, start_date: str, end_date: str):
    """BigQuery에서 전월 기기 현황 조회"""
    query = """
    SELECT
        date,
        CASE
            WHEN franchise_provide_type IS NULL THEN 'In-house'
            WHEN franchise_provide_type = 10 THEN '일시불'
            WHEN franchise_provide_type = 20 THEN '임대'
            WHEN franchise_provide_type = 30 THEN '위탁'
            ELSE '기타'
        END AS type,
        COUNT(sn) / 24 AS all_bike,
        COUNTIF(bike_status IN ('BAV', 'BNB', 'BRD', 'LRD')) / 24 AS av_bike
    FROM `bikeshare.service.bike_snapshot` AS bs
    WHERE date BETWEEN @start_date AND @end_date
        AND is_active = TRUE
        AND in_testing = FALSE
    GROUP BY 1, 2
    ORDER BY 1, 2
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
            bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
        ]
    )

    df = client.query(query, job_config=job_config).to_dataframe()
    return df


def get_bq_client():
    """BigQuery 클라이언트 생성 (재사용)"""
    credentials = service_account.Credentials.from_service_account_file(
        CREDENTIALS_PATH, scopes=SCOPES
    )
    return bigquery.Client(credentials=credentials, project='bikeshare-project')


def query_type_changes(client, start_date: str, end_date: str):
    """전월 내 기기의 type(In-house↔가맹) 변화 내역 조회"""
    query = """
    WITH daily_type AS (
        SELECT
            date,
            sn,
            CASE
                WHEN franchise_provide_type IS NULL THEN 'In-house'
                WHEN franchise_provide_type = 10 THEN '일시불'
                WHEN franchise_provide_type = 20 THEN '임대'
                WHEN franchise_provide_type = 30 THEN '위탁'
                ELSE '기타'
            END AS type
        FROM `bikeshare.service.bike_snapshot`
        WHERE date BETWEEN @start_date AND @end_date
            AND is_active = TRUE
            AND in_testing = FALSE
        GROUP BY 1, 2, 3
    ),
    with_prev AS (
        SELECT
            *,
            LAG(type) OVER (PARTITION BY sn ORDER BY date) AS prev_type
        FROM daily_type
    )
    SELECT
        date AS change_date,
        prev_type AS from_type,
        type AS to_type,
        COUNT(DISTINCT sn) AS cnt
    FROM with_prev
    WHERE prev_type IS NOT NULL
        AND prev_type != type
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 3
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
            bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
        ]
    )

    df = client.query(query, job_config=job_config).to_dataframe()
    return df


def build_change_summary(change_df) -> dict:
    """변화 내역을 방향별로 요약"""
    if len(change_df) == 0:
        return {}

    summary = {}
    for _, row in change_df.iterrows():
        key = f"{row['from_type']} → {row['to_type']}"
        if key not in summary:
            summary[key] = 0
        summary[key] += int(row['cnt'])

    return summary


def calculate_monthly_avg(df) -> dict:
    """type별 월평균 계산"""
    result = {}
    for type_name in df['type'].unique():
        type_df = df[df['type'] == type_name]
        # 각 날짜별 값을 합산한 뒤 날짜 수로 나눔 (이미 /24가 적용됨)
        avg_all = type_df['all_bike'].mean()
        avg_av = type_df['av_bike'].mean()
        result[type_name] = {
            'all_bike': round(avg_all, 1),
            'av_bike': round(avg_av, 1),
        }
    return result


def build_slack_message(stats: dict, year: int, month: int, change_summary: dict = None) -> list:
    """Slack Block Kit 메시지 구성"""
    # 분류: In-house vs 가맹(일시불, 임대, 위탁)
    direct = stats.get('In-house', {'all_bike': 0, 'av_bike': 0})
    franchise_types = ['일시불', '임대', '위탁']

    # 가맹 소계
    franchise_all = sum(stats.get(t, {}).get('all_bike', 0) for t in franchise_types)
    franchise_av = sum(stats.get(t, {}).get('av_bike', 0) for t in franchise_types)

    # 전체 합계
    total_all = direct['all_bike'] + franchise_all
    total_av = direct['av_bike'] + franchise_av

    # 가용률
    direct_rate = (direct['av_bike'] / direct['all_bike'] * 100) if direct['all_bike'] > 0 else 0
    franchise_rate = (franchise_av / franchise_all * 100) if franchise_all > 0 else 0
    total_rate = (total_av / total_all * 100) if total_all > 0 else 0

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"📊 {year}년 {month}월 기기 현황 리포트",
                "emoji": True
            }
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*📅 기준기간:* {year}년 {month}월 전체\n*📈 일별 스냅샷 월평균 기준*"
            }
        },
        {"type": "divider"},
        # ── 보유 자전거대수 ──
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    "*🚲 보유 자전거대수 (월평균)*\n\n"
                    f"• *In-house:*  `{direct['all_bike']:,.1f}` 대\n"
                    f"• *가맹 합계:*  `{franchise_all:,.1f}` 대\n"
                    + "".join(
                        f"    ◦ {t}:  `{stats.get(t, {}).get('all_bike', 0):,.1f}` 대\n"
                        for t in franchise_types
                    )
                    + f"\n*합계:  `{total_all:,.1f}` 대*"
                )
            }
        },
        {"type": "divider"},
        # ── 가용 자전거대수 ──
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    "*✅ 가용 자전거대수 (월평균)*\n\n"
                    f"• *In-house:*  `{direct['av_bike']:,.1f}` 대  ({direct_rate:.1f}%)\n"
                    f"• *가맹 합계:*  `{franchise_av:,.1f}` 대  ({franchise_rate:.1f}%)\n"
                    + "".join(
                        f"    ◦ {t}:  `{stats.get(t, {}).get('av_bike', 0):,.1f}` 대\n"
                        for t in franchise_types
                    )
                    + f"\n*합계:  `{total_av:,.1f}` 대  ({total_rate:.1f}%)*"
                )
            }
        },
        {"type": "divider"},
    ]

    # ── type 변화 내역 (있을 때만) ──
    if change_summary:
        change_lines = "\n".join(
            f"• {direction}:  `{cnt}` 대"
            for direction, cnt in sorted(change_summary.items(), key=lambda x: -x[1])
        )
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*🔄 월중 유형 변경 내역*\n\n{change_lines}"
            }
        })
        blocks.append({"type": "divider"})
    else:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*🔄 월중 유형 변경 내역*\n\n변경 없음"
            }
        })
        blocks.append({"type": "divider"})

    blocks.append({
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": f"🤖 자동 생성 | `tf_bike_snapshot` 기준 | 괄호 안은 가용률(가용/보유)"
            }
        ]
    })

    return blocks


def send_slack_message(blocks: list, fallback_text: str):
    """Slack Bot Token으로 메시지 전송"""
    if not SLACK_BOT_TOKEN:
        raise ValueError("SLACK_BOT_TOKEN 환경변수가 설정되지 않았습니다.")

    url = "https://slack.com/api/chat.postMessage"
    headers = {
        "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "channel": SLACK_CHANNEL,
        "text": fallback_text,
        "blocks": blocks,
    }

    resp = requests.post(url, headers=headers, json=payload)
    data = resp.json()

    if not data.get("ok"):
        raise RuntimeError(f"Slack 전송 실패: {data.get('error', 'unknown')}")

    print(f"✅ Slack 전송 완료 → 채널: {SLACK_CHANNEL}")
    return data


def main():
    print("=" * 50)
    print("📊 월간 기기 현황 리포트 생성")
    print("=" * 50)

    # 1. 전월 기간 계산
    start_date, end_date, year, month = get_previous_month_range()
    print(f"\n📅 조회 기간: {start_date} ~ {end_date}")

    # 2. BigQuery 클라이언트 생성
    client = get_bq_client()

    # 3. 기기 현황 조회
    print("\n🔍 BigQuery 기기 현황 조회 중...")
    df = query_bike_stats(client, start_date, end_date)
    print(f"   조회된 행 수: {len(df)}")

    if len(df) == 0:
        print("⚠️ 조회된 데이터가 없습니다. 종료합니다.")
        return

    # 4. 월평균 계산
    stats = calculate_monthly_avg(df)
    print("\n📈 type별 월평균:")
    for type_name, values in stats.items():
        print(f"   {type_name}: 보유 {values['all_bike']:,.1f}대 / 가용 {values['av_bike']:,.1f}대")

    # 5. type 변화 내역 조회
    print("\n🔄 유형 변경 내역 조회 중...")
    change_df = query_type_changes(client, start_date, end_date)
    change_summary = build_change_summary(change_df)
    if change_summary:
        print("   변경 내역:")
        for direction, cnt in change_summary.items():
            print(f"   {direction}: {cnt}대")
    else:
        print("   변경 없음")

    # 6. Slack 메시지 구성 & 전송
    blocks = build_slack_message(stats, year, month, change_summary)
    fallback_text = f"{year}년 {month}월 기기 현황 리포트"

    print("\n📤 Slack 전송 중...")
    send_slack_message(blocks, fallback_text)

    print("\n✅ 완료!")


if __name__ == '__main__':
    main()
