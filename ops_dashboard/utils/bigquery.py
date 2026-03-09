from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import os

# 서비스 계정 JSON 경로
KEY_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "key.json")

# BigQuery + Drive + Sheets 권한 scopes
SCOPES = [
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/drive",  # Sheets 읽기/쓰기 위해 full 권한
    "https://www.googleapis.com/auth/spreadsheets",  # Sheets API
]

# BigQuery 클라이언트 생성
credentials = service_account.Credentials.from_service_account_file(KEY_PATH, scopes=SCOPES)
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

def run_query(query: str) -> pd.DataFrame:
    """
    BigQuery에서 SQL 쿼리를 실행하여 Pandas DataFrame으로 반환.
    """
    query_job = client.query(query)
    df = query_job.result().to_dataframe()
    return df
