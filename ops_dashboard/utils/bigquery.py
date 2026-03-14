from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import os

# 인증 설정 (환경변수에서 로드)
CREDENTIALS_PATH = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

SCOPES = [
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=SCOPES)
client = bigquery.Client(credentials=credentials, project=os.environ.get("GCP_PROJECT_ID"))

def run_query(query: str) -> pd.DataFrame:
    """
    BigQuery에서 SQL 쿼리를 실행하여 Pandas DataFrame으로 반환.
    """
    query_job = client.query(query)
    df = query_job.result().to_dataframe()
    return df
