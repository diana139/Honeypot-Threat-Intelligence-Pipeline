import os
from datetime import datetime, timedelta
from pathlib import Path
import pendulum
from airflow.providers.amazon.aws.operators.s3 import S3Hook
from airflow.sdk import DAG, task


log = logging.getLogger(__name__)

PROJECT_DIR = Path(__file__).resolve().parent.parent
LANDING_DIR = PROJECT_DIR / "landing"
BUCKET = os.environ.get("HONEYPOT_BUCKET", "sundew-honeypot-diana")
S3_PREFIX = "raw/"
AWS_CONN_ID = "aws_default"

@dag(
    dag_id="honeypot_dag",
    schedule="0 4 * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["honeypot"],
)


def honeypot_dag():

    @task()
    def extract():
        hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        prefixes = hook.list_keys(bucket_name=BUCKET, prefix=S3_PREFIX, delimiter="/") or []
        s3_days = sorted(p.rstrip("/").split("dt=")[-1] for p in prefixes if "dt=" in p)
        log.info(f"Found {len(s3_days)} days in S3: {s3_days}")

        downloaded = []
        for day in s3_days:
            



    @task()
    def trasnform(data):
        log.info("Transforming data...")

    @task()
    def load(data):
        log.info("Loading data into the database...")

    @task()
    def validate(data):
        log.info("Validating data...")


    extract() >> trasnform() >> load() >> validate()