import os
import logging
from datetime import timedelta
from pathlib import Path
import pendulum
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, task, Param
import duckdb



log = logging.getLogger(__name__)

PROJECT_DIR = Path(__file__).resolve().parent.parent
BUCKET = os.environ.get("HONEYPOT_BUCKET", "sundew-honeypot-diana")
RAW_PREFIX = "raw/"                       # zona raw a lake-ului: json.gz exact cum vine de la honeypot
BRONZE_PREFIX = "bronze/cowrie_events/"   # zona bronze a lake-ului: Parquet partitionat pe dt
AWS_CONN_ID = "aws_default"
DUCKDB_PATH = os.environ.get("DUCKDB_PATH", str(PROJECT_DIR / "warehouse.duckdb"))

def list_days(hook, prefix):
    """Zilele (dt=YYYY-MM-DD) care exista sub un prefix din S3."""
    prefixes = hook.list_prefixes(bucket_name=BUCKET, prefix=prefix, delimiter="/") or []
    return {p.rstrip("/").split("dt=")[-1] for p in prefixes if "dt=" in p}


def connect_s3(hook, path=":memory:"):
    """Conexiune DuckDB care poate citi/scrie s3:// cu credentialele din conexiunea Airflow."""
    con = duckdb.connect(path)
    con.execute("INSTALL httpfs; LOAD httpfs;")
    creds = hook.get_credentials()
    region = hook.conn_region_name or os.environ.get("AWS_REGION", "eu-north-1")
    token = f", SESSION_TOKEN '{creds.token}'" if creds.token else ""
    con.execute(f"""
        CREATE OR REPLACE SECRET s3 (
            TYPE s3,
            KEY_ID '{creds.access_key}',
            SECRET '{creds.secret_key}'{token},
            REGION '{region}'
        )
    """)
    return con


@dag(
    dag_id="honeypot_dag",
    schedule="0 4 * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    params={
        # true = reconverteste toate zilele din raw/ in Parquet (de ex. dupa ce se schimba logica din convert)
        "full_refresh": Param(False, type="boolean"),
    },
    tags=["honeypot"],
)


def honeypot_dag():

    @task()
    def convert(**context):
        """raw/ (json.gz) -> bronze/ (Parquet), totul in S3, fara download local."""
        full_refresh = context["params"]["full_refresh"]
        hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        raw_days = list_days(hook, RAW_PREFIX)
        bronze_days = set() if full_refresh else list_days(hook, BRONZE_PREFIX)
        todo = sorted(raw_days - bronze_days)
        log.info(f"CONVERT: full_refresh={full_refresh}, {len(raw_days)} zile in raw, de convertit: {todo}")

        con = connect_s3(hook)
        for day in todo:
            src = f"s3://{BUCKET}/{RAW_PREFIX}dt={day}/cowrie.json.gz"
            out = f"s3://{BUCKET}/{BRONZE_PREFIX}dt={day}/events.parquet"
            # un PUT in S3 e atomic: ori apare fisierul complet, ori deloc, deci nu mai e nevoie de .part
            con.execute(f"""
                COPY (
                    -- DuckDB detecteaza singur toate campurile si tipurile lor (sample_size=-1: citeste tot fisierul).
                    -- hive_partitioning=false: altfel ar adauga inca o coloana dt din path-ul dt=.../
                    SELECT DATE '{day}' AS dt, *
                    FROM read_json('{src}', format = 'newline_delimited', sample_size = -1, hive_partitioning = false)
                ) TO '{out}' (FORMAT parquet, COMPRESSION zstd)
            """)
            log.info(f"CONVERT: dt={day} -> {out}")

        con.close()
        log.info(f"CONVERT: {len(todo)} zile convertite: {todo}")

    @task()
    def load():
        """bronze/ din S3 -> tabela bronze.cowrie_events din warehouse (reconstruita complet la fiecare rulare)."""
        hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        con = connect_s3(hook, DUCKDB_PATH)
        con.execute("CREATE SCHEMA IF NOT EXISTS bronze")

        # Zilele pot avea coloane diferite (ex. o zi fara download-uri n-are coloana url),
        # asa ca citim toate fisierele odata cu union_by_name: coloanele lipsa devin NULL.
        # CREATE OR REPLACE e atomic: daca pica, tabela veche ramane neatinsa.
        src = f"s3://{BUCKET}/{BRONZE_PREFIX}dt=*/events.parquet"
        con.execute(f"""
            CREATE OR REPLACE TABLE bronze.cowrie_events AS
            SELECT * FROM read_parquet('{src}', union_by_name = true, hive_partitioning = false)
        """)

        days, rows, cols = con.execute("""
            SELECT count(DISTINCT dt), count(*), (SELECT count(*) FROM duckdb_columns()
                WHERE schema_name = 'bronze' AND table_name = 'cowrie_events')
            FROM bronze.cowrie_events
        """).fetchone()
        log.info(f"LOAD: bronze.cowrie_events = {days} zile, {rows} evenimente, {cols} coloane")
        con.close()

    @task()
    def validate():
        log.info("Validating data...")


    convert() >> load() >> validate()



honeypot_dag()
