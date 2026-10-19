import os
import logging
import subprocess
from datetime import timedelta
from pathlib import Path
import pendulum
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, task, Param
import duckdb



log = logging.getLogger(__name__)

PROJECT_DIR = Path(__file__).resolve().parent.parent
BUCKET = os.environ.get("HONEYPOT_BUCKET", "sundew-honeypot-diana")
RAW_PREFIX = "raw/"                       # raw zone of the lake: json.gz exactly as the honeypot writes it
BRONZE_PREFIX = "bronze/cowrie_events/"   # bronze zone of the lake: Parquet partitioned by dt
AWS_CONN_ID = "aws_default"
DUCKDB_PATH = os.environ.get("DUCKDB_PATH", str(PROJECT_DIR / "warehouse.duckdb"))
DBT_DIR = PROJECT_DIR / "dbt_project"
DBT_BIN = os.environ.get("DBT_BIN", "dbt")   # in the image, dbt lives in its own venv (see Dockerfile)

def run_dbt(*command, min_ips=None):
    """Runs a dbt command on dbt_project/; fails the task if dbt fails."""
    full_command = [DBT_BIN, *command, "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    if min_ips is not None:
        full_command += ["--vars", f"{{campaign_min_ips: {min_ips}}}"]
    log.info(f"DBT: {' '.join(full_command)}")

    # dbt gets DUCKDB_PATH from the task's environment
    result = subprocess.run(full_command, capture_output=True, text=True)
    log.info(result.stdout)
    if result.stderr:
        log.info(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"dbt {' '.join(command)} failed with exit code {result.returncode}")


def list_days(hook, prefix):
    """The days (dt=YYYY-MM-DD) that exist under an S3 prefix."""
    prefixes = hook.list_prefixes(bucket_name=BUCKET, prefix=prefix, delimiter="/") or []
    return {p.rstrip("/").split("dt=")[-1] for p in prefixes if "dt=" in p}


def connect_s3(hook, path=":memory:"):
    """A DuckDB connection that can read/write s3:// with the credentials from the Airflow connection."""
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
        # true = reconvert every day in raw/ to Parquet and reload the whole warehouse
        # (e.g. after the convert logic changes, or when a new column breaks the incremental load)
        "full_refresh": Param(False, type="boolean"),
        # how many different IPs a script must be seen from to count as a campaign
        "campaign_min_ips": Param(10, type="integer", minimum=2),
    },
    tags=["honeypot"],
)


def honeypot_dag():

    @task()
    def convert(**context):
        """raw/ (json.gz) -> bronze/ (Parquet), entirely in S3, nothing downloaded locally."""
        full_refresh = context["params"]["full_refresh"]
        hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        raw_days = list_days(hook, RAW_PREFIX)
        bronze_days = set() if full_refresh else list_days(hook, BRONZE_PREFIX)
        todo = sorted(raw_days - bronze_days)
        log.info(f"CONVERT: full_refresh={full_refresh}, {len(raw_days)} days in raw, to convert: {todo}")

        con = connect_s3(hook)
        for day in todo:
            src = f"s3://{BUCKET}/{RAW_PREFIX}dt={day}/cowrie.json.gz"
            out = f"s3://{BUCKET}/{BRONZE_PREFIX}dt={day}/events.parquet"
            # an S3 PUT is atomic: the file appears complete or not at all, so no .part file is needed
            con.execute(f"""
                COPY (
                    -- DuckDB detects all fields and their types (sample_size=-1: reads the whole file).
                    -- hive_partitioning=false: otherwise it would add a second dt column from the dt=.../ path
                    SELECT DATE '{day}' AS dt, *
                    FROM read_json('{src}', format = 'newline_delimited', sample_size = -1, hive_partitioning = false)
                ) TO '{out}' (FORMAT parquet, COMPRESSION zstd)
            """)
            log.info(f"CONVERT: dt={day} -> {out}")

        con.close()
        log.info(f"CONVERT: {len(todo)} days converted: {todo}")

    @task()
    def load(**context):
        """bronze/ in S3 -> bronze.cowrie_events in the warehouse: only the days after the last one loaded."""
        full_refresh = context["params"]["full_refresh"]
        hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        con = connect_s3(hook, DUCKDB_PATH)
        con.execute("CREATE SCHEMA IF NOT EXISTS bronze")

        # watermark: the last day in the warehouse (None if the table doesn't exist yet, or on full_refresh)
        last_day = None
        exists = con.execute("""
            SELECT count(*) FROM duckdb_tables() WHERE schema_name = 'bronze' AND table_name = 'cowrie_events'
        """).fetchone()[0]
        if exists and not full_refresh:
            last_day = con.execute("SELECT max(dt) FROM bronze.cowrie_events").fetchone()[0]

        if last_day is None:
            src = f"s3://{BUCKET}/{BRONZE_PREFIX}dt=*/events.parquet"
            log.info(f"LOAD: loading everything from {src}")
            con.execute(f"""
                CREATE OR REPLACE TABLE bronze.cowrie_events AS
                SELECT * FROM read_parquet('{src}', union_by_name = true, hive_partitioning = false)
            """)
        else:
            todo = sorted(day for day in list_days(hook, BRONZE_PREFIX) if day > str(last_day))
            log.info(f"LOAD: last day in the warehouse = {last_day}, to load: {todo}")
            if todo:
                src = [f"s3://{BUCKET}/{BRONZE_PREFIX}dt={day}/events.parquet" for day in todo]
                con.execute(f"""
                    INSERT INTO bronze.cowrie_events BY NAME
                    SELECT * FROM read_parquet({src}, union_by_name = true, hive_partitioning = false)
                """)

        days, rows, cols = con.execute("""
            SELECT count(DISTINCT dt), count(*), (SELECT count(*) FROM duckdb_columns()
                WHERE schema_name = 'bronze' AND table_name = 'cowrie_events')
            FROM bronze.cowrie_events
        """).fetchone()
        log.info(f"LOAD: bronze.cowrie_events = {days} days, {rows} events, {cols} columns")
        con.close()

    @task()
    def transform_staging():
        """bronze -> silver: stg_cowrie_events."""
        run_dbt("run", "--select", "staging")

    @task()
    def enrich_ips():
        """New IPs from silver -> enrichment.ipinfo (country, ASN)."""
        from enrichment.base import EnricherAuthError
        from enrichment.run import ENRICHERS, enrich
        from enrichment.store import EnrichmentStore

        with EnrichmentStore(DUCKDB_PATH) as store:
            for enricher_cls in ENRICHERS.values():
                store.ensure_table(enricher_cls)
                try:
                    saved = enrich(enricher_cls(), store)
                except EnricherAuthError as e:
                    log.warning(f"ENRICH: {enricher_cls.name} skipped: {e}")
                    continue
                log.info(f"ENRICH: {enricher_cls.name} = {saved} new IPs")

    @task()
    def transform_marts(**context):
        """silver -> gold: the dimensions, fct_events, fct_sessions, campaigns."""
        run_dbt("run", "--select", "marts", min_ips=context["params"]["campaign_min_ips"])

    @task()
    def validate(**context):
        """All dbt tests (staging, gold and the reconciliations in dbt_project/tests/)."""
        run_dbt("test", min_ips=context["params"]["campaign_min_ips"])

    @task()
    def dbt_docs_generate():
        """The dbt documentation (target/index.html), viewable with `dbt docs serve`."""
        run_dbt("docs", "generate")


    convert() >> load() >> transform_staging() >> enrich_ips() >> transform_marts() >> validate() >> dbt_docs_generate()



honeypot_dag()
