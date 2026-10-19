import logging

import duckdb

from enrichment.base import Enricher

log = logging.getLogger(__name__)

SCHEMA = "enrichment"
IP_SOURCE = "silver.stg_cowrie_events"   # where the IPs to enrich come from


class EnrichmentStore:
    """The enrichment.<source> tables in the warehouse: one row per IP, filled once (a cache)."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.con = None

    def __enter__(self):
        self.con = duckdb.connect(self.db_path)
        self.con.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
        return self

    def __exit__(self, *exc):
        self.con.close()

    def table(self, enricher: type[Enricher] | Enricher) -> str:
        return f"{SCHEMA}.{enricher.name}"

    def ensure_table(self, enricher: type[Enricher] | Enricher):
        """Created even without a token, so dbt always has the source it joins."""
        columns = ", ".join(f"{field} VARCHAR" for field in enricher.fields)
        self.con.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.table(enricher)} (
                ip VARCHAR PRIMARY KEY,
                {columns},
                enriched_at TIMESTAMP DEFAULT current_timestamp
            )
        """)

    def pending_ips(self, enricher: Enricher) -> list[str]:
        """Honeypot IPs that are not in the source's table yet."""
        rows = self.con.execute(f"""
            SELECT DISTINCT src_ip FROM {IP_SOURCE}
            WHERE src_ip NOT IN (SELECT ip FROM {self.table(enricher)})
            ORDER BY src_ip
        """).fetchall()
        return [ip for (ip,) in rows]

    def save(self, enricher: Enricher, results: dict[str, dict]):
        """Unknown IPs ({}) are saved too, with NULLs, so they aren't looked up again."""
        if not results:
            return
        columns = ", ".join(("ip", *enricher.fields))
        placeholders = ", ".join("?" * (len(enricher.fields) + 1))
        rows = [(ip, *(data.get(field) for field in enricher.fields)) for ip, data in results.items()]
        self.con.executemany(
            f"INSERT OR REPLACE INTO {self.table(enricher)} ({columns}) VALUES ({placeholders})", rows
        )
