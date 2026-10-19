import argparse
import logging
import os

from enrichment.base import Enricher, EnricherAuthError
from enrichment.ipinfo import IPInfoEnricher
from enrichment.store import EnrichmentStore

log = logging.getLogger(__name__)

# a new source = a new class here, nothing else changes
ENRICHERS: dict[str, type[Enricher]] = {
    IPInfoEnricher.name: IPInfoEnricher,
}

BATCH_SIZE = 500   # save in batches, so an interrupted run keeps what it already has


def enrich(enricher: Enricher, store: EnrichmentStore, limit: int | None = None) -> int:
    """Looks up only the missing IPs and saves them. Returns how many were saved."""
    ips = store.pending_ips(enricher)[:limit]
    log.info("%s: %d new IPs to enrich", enricher.name, len(ips))

    saved = 0
    for i in range(0, len(ips), BATCH_SIZE):
        results = enricher.lookup_many(ips[i:i + BATCH_SIZE])
        store.save(enricher, results)
        saved += len(results)
        log.info("%s: %d/%d saved", enricher.name, saved, len(ips))
    return saved


def run_all(db_path: str, sources: list[str] | None = None, limit: int | None = None):
    with EnrichmentStore(db_path) as store:
        for name in sources or ENRICHERS:
            enricher_cls = ENRICHERS[name]
            store.ensure_table(enricher_cls)
            enrich(enricher_cls(), store, limit)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", default=os.environ.get("DUCKDB_PATH", "warehouse.duckdb"))
    parser.add_argument("--source", action="append", choices=ENRICHERS, help="default: all")
    parser.add_argument("--limit", type=int, help="only the first N IPs (for testing)")
    args = parser.parse_args()

    # run by hand: read IPINFO_TOKEN from .env (in Airflow it comes from docker-compose)
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        run_all(args.db, args.source, args.limit)
    except EnricherAuthError as e:
        parser.exit(1, f"Error: {e}\n")


if __name__ == "__main__":
    main()
