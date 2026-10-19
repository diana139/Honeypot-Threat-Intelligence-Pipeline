from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
import logging

import requests
from requests.adapters import HTTPAdapter, Retry

log = logging.getLogger(__name__)


class EnricherAuthError(RuntimeError):
    """Missing or invalid token: no point in trying the remaining IPs."""


def make_session(headers: dict, pool_size: int = 10) -> requests.Session:
    session = requests.Session()
    session.headers.update(headers)
    session.mount("https://", HTTPAdapter(
        pool_maxsize=pool_size,
        max_retries=Retry(total=5, backoff_factor=1,
                          status_forcelist=[429, 500, 502, 503, 504]),
    ))
    return session


class Enricher(ABC):
    """A source of data about IPs. Each source has its own table: enrichment.<name>."""
    name: str
    fields: tuple[str, ...]   # the columns lookup() returns, besides ip
    workers: int = 10

    @abstractmethod
    def lookup(self, ip: str) -> dict:
        """Returns the data for one IP, or {} if the source doesn't know it."""

    def _safe_lookup(self, ip: str) -> dict | None:
        """None = the lookup failed (retried on the next run), {} = the source doesn't know the IP."""
        try:
            return self.lookup(ip)
        except EnricherAuthError:
            raise
        except Exception as e:
            log.warning("%s: lookup failed for %s: %s", self.name, ip, e)
            return None

    def lookup_many(self, ips: list[str]) -> dict[str, dict]:
        """Only the IPs that got an answer; failed lookups are left out."""
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            results = dict(zip(ips, pool.map(self._safe_lookup, ips)))
        return {ip: data for ip, data in results.items() if data is not None}
