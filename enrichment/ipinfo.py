import os

from enrichment.base import Enricher, EnricherAuthError, make_session


class IPInfoEnricher(Enricher):
    """IPinfo Lite: country and ASN, free. Get a token at https://ipinfo.io/signup."""
    name = "ipinfo"
    fields = ("country_code", "country", "continent", "asn", "as_name", "as_domain")
    workers = 20

    def __init__(self, token: str | None = None):
        token = token or os.environ.get("IPINFO_TOKEN")
        if not token:
            raise EnricherAuthError("IPINFO_TOKEN is not set")
        self.session = make_session(
            {"Authorization": f"Bearer {token}"},
            pool_size=self.workers,
        )

    def lookup(self, ip: str) -> dict:
        r = self.session.get(f"https://api.ipinfo.io/lite/{ip}", timeout=10)

        if r.status_code in (401, 403):
            raise EnricherAuthError("IPinfo token is invalid or missing")
        if r.status_code == 404:
            return {}
        r.raise_for_status()

        data = r.json()
        return {field: data.get(field) for field in self.fields}
