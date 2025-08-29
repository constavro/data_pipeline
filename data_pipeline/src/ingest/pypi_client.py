from __future__ import annotations
import requests
from typing import Optional

class PyPIClient:
    # Community API for PyPI download stats
    BASE = "https://pypistats.org/api"

    def __init__(self, timeout: int = 20):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "scaletech-pipeline/1.0"})
        self.timeout = timeout

    def recent(self, package: str) -> dict | None:
        url = f"{self.BASE}/packages/{package}/recent"
        resp = self.session.get(url, timeout=self.timeout)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json().get("data", None)
