from __future__ import annotations
import os
import time
from typing import Any, Optional
import requests

class GitHubClient:
    BASE = "https://api.github.com"

    def __init__(self, token: Optional[str] = None, timeout: int = 20, max_retries: int = 3, backoff: float = 1.5):
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/vnd.github+json",
            "User-Agent": "scaletech-pipeline/1.0"
        })
        token = token or os.getenv("GITHUB_TOKEN")
        if token:
            print(token)
            self.session.headers.update({"Authorization": f"Bearer {token}"})
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff = backoff

    def _request(self, method: str, path: str, params: dict | None = None) -> Any:
        url = f"{self.BASE}{path}"
        last_exc = None
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.request(method, url, params=params, timeout=self.timeout)
                if resp.status_code == 403 and 'X-RateLimit-Remaining' in resp.headers and resp.headers.get('X-RateLimit-Remaining') == '0':
                    # Rate limited: sleep until reset
                    reset = int(resp.headers.get('X-RateLimit-Reset', '0'))
                    max_req = int(resp.headers.get('x-ratelimit-limit'))
                    print(max_req)
                    wait = max(reset - int(time.time()) + 1, 5)
                    print(wait)
                    time.sleep(wait)
                    continue
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return resp.json()
            except requests.RequestException as e:
                last_exc = e
                if attempt == self.max_retries:
                    raise
                time.sleep(self.backoff ** attempt)
        if last_exc:
            raise last_exc

    def get_repo(self, repo_full_name: str) -> dict | None:
        return self._request("GET", f"/repos/{repo_full_name}")

    def get_latest_release(self, repo_full_name: str) -> dict | None:
        return self._request("GET", f"/repos/{repo_full_name}/releases/latest")

    def get_latest_commit_datetime(self, repo_full_name: str, branch: str | None) -> str | None:
        sha = branch or None
        data = self._request("GET", f"/repos/{repo_full_name}/commits", params={"per_page": 1, **({"sha": sha} if sha else {})})
        if isinstance(data, list) and data:
            commit = data[0].get("commit", {})
            # Prefer author date, fallback to committer
            return commit.get("author", {}).get("date") or commit.get("committer", {}).get("date")
        return None
