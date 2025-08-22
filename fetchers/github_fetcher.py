"""
Fetchers - Responsible for fetching data from GitHub API
and saving it to the data folder for loaders to access.
"""

import os
import time
import logging
import requests
import polars as pl
from .cleaning import clean_polars_df
from datetime import datetime, timezone
from typing import Optional, Dict, List

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BaseFetcher:
    """Base class for all fetchers."""

    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)

    def _get_github_headers(self) -> Dict[str, str]:
        headers = {
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "GitHub-Analytics-Dashboard",
        }
        token = os.getenv("GITHUB_TOKEN")
        if token:
            headers["Authorization"] = f"token {token}"
        return headers

    def _make_request(self, url: str, params: Optional[Dict] = None) -> Optional[requests.Response]:
        try:
            headers = self._get_github_headers()
            response = requests.get(url, headers=headers, params=params, timeout=30)

            # Rate limit handling
            if response.status_code == 403 and "rate limit" in response.text.lower():
                reset_ts = response.headers.get("X-RateLimit-Reset")
                wait_seconds = 60
                if reset_ts:
                    try:
                        reset_time = int(reset_ts)
                        now = int(time.time())
                        wait_seconds = max(1, reset_time - now + 2)
                    except Exception:
                        wait_seconds = 60
                logger.warning(f"Rate limit exceeded. Waiting {wait_seconds} seconds...")
                time.sleep(wait_seconds)
                response = requests.get(url, headers=headers, params=params, timeout=30)

            if response.status_code == 200:
                return response
            else:
                logger.error(f"HTTP {response.status_code}: {response.text}")
                return None

        except requests.RequestException as e:
            logger.error(f"Request failed: {e}")
            return None


# --------------------------- Fetchers ---------------------------
class StarsFetcher(BaseFetcher):
    """Fetch stars over time."""

    def fetch(self, owner: str, repo: str, since_date: Optional[datetime] = None) -> pl.DataFrame:
        url = f"https://api.github.com/repos/{owner}/{repo}/stargazers"
        headers = self._get_github_headers()
        headers["Accept"] = "application/vnd.github.v3.star+json"

        page = 1
        all_rows: List[Dict] = []
        stop = False
        while not stop:
            response = requests.get(url, headers=headers, params={"per_page": 100, "page": page}, timeout=30)
            if not response or response.status_code != 200:
                break
            data = response.json()
            if not data:
                break
            for star in data:
                starred_at = star.get("starred_at")
                if not starred_at:
                    starred_at = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
                dt = datetime.fromisoformat(starred_at.replace("Z", "+00:00"))
                # Normalize to naive before comparison to avoid offset-aware vs naive errors
                sd = since_date.replace(tzinfo=None) if (since_date and since_date.tzinfo) else since_date
                if sd and dt.replace(tzinfo=None) <= sd:
                    stop = True
                    break
                all_rows.append({"date": starred_at, "count": 1})
            page += 1
            # Respect secondary rate limit via remaining header
            remaining = response.headers.get("X-RateLimit-Remaining")
            if remaining is not None and remaining == "0":
                reset_ts = response.headers.get("X-RateLimit-Reset")
                wait_seconds = 60
                if reset_ts:
                    try:
                        reset_time = int(reset_ts)
                        now = int(time.time())
                        wait_seconds = max(1, reset_time - now + 2)
                    except Exception:
                        wait_seconds = 60
                logger.warning(f"Rate limit remaining 0. Sleeping {wait_seconds}s")
                time.sleep(wait_seconds)

        if not all_rows:
            return pl.DataFrame({"date": [], "count": []})

        df = pl.DataFrame(all_rows).with_columns([
            pl.col("date").str.strptime(pl.Datetime, strict=False).dt.date().alias("date")
        ]).group_by("date").agg(pl.len().alias("count")).sort("date")
        df = clean_polars_df(df)

        return df


class ForksFetcher(BaseFetcher):
    """Fetch forks over time."""

    def fetch(self, owner: str, repo: str, since_date: Optional[datetime] = None) -> pl.DataFrame:
        url = f"https://api.github.com/repos/{owner}/{repo}/forks"
        page = 1
        all_rows: List[Dict] = []
        stop = False
        while not stop:
            response = self._make_request(url, params={"per_page": 100, "page": page, "sort": "newest"})
            if not response:
                break
            data = response.json()
            if not data:
                break
            for fork in data:
                created_at = fork.get("created_at", datetime.utcnow().replace(tzinfo=timezone.utc).isoformat())
                dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                sd = since_date.replace(tzinfo=None) if (since_date and since_date.tzinfo) else since_date
                if sd and dt.replace(tzinfo=None) <= sd:
                    stop = True
                    break
                all_rows.append({"date": created_at, "count": 1})
            page += 1
            if response.headers.get("X-RateLimit-Remaining") == "0":
                reset_ts = response.headers.get("X-RateLimit-Reset")
                wait_seconds = 60
                if reset_ts:
                    try:
                        reset_time = int(reset_ts)
                        now = int(time.time())
                        wait_seconds = max(1, reset_time - now + 2)
                    except Exception:
                        wait_seconds = 60
                logger.warning(f"Rate limit remaining 0. Sleeping {wait_seconds}s")
                time.sleep(wait_seconds)

        if not all_rows:
            return pl.DataFrame({"date": [], "count": []})

        df = pl.DataFrame(all_rows).with_columns([
            pl.col("date").str.strptime(pl.Datetime, strict=False).dt.date().alias("date")
        ]).group_by("date").agg(pl.len().alias("count")).sort("date")
        df = clean_polars_df(df)
        return df


class PRsFetcher(BaseFetcher):
    """Fetch pull requests over time."""

    def fetch(self, owner: str, repo: str, since_date: Optional[datetime] = None) -> pl.DataFrame:
        url = f"https://api.github.com/repos/{owner}/{repo}/pulls"
        page = 1
        all_rows: List[Dict] = []
        stop = False
        while not stop:
            response = self._make_request(url, params={"state": "all", "per_page": 100, "page": page, "sort": "created", "direction": "desc"})
            if not response:
                break
            data = response.json()
            if not data:
                break
            for pr in data:
                created_at = pr.get("created_at", datetime.utcnow().replace(tzinfo=timezone.utc).isoformat())
                dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                sd = since_date.replace(tzinfo=None) if (since_date and since_date.tzinfo) else since_date
                if sd and dt.replace(tzinfo=None) <= sd:
                    stop = True
                    break
                all_rows.append({"date": created_at, "count": 1})
            page += 1
            if response.headers.get("X-RateLimit-Remaining") == "0":
                reset_ts = response.headers.get("X-RateLimit-Reset")
                wait_seconds = 60
                if reset_ts:
                    try:
                        reset_time = int(reset_ts)
                        now = int(time.time())
                        wait_seconds = max(1, reset_time - now + 2)
                    except Exception:
                        wait_seconds = 60
                logger.warning(f"Rate limit remaining 0. Sleeping {wait_seconds}s")
                time.sleep(wait_seconds)

        if not all_rows:
            return pl.DataFrame({"date": [], "count": []})

        df = pl.DataFrame(all_rows).with_columns([
            pl.col("date").str.strptime(pl.Datetime, strict=False).dt.date().alias("date")
        ]).group_by("date").agg(pl.len().alias("count")).sort("date")
        df = clean_polars_df(df)
        return df


class DownloadsFetcher(BaseFetcher):
    """Fetch release download counts."""

    def fetch(self, owner: str, repo: str, since_date: Optional[datetime] = None) -> pl.DataFrame:
        url = f"https://api.github.com/repos/{owner}/{repo}/releases"
        page = 1
        rows: List[Dict] = []
        stop = False
        while not stop:
            response = self._make_request(url, params={"per_page": 100, "page": page})
            if not response:
                break
            data = response.json()
            if not data:
                break
            for release in data:
                date_val = release.get("published_at", datetime.utcnow().replace(tzinfo=timezone.utc).isoformat())
                dt = datetime.fromisoformat(date_val.replace("Z", "+00:00"))
                sd = since_date.replace(tzinfo=None) if (since_date and since_date.tzinfo) else since_date
                if sd and dt.replace(tzinfo=None) <= sd:
                    stop = True
                    break
                assets = release.get("assets", [])
                count = sum(asset.get("download_count", 0) for asset in assets)
                rows.append({"date": date_val, "count": count})
            page += 1
            if response.headers.get("X-RateLimit-Remaining") == "0":
                reset_ts = response.headers.get("X-RateLimit-Reset")
                wait_seconds = 60
                if reset_ts:
                    try:
                        reset_time = int(reset_ts)
                        now = int(time.time())
                        wait_seconds = max(1, reset_time - now + 2)
                    except Exception:
                        wait_seconds = 60
                logger.warning(f"Rate limit remaining 0. Sleeping {wait_seconds}s")
                time.sleep(wait_seconds)
        if not rows:
            return pl.DataFrame({"date": [], "count": []})

        df = pl.DataFrame(rows).with_columns([
            pl.col("date").str.strptime(pl.Datetime, strict=False).dt.date().alias("date")
        ]).group_by("date").agg(pl.col("count").sum()).sort("date")
        df = clean_polars_df(df)
        return df


# --------------------------- GitHubFetcher Wrapper ---------------------------
class GitHubFetcher:
    """Wrapper to fetch all GitHub repo data as a dict of DataFrames."""

    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.stars_fetcher = StarsFetcher(data_dir)
        self.forks_fetcher = ForksFetcher(data_dir)
        self.prs_fetcher = PRsFetcher(data_dir)
        self.downloads_fetcher = DownloadsFetcher(data_dir)

    def fetch_all(self, owner: str, repo: str) -> Dict[str, pl.DataFrame]:
        """Fetch all metrics and return as dict of DataFrames."""
        data = {}
        data["stars"] = self.stars_fetcher.fetch(owner, repo)
        data["forks"] = self.forks_fetcher.fetch(owner, repo)
        data["prs"] = self.prs_fetcher.fetch(owner, repo)
        data["downloads"] = self.downloads_fetcher.fetch(owner, repo)
        return data

    def get_repo_created_at(self, owner: str, repo: str) -> Optional[datetime]:
        """Fetch repository metadata and return created_at as datetime (UTC)."""
        url = f"https://api.github.com/repos/{owner}/{repo}"
        response = self.stars_fetcher._make_request(url)
        if not response:
            return None
        try:
            info = response.json()
            created_at = info.get("created_at")
            if not created_at:
                return None
            return datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        except Exception:
            return None