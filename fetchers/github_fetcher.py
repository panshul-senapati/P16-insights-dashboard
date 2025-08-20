"""
Fetchers - Responsible for fetching data from GitHub API
and saving it to the data folder for loaders to access.
"""

import os
import time
import logging
import requests
import pandas as pd
from datetime import datetime
from typing import Optional, Dict

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
                logger.warning("Rate limit exceeded. Waiting 60 seconds...")
                time.sleep(60)
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

    def fetch(self, owner: str, repo: str) -> pd.DataFrame:
        url = f"https://api.github.com/repos/{owner}/{repo}/stargazers"
        headers = self._get_github_headers()
        headers["Accept"] = "application/vnd.github.v3.star+json"  # To get starred_at
        response = requests.get(url, headers=headers, params={"per_page": 100})
        if not response or response.status_code != 200:
            return pd.DataFrame()

        data = response.json()
        # Build DataFrame with date and count
        df = pd.DataFrame([{"date": star.get("starred_at", datetime.utcnow()), "count": 1} for star in data])
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
            df = df.groupby("date").count().reset_index()
            df.to_csv(os.path.join(self.data_dir, f"{repo}_stars.csv"), index=False)
            logger.info(f"Saved stars data → {self.data_dir}/{repo}_stars.csv")
        return df


class ForksFetcher(BaseFetcher):
    """Fetch forks over time."""

    def fetch(self, owner: str, repo: str) -> pd.DataFrame:
        url = f"https://api.github.com/repos/{owner}/{repo}/forks"
        response = self._make_request(url, params={"per_page": 100})
        if not response:
            return pd.DataFrame()
        data = response.json()
        df = pd.DataFrame([{"date": fork.get("created_at", datetime.utcnow()), "count": 1} for fork in data])
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
            df = df.groupby("date").count().reset_index()
            df.to_csv(os.path.join(self.data_dir, f"{repo}_forks.csv"), index=False)
            logger.info(f"Saved forks data → {self.data_dir}/{repo}_forks.csv")
        return df


class PRsFetcher(BaseFetcher):
    """Fetch pull requests over time."""

    def fetch(self, owner: str, repo: str) -> pd.DataFrame:
        url = f"https://api.github.com/repos/{owner}/{repo}/pulls"
        response = self._make_request(url, params={"state": "all", "per_page": 100})
        if not response:
            return pd.DataFrame()
        data = response.json()
        df = pd.DataFrame([{"date": pr.get("created_at", datetime.utcnow()), "count": 1} for pr in data])
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
            df = df.groupby("date").count().reset_index()
            df.to_csv(os.path.join(self.data_dir, f"{repo}_prs.csv"), index=False)
            logger.info(f"Saved PRs data → {self.data_dir}/{repo}_prs.csv")
        return df


class DownloadsFetcher(BaseFetcher):
    """Fetch release download counts."""

    def fetch(self, owner: str, repo: str) -> pd.DataFrame:
        url = f"https://api.github.com/repos/{owner}/{repo}/releases"
        response = self._make_request(url, params={"per_page": 100})
        if not response:
            return pd.DataFrame()
        data = response.json()
        rows = []
        for release in data:
            date = release.get("published_at", datetime.utcnow())
            assets = release.get("assets", [])
            count = sum(asset.get("download_count", 0) for asset in assets)
            rows.append({"date": date, "count": count})
        df = pd.DataFrame(rows)
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
            df.to_csv(os.path.join(self.data_dir, f"{repo}_downloads.csv"), index=False)
            logger.info(f"Saved downloads data → {self.data_dir}/{repo}_downloads.csv")
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

    def fetch_all(self, owner: str, repo: str) -> Dict[str, pd.DataFrame]:
        """Fetch all metrics and return as dict of DataFrames."""
        data = {}
        data["stars"] = self.stars_fetcher.fetch(owner, repo)
        data["forks"] = self.forks_fetcher.fetch(owner, repo)
        data["prs"] = self.prs_fetcher.fetch(owner, repo)
        data["downloads"] = self.downloads_fetcher.fetch(owner, repo)
        return data