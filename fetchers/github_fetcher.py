import os
import time
import logging
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests


class BaseFetcher:
    """
    Shared HTTP logic for GitHub API access with optional authentication and
    simple rate limit handling (sleep-and-retry on 403 due to rate limit).
    """

    def __init__(self, per_page: int = 100, max_pages: int = 1000, request_timeout_s: int = 30):
        self.github_token = os.getenv("GITHUB_TOKEN")
        self.headers = {
            "Accept": "application/vnd.github+json",
        }
        if self.github_token:
            self.headers["Authorization"] = f"token {self.github_token}"
        self.per_page = per_page
        self.max_pages = max_pages
        self.request_timeout_s = request_timeout_s

    def _request(self, url: str, params: Optional[Dict] = None, extra_headers: Optional[Dict] = None) -> requests.Response:
        headers = dict(self.headers)
        if extra_headers:
            headers.update(extra_headers)

        backoff_s = 2
        max_retries = 3  # Limit retries to prevent infinite loops
        
        for attempt in range(max_retries):
            resp = requests.get(url, headers=headers, params=params, timeout=self.request_timeout_s)
            if resp.status_code == 403:
                # Possibly rate-limited; attempt to wait until reset if provided
                reset = resp.headers.get("X-RateLimit-Reset")
                now = int(time.time())
                if reset and reset.isdigit():
                    wait_s = max(0, int(reset) - now) + 1
                    # Cap wait time to prevent very long sleeps
                    wait_s = min(wait_s, 10)  # Max 10 seconds wait
                    logging.info(f"Rate limited, waiting {wait_s}s (attempt {attempt + 1}/{max_retries})")
                    time.sleep(wait_s)
                else:
                    wait_s = min(backoff_s, 5)  # Cap backoff to 5 seconds
                    logging.info(f"Rate limited, backoff wait {wait_s}s (attempt {attempt + 1}/{max_retries})")
                    time.sleep(wait_s)
                    backoff_s = min(backoff_s * 2, 5)
                
                if attempt == max_retries - 1:
                    logging.warning(f"Max retries ({max_retries}) reached for {url}")
                    break
                continue
            return resp
        
        # If we get here, all retries failed
        logging.error(f"Failed to fetch {url} after {max_retries} attempts")
        return resp  # Return the last response even if it's an error

    @staticmethod
    def _to_date(date_str: str) -> Optional[pd.Timestamp]:
        try:
            return pd.to_datetime(date_str, utc=True).tz_localize(None).normalize()
        except Exception:
            return None


class GitHubGraphQL:
    """Minimal GitHub GraphQL v4 client with pagination helpers."""

    def __init__(self, request_timeout_s: int = 30):
        self.endpoint = "https://api.github.com/graphql"
        token = os.getenv("GITHUB_TOKEN")
        self.headers = {
            "Accept": "application/vnd.github+json",
            "Content-Type": "application/json",
        }
        if token:
            self.headers["Authorization"] = f"bearer {token}"
        self.request_timeout_s = request_timeout_s

    def query(self, query: str, variables: Optional[Dict] = None) -> Dict:
        payload = {"query": query, "variables": variables or {}}
        resp = requests.post(self.endpoint, json=payload, headers=self.headers, timeout=self.request_timeout_s)
        if resp.status_code != 200:
            logging.warning("GraphQL non-200: %s", resp.status_code)
            return {}
        data = resp.json() or {}
        if "errors" in data:
            logging.warning("GraphQL errors: %s", data.get("errors"))
        return data.get("data", {})


class StarsFetcher(BaseFetcher):
    """
    Fetches stargazer events with timestamps and returns daily stars by date.
    CSV columns: date, stars (daily count, not cumulative)
    """

    def __init__(self, per_page: int = 100, max_pages: int = 1000, request_timeout_s: int = 30):
        super().__init__(per_page, max_pages, request_timeout_s)
        self.graphql_client = GitHubGraphQL(request_timeout_s)

    def fetch(self, owner: str, repo: str) -> pd.DataFrame:
        """Fetch daily star counts (not cumulative)."""
        if os.getenv("P16_USE_GRAPHQL") == "1":
            return self.fetch_graphql(owner, repo)
        return self.fetch_rest(owner, repo)

    def fetch_rest(self, owner: str, repo: str) -> pd.DataFrame:
        """Fetch stars using REST API - returns daily counts."""
        url = f"https://api.github.com/repos/{owner}/{repo}/stargazers"
        all_stars = []
        page = 1
        
        while page <= self.max_pages:
            params = {"per_page": self.per_page, "page": page}
            resp = self._request(url, params=params)
            
            if resp.status_code != 200:
                logging.error(f"Failed to fetch stars: {resp.status_code} - {resp.text}")
                break
                
            stars = resp.json()
            if not stars:
                break
                
            for star in stars:
                starred_at = star.get("starred_at")
                if starred_at:
                    date = self._to_date(starred_at)
                    if date:
                        all_stars.append({"date": date, "stars": 1})
            
            page += 1
        
        if not all_stars:
            return pd.DataFrame(columns=["date", "stars"])
        
        # Convert to daily counts (not cumulative)
        df = pd.DataFrame(all_stars)
        df = df.groupby("date")["stars"].sum().reset_index()
        df = df.sort_values("date")
        
        return df

    def fetch_graphql(self, owner: str, repo: str) -> pd.DataFrame:
        """Fetch stars using GraphQL API - returns daily counts."""
        query = """
        query($owner: String!, $repo: String!, $cursor: String) {
            repository(owner: $owner, name: $repo) {
                stargazers(first: 100, after: $cursor) {
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                    edges {
                        starredAt
                    }
                }
            }
        }
        """
        
        all_stars = []
        cursor = None
        
        while True:
            variables = {"owner": owner, "repo": repo, "cursor": cursor}
            data = self.graphql_client.query(query, variables)
            
            if not data or "repository" not in data:
                logging.error("GraphQL query failed for stars")
                break
                
            repo_data = data["repository"]
            if not repo_data or "stargazers" not in repo_data:
                break
                
            stargazers = repo_data["stargazers"]
            edges = stargazers.get("edges", [])
            
            for edge in edges:
                starred_at = edge.get("starredAt")
                if starred_at:
                    date = self._to_date(starred_at)
                    if date:
                        all_stars.append({"date": date, "stars": 1})
            
            page_info = stargazers.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
                
            cursor = page_info.get("endCursor")
            if not cursor:
                break
        
        if not all_stars:
            return pd.DataFrame(columns=["date", "stars"])
        
        # Convert to daily counts (not cumulative)
        df = pd.DataFrame(all_stars)
        df = df.groupby("date")["stars"].sum().reset_index()
        df = df.sort_values("date")
        
        return df


class ForksFetcher(BaseFetcher):
    """
    Fetches forks and returns daily fork counts by creation date.
    CSV columns: date, forks (daily count, not cumulative)
    """

    def __init__(self, per_page: int = 100, max_pages: int = 1000, request_timeout_s: int = 30):
        super().__init__(per_page, max_pages, request_timeout_s)
        self.graphql_client = GitHubGraphQL(request_timeout_s)

    def fetch(self, owner: str, repo: str) -> pd.DataFrame:
        """Fetch daily fork counts (not cumulative)."""
        if os.getenv("P16_USE_GRAPHQL") == "1":
            return self.fetch_graphql(owner, repo)
        return self.fetch_rest(owner, repo)

    def fetch_rest(self, owner: str, repo: str) -> pd.DataFrame:
        """Fetch forks using REST API - returns daily counts."""
        url = f"https://api.github.com/repos/{owner}/{repo}/forks"
        all_forks = []
        page = 1
        
        while page <= self.max_pages:
            params = {"per_page": self.per_page, "page": page, "sort": "newest"}
            resp = self._request(url, params=params)
            
            if resp.status_code != 200:
                logging.error(f"Failed to fetch forks: {resp.status_code} - {resp.text}")
                break
                
            forks = resp.json()
            if not forks:
                break
                
            for fork in forks:
                created_at = fork.get("created_at")
                if created_at:
                    date = self._to_date(created_at)
                    if date:
                        all_forks.append({"date": date, "forks": 1})
            
            page += 1
        
        if not all_forks:
            return pd.DataFrame(columns=["date", "forks"])
        
        # Convert to daily counts (not cumulative)
        df = pd.DataFrame(all_forks)
        df = df.groupby("date")["forks"].sum().reset_index()
        df = df.sort_values("date")
        
        return df

    def fetch_graphql(self, owner: str, repo: str) -> pd.DataFrame:
        gql = GitHubGraphQL()
        query = """
        query($owner: String!, $name: String!, $cursor: String) {
          repository(owner: $owner, name: $name) {
            forks(first: 100, after: $cursor, orderBy: {field: CREATED_AT, direction: ASC}) {
              nodes { createdAt }
              pageInfo { endCursor hasNextPage }
            }
          }
        }
        """
        dates: List[pd.Timestamp] = []
        cursor = None
        for _ in range(200):
            data = gql.query(query, {"owner": owner, "name": repo, "cursor": cursor})
            forks = (((data or {}).get("repository") or {}).get("forks") or {})
            nodes = forks.get("nodes") or []
            for n in nodes:
                ts = self._to_date(n.get("createdAt"))
                if ts is not None:
                    dates.append(ts)
            page = forks.get("pageInfo") or {}
            if not page.get("hasNextPage"):
                break
            cursor = page.get("endCursor")
        if not dates:
            return pd.DataFrame(columns=["date", "forks"])
        df = pd.DataFrame({"date": dates, "delta": 1})
        daily = df.groupby("date", as_index=False)["delta"].sum().sort_values("date")
        daily["forks"] = daily["delta"].cumsum()
        return daily[["date", "forks"]]


class PRsFetcher(BaseFetcher):
    """
    Fetches pull requests (state=all) and aggregates daily count by creation date.
    CSV columns: date, pr_count (daily)
    """

    def fetch(self, owner: str, repo: str) -> pd.DataFrame:
        url = f"https://api.github.com/repos/{owner}/{repo}/pulls"
        dates: List[pd.Timestamp] = []
        for page in range(1, self.max_pages + 1):
            params = {"per_page": self.per_page, "page": page, "state": "all", "sort": "created", "direction": "asc"}
            resp = self._request(url, params=params)
            if resp.status_code != 200:
                logging.warning("PRs API non-200: %s", resp.status_code)
                break
            items = resp.json()
            if not items:
                break
            for it in items:
                created_at = it.get("created_at")
                ts = self._to_date(created_at) if created_at else None
                if ts is not None:
                    dates.append(ts)
            if len(items) < self.per_page:
                break

        if not dates:
            return pd.DataFrame(columns=["date", "pr_count"])

        df = pd.DataFrame({"date": dates, "delta": 1})
        daily = df.groupby("date", as_index=False)["delta"].sum().sort_values("date")
        daily = daily.rename(columns={"delta": "pr_count"})
        return daily

    def fetch_graphql(self, owner: str, repo: str) -> pd.DataFrame:
        gql = GitHubGraphQL()
        query = """
        query($owner: String!, $name: String!, $cursor: String) {
          repository(owner: $owner, name: $name) {
            pullRequests(first: 100, after: $cursor, orderBy: {field: CREATED_AT, direction: ASC}, states: [OPEN, MERGED, CLOSED]) {
              nodes { createdAt }
              pageInfo { endCursor hasNextPage }
            }
          }
        }
        """
        dates: List[pd.Timestamp] = []
        cursor = None
        for _ in range(200):
            data = gql.query(query, {"owner": owner, "name": repo, "cursor": cursor})
            prs = (((data or {}).get("repository") or {}).get("pullRequests") or {})
            nodes = prs.get("nodes") or []
            for n in nodes:
                ts = self._to_date(n.get("createdAt"))
                if ts is not None:
                    dates.append(ts)
            page = prs.get("pageInfo") or {}
            if not page.get("hasNextPage"):
                break
            cursor = page.get("endCursor")
        if not dates:
            return pd.DataFrame(columns=["date", "pr_count"])
        df = pd.DataFrame({"date": dates, "delta": 1})
        daily = df.groupby("date", as_index=False)["delta"].sum().sort_values("date")
        daily = daily.rename(columns={"delta": "pr_count"})
        return daily


class IssuesFetcher(BaseFetcher):
    """
    Fetches issues (state=all), excludes pull requests, and aggregates daily count by creation date.
    CSV columns: date, issues (daily)
    """

    def fetch(self, owner: str, repo: str) -> pd.DataFrame:
        url = f"https://api.github.com/repos/{owner}/{repo}/issues"
        dates: List[pd.Timestamp] = []
        # Use optimal page limit for comprehensive data
        max_pages = 20
        for page in range(1, max_pages + 1):
            params = {"per_page": self.per_page, "page": page, "state": "all", "sort": "created", "direction": "asc"}
            resp = self._request(url, params=params)
            if resp.status_code != 200:
                logging.warning("Issues API non-200: %s", resp.status_code)
                break
            items = resp.json()
            if not items:
                break
            for it in items:
                # Exclude PRs which also appear in issues API
                if it.get("pull_request") is not None:
                    continue
                created_at = it.get("created_at")
                ts = self._to_date(created_at) if created_at else None
                if ts is not None:
                    dates.append(ts)
            if len(items) < self.per_page:
                break
            # Early exit if we have enough data for a reasonable series
            if len(dates) > 1000:
                break

        if not dates:
            return pd.DataFrame(columns=["date", "issues"])

        df = pd.DataFrame({"date": dates, "delta": 1})
        daily = df.groupby("date", as_index=False)["delta"].sum().sort_values("date")
        daily = daily.rename(columns={"delta": "issues"})
        return daily

    def fetch_graphql(self, owner: str, repo: str) -> pd.DataFrame:
        gql = GitHubGraphQL()
        query = """
        query($owner: String!, $name: String!, $cursor: String) {
          repository(owner: $owner, name: $name) {
            issues(first: 100, after: $cursor, orderBy: {field: CREATED_AT, direction: ASC}, states: [OPEN, CLOSED]) {
              nodes { createdAt }
              pageInfo { endCursor hasNextPage }
            }
          }
        }
        """
        dates: List[pd.Timestamp] = []
        cursor = None
        # Use optimal page limit for comprehensive data
        max_pages = 10
        for _ in range(max_pages):
            data = gql.query(query, {"owner": owner, "name": repo, "cursor": cursor})
            sg = (((data or {}).get("repository") or {}).get("issues") or {})
            edges = sg.get("edges") or []
            for e in edges:
                ts = self._to_date(e.get("createdAt"))
                if ts is not None:
                    dates.append(ts)
            page = sg.get("pageInfo") or {}
            if not page.get("hasNextPage"):
                break
            cursor = page.get("endCursor")
            # Early exit if we have enough data
            if len(dates) > 500:
                break
        if not dates:
            return pd.DataFrame(columns=["date", "issues"])
        df = pd.DataFrame({"date": dates, "delta": 1})
        daily = df.groupby("date", as_index=False)["delta"].sum().sort_values("date")
        daily = daily.rename(columns={"delta": "issues"})
        return daily


class ContributionsFetcher(BaseFetcher):
    """
    Fetches real commit data from GitHub using multiple approaches.
    Primary: GitHub stats API for weekly commit activity
    Fallback: Direct commit API for recent commits
    CSV columns: date, commits (daily commit counts)
    """

    def fetch(self, owner: str, repo: str) -> pd.DataFrame:
        """Fetch real commit data using multiple GitHub APIs."""
        # Try the stats API first (most comprehensive)
        stats_data = self._fetch_from_stats_api(owner, repo)
        if not stats_data.empty:
            return stats_data
        
        # Fallback to direct commit API
        return self._fetch_from_commits_api(owner, repo)

    def _fetch_from_stats_api(self, owner: str, repo: str) -> pd.DataFrame:
        """Fetch commit data from GitHub stats API."""
        url = f"https://api.github.com/repos/{owner}/{repo}/stats/commit_activity"
        
        # Try multiple times as stats API can be slow
        for attempt in range(3):
            resp = self._request(url)
            
            if resp.status_code == 202:
                # Stats are still being generated, wait and retry
                if attempt < 2:
                    time.sleep(2)
                    continue
                logging.info("Commit stats still generating after retries")
                return pd.DataFrame(columns=["date", "commits"])
                
            if resp.status_code != 200:
                logging.warning(f"Commit stats API non-200: {resp.status_code}")
                return pd.DataFrame(columns=["date", "commits"])
            
            data = resp.json() or []
            if not data:
                return pd.DataFrame(columns=["date", "commits"])
            
            # Process weekly data into daily data
            rows = []
            counts = []
            
            for week in data:
                # week['week'] is a unix timestamp (start of week, Sunday)
                base = pd.to_datetime(week.get("week", 0), unit="s")
                daily_counts = week.get("days", []) or []
                
                for i, count in enumerate(daily_counts):
                    date = (base + pd.Timedelta(days=i)).normalize()
                    count_value = int(count or 0)
                    
                    if count_value > 0:  # Only include days with commits
                        rows.append(date)
                        counts.append(count_value)
            
            if not rows:
                return pd.DataFrame(columns=["date", "commits"])
            
            # Create DataFrame and sort by date
            df = pd.DataFrame({"date": rows, "commits": counts})
            df = df.dropna(subset=["date"]).sort_values("date")
            
            return df
        
        return pd.DataFrame(columns=["date", "commits"])

    def _fetch_from_commits_api(self, owner: str, repo: str) -> pd.DataFrame:
        """Fallback: Fetch recent commits directly from commits API."""
        url = f"https://api.github.com/repos/{owner}/{repo}/commits"
        
        all_commits = []
        page = 1
        
        # Fetch recent commits (last 1000 commits)
        while page <= min(10, self.max_pages):  # Limit to 10 pages for recent commits
            params = {"per_page": self.per_page, "page": page}
            resp = self._request(url, params=params)
            
            if resp.status_code != 200:
                logging.error(f"Failed to fetch commits: {resp.status_code}")
                break
                
            commits = resp.json()
            if not commits:
                break
                
            for commit in commits:
                commit_date = commit.get("commit", {}).get("author", {}).get("date")
                if commit_date:
                    date_ts = self._to_date(commit_date)
                    if date_ts:
                        all_commits.append({"date": date_ts, "commits": 1})
            
            page += 1
            
            # Stop if we got fewer items than requested
            if len(commits) < self.per_page:
                break
        
        if not all_commits:
            return pd.DataFrame(columns=["date", "commits"])
        
        # Convert to DataFrame and group by date
        df = pd.DataFrame(all_commits)
        daily_commits = df.groupby("date")["commits"].sum().reset_index()
        daily_commits = daily_commits.sort_values("date")
        
        return daily_commits

class DownloadsFetcher(BaseFetcher):
    """
    Fetches real download data from GitHub releases and assets.
    Gets comprehensive download counts from all release assets.
    CSV columns: date, downloads (daily download counts)
    """

    def fetch(self, owner: str, repo: str) -> pd.DataFrame:
        """Fetch real download data from GitHub releases and assets."""
        url = f"https://api.github.com/repos/{owner}/{repo}/releases"
        
        all_downloads = []
        page = 1
        
        # Fetch all releases with assets
        while page <= self.max_pages:
            params = {"per_page": self.per_page, "page": page}
            resp = self._request(url, params=params)
            
            if resp.status_code != 200:
                logging.error(f"Failed to fetch releases: {resp.status_code} - {resp.text}")
                break
                
            releases = resp.json()
            if not releases:
                break
                
            for release in releases:
                # Get release date
                release_date = release.get("published_at") or release.get("created_at")
                if not release_date:
                    continue
                    
                release_ts = self._to_date(release_date)
                if not release_ts:
                    continue
                
                # Get assets from this release
                assets = release.get("assets", [])
                for asset in assets:
                    # Get asset creation date
                    asset_date = asset.get("created_at") or asset.get("updated_at") or release_date
                    asset_ts = self._to_date(asset_date)
                    if not asset_ts:
                        continue
                    
                    # Get download count
                    download_count = int(asset.get("download_count", 0) or 0)
                    
                    if download_count > 0:
                        all_downloads.append({
                            "date": asset_ts,
                            "downloads": download_count,
                            "asset_name": asset.get("name", ""),
                            "release_tag": release.get("tag_name", "")
                        })
            
            page += 1
            
            # Stop if we got fewer items than requested
            if len(releases) < self.per_page:
                break
        
        if not all_downloads:
            return pd.DataFrame(columns=["date", "downloads"])
        
        # Convert to DataFrame and process
        df = pd.DataFrame(all_downloads)
        
        # Group by date and sum downloads for that date
        daily_downloads = df.groupby("date")["downloads"].sum().reset_index()
        daily_downloads = daily_downloads.sort_values("date")
        
        # For daily activity, we want the sum of downloads on each date
        # This represents the total download activity on each specific date
        return daily_downloads[["date", "downloads"]]


class GitHubFetcher:
    """Aggregates all metric fetchers and exposes a single interface."""

    def __init__(self):
        self.stars_fetcher = StarsFetcher()
        self.forks_fetcher = ForksFetcher()
        self.prs_fetcher = PRsFetcher()
        self.downloads_fetcher = DownloadsFetcher()
        self.issues_fetcher = IssuesFetcher()
        self.contributions_fetcher = ContributionsFetcher()
        self.use_graphql = os.getenv("P16_USE_GRAPHQL") == "1"

    def fetch_all(self, owner: str, repo: str) -> Dict[str, pd.DataFrame]:
        if self.use_graphql:
            stars = self.stars_fetcher.fetch_graphql(owner, repo)
            forks = self.forks_fetcher.fetch_graphql(owner, repo)
            prs = self.prs_fetcher.fetch_graphql(owner, repo)
            issues = self.issues_fetcher.fetch_graphql(owner, repo)
        else:
            stars = self.stars_fetcher.fetch(owner, repo)
            forks = self.forks_fetcher.fetch(owner, repo)
            prs = self.prs_fetcher.fetch(owner, repo)
            issues = self.issues_fetcher.fetch(owner, repo)
        downloads = self.downloads_fetcher.fetch(owner, repo)
        contribs = self.contributions_fetcher.fetch(owner, repo)
        return {
            "stars": stars,
            "forks": forks,
            "prs": prs,
            "downloads": downloads,
            "issues": issues,
            "contributions": contribs,
        }


