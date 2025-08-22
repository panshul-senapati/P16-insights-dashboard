import os
import logging
from datetime import datetime
from typing import Dict, Optional
import pandas as pd
from fetchers.github_fetcher import GitHubFetcher

class DataManager:
    """
    Manages GitHub repo metrics.
    Fetches data from GitHub if cached CSVs are missing or stale.
    """

    def __init__(self, data_dir: str = "data", refresh_threshold_hours: int = 24):
        self.data_dir = data_dir
        self.refresh_threshold_hours = refresh_threshold_hours
        self.github_fetcher = GitHubFetcher(data_dir=self.data_dir)

        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

        # Cached CSV file paths
        self.data_files = {
            "stars": os.path.join(self.data_dir, "stars.csv"),
            "forks": os.path.join(self.data_dir, "forks.csv"),
            "prs": os.path.join(self.data_dir, "prs.csv"),
            "downloads": os.path.join(self.data_dir, "downloads.csv"),
        }

    def _is_data_stale(self, filepath: str) -> bool:
        """Check if file is missing or stale."""
        if not os.path.exists(filepath):
            return True
        mod_time = datetime.fromtimestamp(os.path.getmtime(filepath))
        age_hours = (datetime.now() - mod_time).total_seconds() / 3600
        return age_hours > self.refresh_threshold_hours

    def _fetch_and_cache_all(self, owner: str, repo: str) -> None:
        """Fetch all metrics from GitHub and save as CSVs."""
        self.logger.info(f"Fetching fresh data for {owner}/{repo} from GitHub...")
        data_dict = self.github_fetcher.fetch_all(owner, repo)

        for key, df in data_dict.items():
            if df is not None and not df.empty:
                filepath = self.data_files.get(key)
                if filepath:
                    df.to_csv(filepath, index=False)
                    self.logger.info(f"Saved {key} data → {filepath}")
            else:
                self.logger.warning(f"No data returned for {key}")

    def get_data(self, data_type: str, owner: str, repo: str, force_refresh: bool = False) -> pd.DataFrame:
        """Return a single type of data, fetching if missing or stale."""
        filepath = self.data_files.get(data_type)
        if not filepath:
            self.logger.error(f"Unknown data type: {data_type}")
            return pd.DataFrame()

        if force_refresh or self._is_data_stale(filepath):
            self._fetch_and_cache_all(owner, repo)

        if os.path.exists(filepath):
            try:
                df = pd.read_csv(filepath)
                return df
            except Exception as e:
                self.logger.error(f"Failed to load {data_type} CSV: {e}")
                return pd.DataFrame()
        else:
            self.logger.warning(f"CSV for {data_type} not found after fetch attempt")
            return pd.DataFrame()

    def get_all_cached_data(self, owner: str, repo: str, force_refresh: bool = False) -> Dict[str, pd.DataFrame]:
        """Return all cached GitHub metrics."""
        if force_refresh or any(self._is_data_stale(fp) for fp in self.data_files.values()):
            self._fetch_and_cache_all(owner, repo)

        data = {}
        for key in self.data_files.keys():
            data[key] = self.get_data(key, owner, repo, force_refresh=False)
        return data

    def get_real_time_data(self, owner: str, repo: str) -> Dict[str, pd.DataFrame]:
        """
        Fetch real-time metrics that are not cached.
        Returns empty DataFrames if no data is available.
        """
        data = {}
        try:
            data['contributions'] = self.github_fetcher.stars_fetcher.fetch(owner, repo)  # placeholder
            data['issues'] = self.github_fetcher.prs_fetcher.fetch(owner, repo)          # placeholder
            data['dependents'] = pd.DataFrame(columns=["name", "stars"])                  # placeholder
        except Exception as e:
            self.logger.error(f"Error fetching real-time data: {e}")
            data['contributions'] = pd.DataFrame()
            data['issues'] = pd.DataFrame()
            data['dependents'] = pd.DataFrame()
        return data

    def get_data_status(self) -> Dict[str, dict]:
        """Return the status of cached CSV files."""
        status = {}
        for key, filepath in self.data_files.items():
            if os.path.exists(filepath):
                mod_time = datetime.fromtimestamp(os.path.getmtime(filepath))
                age_hours = (datetime.now() - mod_time).total_seconds() / 3600
                status[key] = {
                    "exists": True,
                    "last_updated": mod_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "age_hours": round(age_hours, 1),
                    "is_stale": age_hours > self.refresh_threshold_hours
                }
            else:
                status[key] = {
                    "exists": False,
                    "last_updated": "Never",
                    "age_hours": float("inf"),
                    "is_stale": True
                }
        return status

    def clear_cache(self, data_type: Optional[str] = None):
        """Delete cached CSV files."""
        if data_type:
            filepath = self.data_files.get(data_type)
            if filepath and os.path.exists(filepath):
                os.remove(filepath)
                self.logger.info(f"Cleared cache for {data_type}")
        else:
            for key, filepath in self.data_files.items():
                if os.path.exists(filepath):
                    os.remove(filepath)
                    self.logger.info(f"Cleared cache for {key}")
