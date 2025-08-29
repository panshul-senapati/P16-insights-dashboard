import os
import time
import logging
from dataclasses import dataclass
from typing import Dict, Optional, List

import pandas as pd

from fetchers.github_fetcher import GitHubFetcher
from loaders.data_loader import DataLoader


@dataclass
class DataFileInfo:
    path: str
    exists: bool
    age_hours: Optional[float]
    stale: bool


class DataManager:
    """
    Orchestrates fetching vs. loading, manages cache freshness, and exposes
    simple APIs for retrieving metric dataframes.
    """

    def __init__(self, data_dir: str = "data", refresh_threshold_hours: int = 24):
        self.data_dir = data_dir
        self.refresh_threshold_hours = refresh_threshold_hours
        os.makedirs(self.data_dir, exist_ok=True)

        self.fetcher = GitHubFetcher()
        self.loader = DataLoader(data_dir=self.data_dir)

        self.types = ["stars", "forks", "prs", "downloads", "issues", "contributions"]

    def _is_data_stale(self, path: str) -> bool:
        if not os.path.exists(path):
            return True
        mtime = os.path.getmtime(path)
        age_hours = (time.time() - mtime) / 3600.0
        return age_hours > self.refresh_threshold_hours

    def _fetch_and_save_data(self, data_type: str, owner: str, repo: str) -> pd.DataFrame:
        """Fetch data and save to CSV with proper error handling."""
        try:
            if data_type == "stars":
                df = self.fetcher.stars_fetcher.fetch(owner, repo)
            elif data_type == "forks":
                df = self.fetcher.forks_fetcher.fetch(owner, repo)
            elif data_type == "prs":
                df = self.fetcher.prs_fetcher.fetch(owner, repo)
            elif data_type == "downloads":
                df = self.fetcher.downloads_fetcher.fetch(owner, repo)
            elif data_type == "issues":
                df = self.fetcher.issues_fetcher.fetch(owner, repo)
            elif data_type == "contributions":
                df = self.fetcher.contributions_fetcher.fetch(owner, repo)
            else:
                raise ValueError(f"Unknown data_type: {data_type}")

            # Ensure columns are as expected and save
            expected_cols = {
                "stars": ["date", "stars"],
                "forks": ["date", "forks"],
                "prs": ["date", "pr_count"],
                "downloads": ["date", "downloads"],
                "issues": ["date", "issues"],
                "contributions": ["date", "commits"],
            }[data_type]
            
            # Filter to only expected columns if they exist
            available_cols = [c for c in expected_cols if c in df.columns]
            if len(available_cols) != len(expected_cols):
                logging.warning(f"Missing columns for {data_type}. Expected: {expected_cols}, Got: {list(df.columns)}")
                # Return empty DataFrame with correct schema if columns are missing
                return pd.DataFrame(columns=expected_cols)
            
            df = df[available_cols]
            
            # Save to CSV
            try:
                out_path = self.loader.path_for(data_type, owner, repo)
                df.to_csv(out_path, index=False)
                logging.info(f"Successfully saved {data_type} data to {out_path}")
            except Exception as e:
                logging.error(f"Failed to write CSV for {data_type}: {e}")
                
            return df
            
        except Exception as e:
            logging.error(f"Error fetching {data_type} data: {e}")
            # Return empty DataFrame with correct schema on error
            expected_cols = {
                "stars": ["date", "stars"],
                "forks": ["date", "forks"],
                "prs": ["date", "pr_count"],
                "downloads": ["date", "downloads"],
                "issues": ["date", "issues"],
                "contributions": ["date", "commits"],
            }[data_type]
            return pd.DataFrame(columns=expected_cols)

    def _load_data(self, data_type: str, owner: str, repo: str) -> pd.DataFrame:
        return self.loader.get_for(data_type, owner, repo)

    def get_data(self, data_type: str, owner: str, repo: str, force_refresh: bool = False) -> pd.DataFrame:
        path = self.loader.path_for(data_type, owner, repo)
        if force_refresh or self._is_data_stale(path):
            logging.info("Fetching fresh data for %s/%s %s (force_refresh=%s)", owner, repo, data_type, force_refresh)
            return self._fetch_and_save_data(data_type, owner, repo)
        logging.info("Loading cached data for %s/%s %s", owner, repo, data_type)
        return self._load_data(data_type, owner, repo)

    def get_all_cached_data(self, owner: str, repo: str, force_refresh: bool = False) -> Dict[str, pd.DataFrame]:
        types_to_process = self.types
        return {t: self.get_data(t, owner, repo, force_refresh=force_refresh) for t in types_to_process}

    def get_all_cached_data_for_range(self, owner: str, repo: str, start_date, end_date, force_refresh: bool = False) -> Dict[str, pd.DataFrame]:
        """
        Smart range-aware retrieval that only fetches missing data.
        - If no cache: fetch full series and save.
        - If force_refresh: fetch fresh data for all types.
        - Else check if cached data covers the requested range and only fetch what's missing.
        """
        result: Dict[str, pd.DataFrame] = {}
        types_to_process = self.types
        
        for t in types_to_process:
            path = self.loader.path_for(t, owner, repo)
            cached = self._load_data(t, owner, repo) if os.path.exists(path) else pd.DataFrame(columns={
                "stars": ["date", "stars"],
                "forks": ["date", "forks"],
                "prs": ["date", "pr_count"],
                "downloads": ["date", "downloads"],
                "issues": ["date", "issues"],
                "contributions": ["date", "commits"],
            }[t])

            need_fetch = force_refresh or cached.empty

            if not need_fetch and not cached.empty:
                # Check if cached data covers the requested range
                try:
                    cached_dates = pd.to_datetime(cached["date"])
                    req_start = pd.to_datetime(start_date)
                    req_end = pd.to_datetime(end_date)
                    
                    # Check if we have data covering the entire requested range
                    range_covered = (
                        cached_dates.min() <= req_start and 
                        cached_dates.max() >= req_end and 
                        not self._is_data_stale(path)
                    )
                    
                    if not range_covered:
                        logging.info(f"Data for {t} doesn't cover requested range {start_date} to {end_date}, fetching fresh data")
                        need_fetch = True
                        
                        # Check if we need to fetch only missing data or full refresh
                        if not cached.empty:
                            # Try to fetch only missing data
                            try:
                                missing_data = self._fetch_missing_data(t, owner, repo, cached, req_start, req_end)
                                if not missing_data.empty:
                                    # Merge missing data with cached data
                                    merged = pd.concat([cached, missing_data], ignore_index=True)
                                    merged = merged.drop_duplicates(subset=["date"]).sort_values("date")
                                    
                                    # Save merged data
                                    try:
                                        merged.to_csv(path, index=False)
                                        logging.info(f"Successfully merged and saved {t} data")
                                        result[t] = merged
                                        continue  # Skip full fetch
                                    except Exception as e:
                                        logging.error(f"Failed to save merged data for {t}: {e}")
                                        need_fetch = True  # Fall back to full fetch
                                else:
                                    need_fetch = True  # No missing data found, do full fetch
                            except Exception as e:
                                logging.warning(f"Failed to fetch missing data for {t}: {e}, will do full fetch")
                                need_fetch = True
                                
                except Exception as e:
                    logging.warning(f"Error checking date coverage for {t}: {e}, will fetch fresh data")
                    need_fetch = True

            if need_fetch:
                logging.info(f"Fetching fresh data for {t}")
                try:
                    fresh = self._fetch_and_save_data(t, owner, repo)
                    if cached.empty:
                        merged = fresh
                    else:
                        # Merge cached and fresh data, removing duplicates
                        merged = pd.concat([cached, fresh], ignore_index=True)
                        merged = merged.drop_duplicates(subset=["date"]).sort_values("date")
                    
                    # Persist merged data
                    try:
                        merged.to_csv(path, index=False)
                        logging.info(f"Successfully saved merged data for {t}")
                    except Exception as e:
                        logging.error(f"Failed to persist merged CSV for %s: %s", t, e)
                    
                    result[t] = merged
                except Exception as e:
                    logging.error(f"Failed to fetch data for {t}: {e}")
                    # Return cached data if available, otherwise empty DataFrame
                    result[t] = cached if not cached.empty else pd.DataFrame(columns={
                        "stars": ["date", "stars"],
                        "forks": ["date", "forks"],
                        "prs": ["date", "pr_count"],
                        "downloads": ["date", "downloads"],
                        "issues": ["date", "issues"],
                        "contributions": ["date", "commits"],
                    }[t])
            else:
                logging.info(f"Using cached data for {t}")
                result[t] = cached

        return result

    def _fetch_missing_data(self, data_type: str, owner: str, repo: str, cached_df: pd.DataFrame, start_date, end_date) -> pd.DataFrame:
        """Try to fetch only missing data for a specific date range."""
        try:
            # This is a simplified approach - in practice, you might need to implement
            # more sophisticated logic based on the specific API endpoints
            # For now, we'll return empty DataFrame to trigger full fetch
            return pd.DataFrame()
        except Exception as e:
            logging.warning(f"Error in _fetch_missing_data for {data_type}: {e}")
            return pd.DataFrame()

    def get_data_status(self) -> Dict[str, DataFileInfo]:
        status: Dict[str, DataFileInfo] = {}
        now = time.time()
        for t, path in self.type_to_file.items():
            exists = os.path.exists(path)
            if exists:
                age_hours = (now - os.path.getmtime(path)) / 3600.0
                stale = age_hours > self.refresh_threshold_hours
            else:
                age_hours = None
                stale = True
            status[t] = DataFileInfo(path=path, exists=exists, age_hours=age_hours, stale=stale)
        return status

    def clear_cache(self, data_type: Optional[str] = None) -> None:
        if data_type is None:
            for path in self.type_to_file.values():
                if os.path.exists(path):
                    os.remove(path)
        else:
            path = self.type_to_file.get(data_type)
            if path and os.path.exists(path):
                os.remove(path)

    def force_fetch_real_data(self, owner: str, repo: str, data_types: List[str] = None) -> Dict[str, pd.DataFrame]:
        """
        Force fetch real data from GitHub APIs for specified data types.
        This ensures fresh, authentic data is fetched and saved to CSV files.
        """
        if data_types is None:
            data_types = ["downloads", "contributions"]  # Default to downloads and commits
        
        result = {}
        
        for data_type in data_types:
            if data_type not in self.types:
                logging.warning(f"Unknown data type: {data_type}")
                continue
                
            try:
                logging.info(f"Force fetching real {data_type} data for {owner}/{repo}")
                
                # Fetch fresh data
                df = self._fetch_and_save_data(data_type, owner, repo)
                
                if not df.empty:
                    logging.info(f"Successfully fetched {len(df)} rows of {data_type} data")
                    result[data_type] = df
                else:
                    logging.warning(f"No {data_type} data returned from API")
                    result[data_type] = df
                    
            except Exception as e:
                logging.error(f"Error force fetching {data_type} data: {e}")
                # Return empty DataFrame with correct schema on error
                expected_cols = {
                    "stars": ["date", "stars"],
                    "forks": ["date", "forks"],
                    "prs": ["date", "pr_count"],
                    "downloads": ["date", "downloads"],
                    "issues": ["date", "issues"],
                    "contributions": ["date", "commits"],
                }[data_type]
                result[data_type] = pd.DataFrame(columns=expected_cols)
        
        return result


