"""
Data Manager: Orchestrates data fetching and loading operations
Handles the decision logic for when to fetch new data vs. load existing data
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple
import pandas as pd

from fetchers.github_fetcher import GitHubFetcher
from loaders.data_loader import DataLoader


class DataManager:
    """
    Manages data flow between fetchers and loaders.
    
    Flow:
    1. Check if data exists and is fresh
    2. If stale or missing, use fetchers to get new data
    3. Save fetched data to data folder
    4. Use loaders to provide data to app
    """
    
    def __init__(self, data_dir: str = "data", refresh_threshold_hours: int = 24):
        self.data_dir = data_dir
        self.refresh_threshold_hours = refresh_threshold_hours
        self.github_fetcher = GitHubFetcher()
        self.data_loader = DataLoader()
        
        # Ensure data directory exists
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
            
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Define data file mappings
        self.data_files = {
            "stars": os.path.join(self.data_dir, "stars.csv"),
            "forks": os.path.join(self.data_dir, "forks.csv"),
            "prs": os.path.join(self.data_dir, "prs.csv"),
            "downloads": os.path.join(self.data_dir, "github_downloads.csv"),
        }
        
        # Define fetcher method mappings
        self.fetch_methods = {
            "stars": self.github_fetcher.get_stars_over_time,
            "forks": self.github_fetcher.get_forks_over_time,
            "prs": self.github_fetcher.get_pull_requests_over_time,
            "downloads": self.github_fetcher.get_downloads_over_time,
        }
    
    def _is_data_stale(self, filepath: str) -> bool:
        """Check if data file is stale or missing."""
        if not os.path.exists(filepath):
            self.logger.info(f"File {filepath} does not exist")
            return True
            
        mod_time = datetime.fromtimestamp(os.path.getmtime(filepath))
        time_diff = (datetime.now() - mod_time).total_seconds()
        is_stale = time_diff > (self.refresh_threshold_hours * 3600)
        
        if is_stale:
            self.logger.info(f"File {filepath} is stale (age: {time_diff/3600:.1f} hours)")
        
        return is_stale
    
    def _fetch_and_save_data(self, data_type: str, owner: str, repo: str) -> pd.DataFrame:
        """Fetch data using appropriate fetcher and save to file."""
        self.logger.info(f"Fetching {data_type} data for {owner}/{repo}")
        
        fetch_method = self.fetch_methods.get(data_type)
        if not fetch_method:
            self.logger.error(f"No fetch method found for {data_type}")
            return pd.DataFrame()
        
        try:
            df = fetch_method(owner, repo)
            if not df.empty:
                filepath = self.data_files[data_type]
                df.to_csv(filepath, index=False)
                self.logger.info(f"Saved {len(df)} rows to {filepath}")
            else:
                self.logger.warning(f"No data fetched for {data_type}")
            return df
        except Exception as e:
            self.logger.error(f"Error fetching {data_type}: {str(e)}")
            return pd.DataFrame()
    
    def _load_data(self, data_type: str) -> pd.DataFrame:
        """Load data using data loader."""
        filepath = self.data_files[data_type]
        if os.path.exists(filepath):
            try:
                return self.data_loader.load_csv(filepath)
            except Exception as e:
                self.logger.error(f"Error loading {data_type}: {str(e)}")
        return pd.DataFrame()
    
    def get_data(self, data_type: str, owner: str, repo: str, force_refresh: bool = False) -> pd.DataFrame:
        """
        Get data for specified type. Decides whether to fetch or load based on freshness.
        
        Args:
            data_type: Type of data (stars, forks, prs, downloads)
            owner: GitHub repository owner
            repo: GitHub repository name
            force_refresh: Force fetching new data regardless of staleness
            
        Returns:
            DataFrame with requested data
        """
        filepath = self.data_files.get(data_type)
        if not filepath:
            self.logger.error(f"Unknown data type: {data_type}")
            return pd.DataFrame()
        
        # Check if we need to fetch new data
        needs_fetch = force_refresh or self._is_data_stale(filepath)
        
        if needs_fetch:
            # Fetch new data
            df = self._fetch_and_save_data(data_type, owner, repo)
            
            # If fetch failed but old data exists, fall back to old data
            if df.empty and os.path.exists(filepath):
                self.logger.warning(f"Fetch failed for {data_type}, falling back to cached data")
                df = self._load_data(data_type)
        else:
            # Load existing data
            df = self._load_data(data_type)
        
        return df
    
    def get_all_cached_data(self, owner: str, repo: str, force_refresh: bool = False) -> Dict[str, pd.DataFrame]:
        """
        Get all cached data types for a repository.
        
        Returns:
            Dictionary with data type as key and DataFrame as value
        """
        results = {}
        
        for data_type in self.data_files.keys():
            results[data_type] = self.get_data(data_type, owner, repo, force_refresh)
            
        return results
    
    def get_real_time_data(self, owner: str, repo: str) -> Dict[str, pd.DataFrame]:
        """
        Get data that should always be fetched fresh (not cached).
        
        Returns:
            Dictionary with real-time data
        """
        real_time_data = {}
        
        try:
            # These are always fetched fresh as they change frequently
            real_time_data['contributions'] = self.github_fetcher.get_weekly_contributions(owner, repo)
            real_time_data['issues'] = self.github_fetcher.get_issues_over_time(owner, repo)
            real_time_data['dependents'] = self.github_fetcher.get_dependents(owner, repo)
        except Exception as e:
            self.logger.error(f"Error fetching real-time data: {str(e)}")
            # Return empty DataFrames for failed fetches
            for key in ['contributions', 'issues', 'dependents']:
                if key not in real_time_data:
                    real_time_data[key] = pd.DataFrame()
        
        return real_time_data
    
    def get_data_status(self) -> Dict[str, dict]:
        """
        Get status information for all data files.
        
        Returns:
            Dictionary with file status information
        """
        status = {}
        
        for data_type, filepath in self.data_files.items():
            if os.path.exists(filepath):
                mod_time = datetime.fromtimestamp(os.path.getmtime(filepath))
                age_hours = (datetime.now() - mod_time).total_seconds() / 3600
                status[data_type] = {
                    'exists': True,
                    'last_updated': mod_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'age_hours': round(age_hours, 1),
                    'is_stale': age_hours > self.refresh_threshold_hours
                }
            else:
                status[data_type] = {
                    'exists': False,
                    'last_updated': 'Never',
                    'age_hours': float('inf'),
                    'is_stale': True
                }
        
        return status
    
    def clear_cache(self, data_type: Optional[str] = None):
        """
        Clear cached data files.
        
        Args:
            data_type: Specific data type to clear, or None to clear all
        """
        if data_type:
            filepath = self.data_files.get(data_type)
            if filepath and os.path.exists(filepath):
                os.remove(filepath)
                self.logger.info(f"Cleared cache for {data_type}")
        else:
            # Clear all cache files
            for data_type, filepath in self.data_files.items():
                if os.path.exists(filepath):
                    os.remove(filepath)
                    self.logger.info(f"Cleared cache for {data_type}")