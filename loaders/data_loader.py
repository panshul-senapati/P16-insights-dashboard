"""
Loaders - Responsible for loading data from the data folder that was stored by fetchers.
Provides a clean interface for accessing persisted data.
"""

import os
import json
import logging
import pandas as pd
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BaseLoader(ABC):
    """Base class for all loaders."""

    def __init__(self, data_dir: str):
        self.data_dir = data_dir

    @abstractmethod
    def load(self, owner: str, repo: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_last_modified(self, owner: str, repo: str) -> Optional[datetime]:
        pass

    def _get_filepath(self, owner: str, repo: str, extension: str = "csv") -> str:
        filename = f"{owner}_{repo}_{self.data_type}.{extension}"
        return os.path.join(self.data_dir, filename)

    def _file_exists(self, filepath: str) -> bool:
        return os.path.exists(filepath) and os.path.isfile(filepath)

    def _get_file_modified_time(self, filepath: str) -> Optional[datetime]:
        try:
            if self._file_exists(filepath):
                timestamp = os.path.getmtime(filepath)
                return datetime.fromtimestamp(timestamp)
        except OSError as e:
            logger.error(f"Error getting modification time for {filepath}: {e}")
        return None


# -------------------------
# Concrete Loaders
# -------------------------
class StarsLoader(BaseLoader):
    data_type = "stars"

    def load(self, owner: str, repo: str) -> pd.DataFrame:
        filepath = self._get_filepath(owner, repo)
        try:
            if self._file_exists(filepath):
                return pd.read_csv(filepath)
            return pd.DataFrame(columns=["date", "stars"])
        except Exception as e:
            logger.error(f"Error loading stars data: {e}")
            return pd.DataFrame(columns=["date", "stars"])

    def get_last_modified(self, owner: str, repo: str):
        return self._get_file_modified_time(self._get_filepath(owner, repo))


class ForksLoader(BaseLoader):
    data_type = "forks"

    def load(self, owner: str, repo: str) -> pd.DataFrame:
        filepath = self._get_filepath(owner, repo)
        try:
            if self._file_exists(filepath):
                return pd.read_csv(filepath)
            return pd.DataFrame(columns=["date", "forks"])
        except Exception as e:
            logger.error(f"Error loading forks data: {e}")
            return pd.DataFrame(columns=["date", "forks"])

    def get_last_modified(self, owner: str, repo: str):
        return self._get_file_modified_time(self._get_filepath(owner, repo))


class PRsLoader(BaseLoader):
    data_type = "prs"

    def load(self, owner: str, repo: str) -> pd.DataFrame:
        filepath = self._get_filepath(owner, repo)
        try:
            if self._file_exists(filepath):
                return pd.read_csv(filepath)
            return pd.DataFrame(columns=["date", "pr_count"])
        except Exception as e:
            logger.error(f"Error loading PRs data: {e}")
            return pd.DataFrame(columns=["date", "pr_count"])

    def get_last_modified(self, owner: str, repo: str):
        return self._get_file_modified_time(self._get_filepath(owner, repo))


class DownloadsLoader(BaseLoader):
    data_type = "downloads"

    def load(self, owner: str, repo: str) -> pd.DataFrame:
        filepath = self._get_filepath(owner, repo)
        try:
            if self._file_exists(filepath):
                return pd.read_csv(filepath)
            return pd.DataFrame(columns=["date", "downloads"])
        except Exception as e:
            logger.error(f"Error loading downloads data: {e}")
            return pd.DataFrame(columns=["date", "downloads"])

    def get_last_modified(self, owner: str, repo: str):
        return self._get_file_modified_time(self._get_filepath(owner, repo))


class ContributionsLoader(BaseLoader):
    data_type = "contributions"

    def load(self, owner: str, repo: str) -> pd.DataFrame:
        filepath = self._get_filepath(owner, repo)
        try:
            if self._file_exists(filepath):
                return pd.read_csv(filepath)
            return pd.DataFrame(columns=["week", "commits"])
        except Exception as e:
            logger.error(f"Error loading contributions data: {e}")
            return pd.DataFrame(columns=["week", "commits"])

    def get_last_modified(self, owner: str, repo: str):
        return self._get_file_modified_time(self._get_filepath(owner, repo))


class IssuesLoader(BaseLoader):
    data_type = "issues"

    def load(self, owner: str, repo: str) -> pd.DataFrame:
        filepath = self._get_filepath(owner, repo)
        try:
            if self._file_exists(filepath):
                return pd.read_csv(filepath)
            return pd.DataFrame(columns=["date", "issue_count"])
        except Exception as e:
            logger.error(f"Error loading issues data: {e}")
            return pd.DataFrame(columns=["date", "issue_count"])

    def get_last_modified(self, owner: str, repo: str):
        return self._get_file_modified_time(self._get_filepath(owner, repo))


class DependentsLoader(BaseLoader):
    data_type = "dependents"

    def load(self, owner: str, repo: str) -> pd.DataFrame:
        filepath = self._get_filepath(owner, repo, "json")
        try:
            if self._file_exists(filepath):
                with open(filepath, "r") as f:
                    data = json.load(f)
                if isinstance(data, list):
                    return pd.DataFrame(data)
            return pd.DataFrame(columns=["name", "owner", "stars", "forks", "description", "language", "url"])
        except Exception as e:
            logger.error(f"Error loading dependents data: {e}")
            return pd.DataFrame(columns=["name", "owner", "stars", "forks", "description", "language", "url"])

    def get_last_modified(self, owner: str, repo: str):
        return self._get_file_modified_time(self._get_filepath(owner, repo, "json"))


# -------------------------
# Wrapper DataLoader
# -------------------------
class DataLoader:
    """
    Unified loader interface for all repository data types.
    """
    def __init__(self, data_dir: str):
        self.loaders = {
            "stars": StarsLoader(data_dir),
            "forks": ForksLoader(data_dir),
            "prs": PRsLoader(data_dir),
            "downloads": DownloadsLoader(data_dir),
            "contributions": ContributionsLoader(data_dir),
            "issues": IssuesLoader(data_dir),
            "dependents": DependentsLoader(data_dir),
        }

    def get(self, data_type: str, owner: str, repo: str) -> pd.DataFrame:
        loader = self.loaders.get(data_type)
        if loader:
            return loader.load(owner, repo)
        return pd.DataFrame()

    def get_last_modified(self, data_type: str, owner: str, repo: str):
        loader = self.loaders.get(data_type)
        if loader:
            return loader.get_last_modified(owner, repo)
        return None