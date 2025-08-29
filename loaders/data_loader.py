import os
from typing import Optional

import pandas as pd


class DataLoader:
    """Loads metric CSVs produced by fetchers into pandas DataFrames."""

    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir

    def _base_name(self, data_type: str) -> str:
        mapping = {
            "stars": "github_stars.csv",
            "forks": "github_forks.csv",
            "prs": "github_pull_requests.csv",
            "downloads": "github_downloads.csv",
            "issues": "github_issues.csv",
            "contributions": "github_contributions.csv",
        }
        fname = mapping.get(data_type)
        if not fname:
            raise ValueError(f"Unknown data_type: {data_type}")
        return fname

    def path_for(self, data_type: str, owner: str = "", repo: str = "") -> str:
        base = self._base_name(data_type)
        if owner and repo:
            prefixed = f"{owner}_{repo}_{base}"
            return os.path.join(self.data_dir, prefixed)
        return os.path.join(self.data_dir, base)

    def get(self, data_type: str) -> pd.DataFrame:
        path = self.path_for(data_type)
        if not os.path.exists(path):
            # Return empty DF with correct schema
            columns = {
                "stars": ["date", "stars"],
                "forks": ["date", "forks"],
                "prs": ["date", "pr_count"],
                "downloads": ["date", "downloads"],
                "issues": ["date", "issues"],
                "contributions": ["date", "commits"],
            }[data_type]
            return pd.DataFrame(columns=columns)

        df = pd.read_csv(path)
        # Ensure expected columns exist
        expected = {
            "stars": {"date", "stars"},
            "forks": {"date", "forks"},
            "prs": {"date", "pr_count"},
            "downloads": {"date", "downloads"},
            "issues": {"date", "issues"},
            "contributions": {"date", "commits"},
        }[data_type]
        if not expected.issubset(set(df.columns)):
            return pd.DataFrame(columns=list(expected))

        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"]).sort_values("date")
        return df

    def get_for(self, data_type: str, owner: str, repo: str) -> pd.DataFrame:
        # Try repository-specific file first
        path = self.path_for(data_type, owner, repo)
        if os.path.exists(path):
            df = pd.read_csv(path)
            if not df.empty and len(df) > 1:  # More than just header
                expected = {
                    "stars": {"date", "stars"},
                    "forks": {"date", "forks"},
                    "prs": {"date", "pr_count"},
                    "downloads": {"date", "downloads"},
                    "issues": {"date", "issues"},
                    "contributions": {"date", "commits"},
                }[data_type]
                if expected.issubset(set(df.columns)):
                    df["date"] = pd.to_datetime(df["date"], errors="coerce")
                    df = df.dropna(subset=["date"]).sort_values("date")
                    return df
        
        # Fall back to generic file
        generic_path = self.path_for(data_type)
        if os.path.exists(generic_path):
            df = pd.read_csv(generic_path)
            expected = {
                "stars": {"date", "stars"},
                "forks": {"date", "forks"},
                "prs": {"date", "pr_count"},
                "downloads": {"date", "downloads"},
                "issues": {"date", "issues"},
                "contributions": {"date", "commits"},
            }[data_type]
            if expected.issubset(set(df.columns)):
                df["date"] = pd.to_datetime(df["date"], errors="coerce")
                df = df.dropna(subset=["date"]).sort_values("date")
                return df
        
        # Return empty DataFrame with correct schema if nothing works
        columns = {
            "stars": ["date", "stars"],
            "forks": ["date", "forks"],
            "prs": ["date", "pr_count"],
            "downloads": ["date", "downloads"],
            "issues": ["date", "issues"],
            "contributions": ["date", "commits"],
        }[data_type]
        return pd.DataFrame(columns=columns)


