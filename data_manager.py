import os
import logging
from datetime import datetime, date
from typing import Dict, Optional
import polars as pl
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

        # Supported metrics
        self.metrics = ["stars", "forks", "prs", "downloads"]

    def _parse_date_col(self, df: pl.DataFrame) -> pl.DataFrame:
        """Robustly parse a 'date' column into pl.Date from various string/datetime formats."""
        if df is None or len(df) == 0 or "date" not in df.columns:
            return df
        dtype = df.schema.get("date")
        out = df
        if dtype == pl.Date:
            return out
        if dtype == pl.Datetime:
            return out.with_columns(pl.col("date").cast(pl.Date, strict=False))
        # Assume string or other -> try multiple parsing strategies and coalesce
        out = out.with_columns([
            pl.coalesce([
                # Exact YYYY-MM-DD
                pl.col("date").str.strptime(pl.Date, format="%Y-%m-%d", strict=False),
                # First 10 chars as date
                pl.col("date").str.slice(0, 10).str.strptime(pl.Date, format="%Y-%m-%d", strict=False),
                # Parse as datetime then cast to date
                pl.col("date").str.strptime(pl.Datetime, strict=False).cast(pl.Date, strict=False),
            ]).alias("date")
        ])
        return out

    def _repo_prefix(self, repo: str) -> str:
        """Map repository name to filename prefix (e.g., scikit-learn -> sklearn)."""
        return "sklearn" if repo == "scikit-learn" else repo

    def _filepath(self, repo: str, metric: str) -> str:
        return os.path.join(self.data_dir, f"{self._repo_prefix(repo)}_{metric}.csv")

    def _generic_filepath(self, metric: str) -> str:
        """Generic metric CSV path (e.g., stars.csv) for single-library dashboards."""
        return os.path.join(self.data_dir, f"{metric}.csv")

    def _is_data_stale(self, filepath: str) -> bool:
        """Check if file is missing or stale."""
        if not os.path.exists(filepath):
            return True
        mod_time = datetime.fromtimestamp(os.path.getmtime(filepath))
        age_hours = (datetime.now() - mod_time).total_seconds() / 3600
        return age_hours > self.refresh_threshold_hours

    def _fetch_and_merge(self, owner: str, repo: str, metric: str, force_full: bool = False) -> pl.DataFrame:
        """Fetch metric data and merge with existing CSV, deduplicate and sort by date.
        If force_full is True, fetch complete history (ignore since_date).
        """
        fetch_map = {
            "stars": self.github_fetcher.stars_fetcher,
            "forks": self.github_fetcher.forks_fetcher,
            "prs": self.github_fetcher.prs_fetcher,
            "downloads": self.github_fetcher.downloads_fetcher,
        }
        fetcher = fetch_map.get(metric)
        if not fetcher:
            return pl.DataFrame({"date": [], "count": []})

        # Determine since_date from cache to enable incremental fetching
        since_date: Optional[datetime] = None
        filepath = self._filepath(repo, metric)
        if os.path.exists(filepath) and not force_full:
            try:
                cached_df_tmp = pl.read_csv(filepath, try_parse_dates=False)
                if len(cached_df_tmp) and "date" in cached_df_tmp.columns:
                    cached_df_tmp = self._parse_date_col(cached_df_tmp)
                    max_date = cached_df_tmp["date"].max()
                    if max_date is not None:
                        # Convert pl.Date to datetime for fetchers
                        since_date = datetime.combine(max_date, datetime.min.time())
            except Exception:
                since_date = None

        fresh_df = fetcher.fetch(owner, repo, since_date=since_date if not force_full else None)
        if fresh_df is None:
            fresh_df = pl.DataFrame({"date": [], "count": []})

        if os.path.exists(filepath):
            try:
                cached_df = pl.read_csv(filepath, try_parse_dates=False)
            except Exception:
                cached_df = pl.DataFrame({"date": [], "count": []})
        else:
            cached_df = pl.DataFrame({"date": [], "count": []})

        # Normalize schemas before concatenation (ensure date=Date, count=Int64)
        def _normalize(df: pl.DataFrame) -> pl.DataFrame:
            if df is None or len(df) == 0:
                return df
            cols = df.columns
            out = df
            if "date" in cols:
                out = self._parse_date_col(out)
            if "count" in cols:
                out = out.with_columns(pl.col("count").cast(pl.Int64, strict=False))
            return out

        cached_df = _normalize(cached_df)
        fresh_df = _normalize(fresh_df)

        combined = pl.concat([cached_df, fresh_df], how="vertical", rechunk=True) if len(cached_df) or len(fresh_df) else fresh_df
        if len(combined):
            combined = (
                combined
                .with_columns([
                    pl.col("date").cast(pl.Date, strict=False),
                    pl.col("count").cast(pl.Int64, strict=False)
                ])
                .drop_nulls(["date"]) 
                .group_by("date")
                .agg(pl.col("count").sum())
                .sort("date")
            )
            # Keep only actual event dates (no artificial zero-padding)
            # Save per-repo CSV
            combined.write_csv(filepath)
            self.logger.info(f"Saved {metric} data → {filepath}\n")
            # Also save generic CSV for convenience when focusing on single library
            gen_path = self._generic_filepath(metric)
            combined.write_csv(gen_path)
            self.logger.info(f"Saved {metric} data → {gen_path}")
        return combined

    def get_data(self, metric: str, owner: str, repo: str, force_refresh: bool = False) -> pl.DataFrame:
        """Return a single metric as Polars DataFrame, updating cache if needed."""
        if metric not in self.metrics:
            self.logger.error(f"Unknown data type: {metric}")
            return pl.DataFrame({"date": [], "count": []})

        filepath = self._filepath(repo, metric)
        if force_refresh or self._is_data_stale(filepath):
            return self._fetch_and_merge(owner, repo, metric)

        if os.path.exists(filepath):
            try:
                df = pl.read_csv(filepath, try_parse_dates=False)
                if len(df) and "date" in df.columns:
                    df = self._parse_date_col(df)
                return df
            except Exception as e:
                self.logger.error(f"Failed to load {metric} CSV: {e}")
                return pl.DataFrame({"date": [], "count": []})
        else:
            # No cache yet → fetch
            return self._fetch_and_merge(owner, repo, metric)

    def get_all_cached_data(self, owner: str, repo: str, force_refresh: bool = False) -> Dict[str, pl.DataFrame]:
        """Return all cached metrics as Polars DataFrames."""
        data: Dict[str, pl.DataFrame] = {}
        for metric in self.metrics:
            data[metric] = self.get_data(metric, owner, repo, force_refresh=force_refresh)
        return data

    def get_real_time_data(self, owner: str, repo: str) -> Dict[str, pl.DataFrame]:
        """
        Fetch real-time metrics that are not cached.
        Returns empty DataFrames if no data is available.
        """
        data: Dict[str, pl.DataFrame] = {}
        try:
            data['contributions'] = pl.DataFrame({"week": [], "commits": []})
            data['issues'] = pl.DataFrame({"date": [], "issue_count": []})
            data['dependents'] = pl.DataFrame({"name": [], "stars": []})
        except Exception as e:
            self.logger.error(f"Error fetching real-time data: {e}")
            data['contributions'] = pl.DataFrame({"week": [], "commits": []})
            data['issues'] = pl.DataFrame({"date": [], "issue_count": []})
            data['dependents'] = pl.DataFrame({"name": [], "stars": []})
        return data

    def get_range_data(self, owner: str, repo: str, start: date, end: date, force_refresh: bool = False) -> Dict[str, pl.DataFrame]:
        """Ensure cache covers [start, end], backfill/fill-forward as needed, then return range."""
        # Ensure coverage for each metric
        for metric in self.metrics:
            fp = self._filepath(repo, metric)
            need_backfill = False
            need_forward = False
            if os.path.exists(fp) and not force_refresh:
                try:
                    cached = pl.read_csv(fp, try_parse_dates=False)
                    if len(cached):
                        cached = self._parse_date_col(cached)
                        min_d = cached["date"].min()
                        max_d = cached["date"].max()
                        if start < min_d:
                            need_backfill = True
                        if end > max_d:
                            need_forward = True
                    else:
                        need_backfill = True
                        need_forward = True
                except Exception:
                    need_backfill = True
                    need_forward = True
            else:
                need_backfill = True
                need_forward = True

            if force_refresh or need_backfill or need_forward:
                self.logger.info(f"Updating cache for {metric}: backfill={need_backfill} forward={need_forward}")
                # For forward fill, we pass since_date (handled inside _fetch_and_merge)
                # For backfill (older gaps), we refetch full history and merge; dedup keeps file small
                self._fetch_and_merge(owner, repo, metric, force_full=need_backfill)

        # Now load all metrics and filter
        all_data = self.get_all_cached_data(owner, repo, force_refresh=False)
        filtered: Dict[str, pl.DataFrame] = {}
        for metric, df in all_data.items():
            if df is None or len(df) == 0 or "date" not in df.columns:
                filtered[metric] = pl.DataFrame({"date": [], "count": []})
                continue
            try:
                df2 = df.with_columns(pl.col("date").cast(pl.Date, strict=False))
                mask = (pl.col("date") >= pl.lit(pl.Date(start))) & (pl.col("date") <= pl.lit(pl.Date(end)))
                filtered[metric] = df2.filter(mask)
            except Exception:
                filtered[metric] = df
        return filtered

    def get_data_status(self) -> Dict[str, dict]:
        """Return the status of cached CSV files."""
        status: Dict[str, dict] = {}
        for key in self.metrics:
            filepath = self._generic_filepath(key)
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
            # Clear for both prefixes potentially used
            for prefix in ["sklearn", "skrub", "scikit-learn"]:
                filepath = os.path.join(self.data_dir, f"{prefix}_{data_type}.csv")
                if os.path.exists(filepath):
                    os.remove(filepath)
                    self.logger.info(f"Cleared cache for {data_type}: {filepath}")
        else:
            for metric in self.metrics:
                for fname in os.listdir(self.data_dir):
                    if fname.endswith(f"_{metric}.csv"):
                        try:
                            os.remove(os.path.join(self.data_dir, fname))
                            self.logger.info(f"Cleared cache for {metric}: {fname}")
                        except OSError:
                            pass
