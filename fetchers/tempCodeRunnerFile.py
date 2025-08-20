"""
Fetchers - Responsible for fetching data from external sources (GitHub API, PyPI, etc.)
and saving it to the data folder for loaders to access.
"""

import os
import json
import time
import logging
import requests
import pandas as pd
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BaseFetcher(ABC):
    """Base class for all fetchers."""
    
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
    
    @abstractmethod
    def fetch_and_save(self, owner: str, repo: str) -> bool:
        """Fetch data and save to data directory. Returns True if successful."""
        pass
    
    def _get_github_headers(self) -> Dict[str, str]:
        """Get GitHub API headers with token if available."""
        headers = {
            'Accept': 'application/vnd.github.v3+json',
            'User-Agent': 'GitHub-Analytics-Dashboard'
        }
        
        # Add token if available in environment
        token = os.getenv('GITHUB_TOKEN')
        if token:
            headers['Authorization'] = f'token {token}'
        
        return headers
    
    def _make_request(self, url: str, params: Optional[Dict] = None) -> Optional[requests.Response]:
        """Make HTTP request with error handling and rate limiting."""
        try:
            headers = self._get_github_headers()
            response = requests.get(url, headers=headers, params=params, timeout=30)
            
            # Handle rate limiting
            if response.status_code == 403 and 'rate limit' in response.text.lower():
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


class StarsFetcher(BaseFetcher):
    """Fetch repository star history."""
    
    def fetch_and_save(self, owner: str, repo: str) -> bool:
        """Fetch stars history and save as CSV."""
        logger.info(f"Fetching stars data for {owner}/{repo}")
        
        # Generate synthetic star data (in real implementation, use GitHub API)
        data = self._generate_star_data(owner, repo)
        
        if data:
            filename = f"{owner}_{repo}_stars.csv"
            filepath = os.path.join(self.data_dir, filename)
            
            df = pd.DataFrame(data)
            df.to_csv(filepath, index=False)
            logger.info(f"Saved stars data to {filepath}")
            return True
        
        return False
    
    def _generate_star_data(self, owner: str, repo: str) -> List[Dict]:
        """Generate synthetic star data for demo purposes."""
        # In real implementation, this would call GitHub API
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        
        dates = pd.date_range(start=start_date, end=end_date, freq='D')
        stars_data = []
        
        cumulative_stars = np.random.randint(1000, 5000)  # Starting stars
        
        for date in dates:
            # Simulate daily star growth
            daily_growth = max(0, np.random.poisson(5))
            cumulative_stars += daily_growth
            
            stars_data.append({
                'date': date.strftime('%Y-%m-%d'),
                'stars': cumulative_stars
            })
        
        return stars_data


class ForksFetcher(BaseFetcher):
    """Fetch repository fork history."""
    
    def fetch_and_save(self, owner: str, repo: str) -> bool:
        """Fetch forks history and save as CSV."""
        logger.info(f"Fetching forks data for {owner}/{repo}")
        
        data = self._generate_fork_data(owner, repo)
        
        if data:
            filename = f"{owner}_{repo}_forks.csv"
            filepath = os.path.join(self.data_dir, filename)
            
            df = pd.DataFrame(data)
            df.to_csv(filepath, index=False)
            logger.info(f"Saved forks data to {filepath}")
            return True
        
        return False
    
    def _generate_fork_data(self, owner: str, repo: str) -> List[Dict]:
        """Generate synthetic fork data."""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        
        dates = pd.date_range(start=start_date, end=end_date, freq='D')
        forks_data = []
        
        cumulative_forks = np.random.randint(100, 1000)
        
        for date in dates:
            daily_growth = max(0, np.random.poisson(2))
            cumulative_forks += daily_growth
            
            forks_data.append({
                'date': date.strftime('%Y-%m-%d'),
                'forks': cumulative_forks
            })
        
        return forks_data


class PRsFetcher(BaseFetcher):
    """Fetch pull requests data."""
    
    def fetch_and_save(self, owner: str, repo: str) -> bool:
        """Fetch PRs data and save as CSV."""
        logger.info(f"Fetching PRs data for {owner}/{repo}")
        
        data = self._generate_pr_data(owner, repo)
        
        if data:
            filename = f"{owner}_{repo}_prs.csv"
            filepath = os.path.join(self.data_dir, filename)
            
            df = pd.DataFrame(data)
            df.to_csv(filepath, index=False)
            logger.info(f"Saved PRs data to {filepath}")
            return True
        
        return False
    
    def _generate_pr_data(self, owner: str, repo: str) -> List[Dict]:
        """Generate synthetic PR data."""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        
        dates = pd.date_range(start=start_date, end=end_date, freq='W')  # Weekly data
        pr_data = []
        
        for date in dates:
            weekly_prs = np.random.poisson(8)  # Average 8 PRs per week
            
            pr_data.append({
                'date': date.strftime('%Y-%m-%d'),
                'pr_count': weekly_prs
            })
        
        return pr_data


class DownloadsFetcher(BaseFetcher):
    """Fetch package downloads data (e.g., from PyPI)."""
    
    def fetch_and_save(self, owner: str, repo: str) -> bool:
        """Fetch downloads data and save as CSV."""
        logger.info(f"Fetching downloads data for {owner}/{repo}")
        
        # Try to fetch real PyPI data, fallback to synthetic
        data = self._fetch_pypi_downloads(repo) or self._generate_downloads_data(owner, repo)
        
        if data:
            filename = f"{owner}_{repo}_downloads.csv"
            filepath = os.path.join(self.data_dir, filename)
            
            df = pd.DataFrame(data)
            df.to_csv(filepath, index=False)
            logger.info(f"Saved downloads data to {filepath}")
            return True
        
        return False
    
    def _fetch_pypi_downloads(self, package_name: str) -> Optional[List[Dict]]:
        """Attempt to fetch real PyPI download data."""
        try:
            # This is a simplified example - real implementation would use pypistats
            url = f"https://pypistats.org/api/packages/{package_name}/recent"
            response = self._make_request(url)
            
            if response:
                # Parse PyPI response (implementation depends on API structure)
                pass
        except Exception as e:
            logger.debug(f"PyPI fetch failed: {e}")
        
        return None
    
    def _generate_downloads_data(self, owner: str, repo: str) -> List[Dict]:
        """Generate synthetic downloads data."""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        
        dates = pd.date_range(start=start_date, end=end_date, freq='D')
        downloads_data = []
        
        for date in dates:
            daily_downloads = np.random.poisson(1000)  # Average 1000 downloads per day
            
            downloads_data.append({
                'date': date.strftime('%Y-%m-%d'),
                'downloads': daily_downloads
            })
        
        return downloads_data


class ContributionsFetcher(BaseFetcher):
    """Fetch contributions/commits data."""
    
    def fetch_and_save(self, owner: str, repo: str) -> bool:
        """Fetch contributions data and save as CSV."""
        logger.info(f"Fetching contributions data for {owner}/{repo}")
        
        data = self._generate_contributions_data(owner, repo)
        
        if data:
            filename = f"{owner}_{repo}_contributions.csv"
            filepath = os.path.join(self.data_dir, filename)
            
            df = pd.DataFrame(data)
            df.to_csv(filepath, index=False)
            logger.info(f"Saved contributions data to {filepath}")
            return True
        
        return False
    
    def _generate_contributions_data(self, owner: str, repo: str) -> List[Dict]:
        """Generate synthetic contributions data."""
        end_date = datetime.now()
        start_date = end_date - timedelta(weeks=52)
        
        weeks = pd.date_range(start=start_date, end=end_date, freq='W')
        contributions_data = []
        
        for week in weeks:
            weekly_commits = np.random.poisson(25)  # Average 25 commits per week
            
            contributions_data.append({
                'week': week.strftime('%Y-%m-%d'),
                'commits': weekly_commits
            })
        
        return contributions_data


class IssuesFetcher(BaseFetcher):
    """Fetch issues data."""
    
    def fetch_and_save(self, owner: str, repo: str) -> bool:
        """Fetch issues data and save as CSV."""
        logger.info(f"Fetching issues data for {owner}/{repo}")
        
        data = self._generate_issues_data(owner, repo)
        
        if data:
            filename = f"{owner}_{repo}_issues.csv"
            filepath = os.path.join(self.data_dir, filename)
            
            df = pd.DataFrame(data)
            df.to_csv(filepath, index=False)
            logger.info(f"Saved issues data to {filepath}")
            return True
        
        return False
    
    def _generate_issues_data(self, owner: str, repo: str) -> List[Dict]:
        """Generate synthetic issues data."""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        
        dates = pd.date_range(start=start_date, end=end_date, freq='W')
        issues_data = []
        
        for date in dates:
            weekly_issues = np.random.poisson(5)  # Average 5 issues per week
            
            issues_data.append({
                'date': date.strftime('%Y-%m-%d'),
                'issue_count': weekly_issues
            })
        
        return issues_data


class DependentsFetcher(BaseFetcher):
    """Fetch repository dependents data."""
    
    def fetch_and_save(self, owner: str, repo: str) -> bool:
        """Fetch dependents data and save as JSON."""
        logger.info(f"Fetching dependents data for {owner}/{repo}")
        
        data = self._generate_dependents_data(owner, repo)
        
        if data:
            filename = f"{owner}_{repo}_dependents.json"
            filepath = os.path.join(self.data_dir, filename)
            
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2)
            logger.info(f"Saved dependents data to {filepath}")
            return True
        
        return False
    
    def _generate_dependents_data(self, owner: str, repo: str) -> List[Dict]:
        """Generate synthetic dependents data."""
        # Simulate repository dependents
        dependents = []
        
        for i in range(np.random.randint(10, 50)):
            dependent = {
                'name': f"dependent-repo-{i+1}",
                'owner': f"user{i+1}",
                'stars': np.random.randint(0, 5000),
                'forks': np.random.randint(0, 500),
                'description': f"A project that depends on {repo}",
                'language': np.random.choice(['Python', 'JavaScript', 'Java', 'Go', 'Rust']),
                'url': f"https://github.com/user{i+1}/dependent-repo-{i+1}"
            }
            dependents.append(dependent)
        
        return dependents
    
    def _fetch_real_dependents(self, owner: str, repo: str) -> Optional[List[Dict]]:
        """Fetch real dependents from GitHub (placeholder for real implementation)."""
        # In real implementation, this would:
        # 1. Search GitHub for repositories that depend on this package
        # 2. Parse dependency files (requirements.txt, package.json, etc.)
        # 3. Use GitHub's dependency graph API if available
        
        url = f"https://api.github.com/repos/{owner}/{repo}/network/dependents"
        response = self._make_request(url)
        
        if response:
            # Parse and return dependents data
            pass
        
        return None