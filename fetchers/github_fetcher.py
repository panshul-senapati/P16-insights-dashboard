import os
import time
import json
import base64
import subprocess
import requests
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class GitHubFetcher:
    """Handles all GitHub API data fetching operations with authentication & retries."""

    def __init__(self):
        self.github_token = os.getenv("GITHUB_TOKEN", None)
        self.headers = {
            "Authorization": f"token {self.github_token}",
            "Accept": "application/vnd.github.v3+json"
        } if self.github_token else {
            "Accept": "application/vnd.github.v3+json"
        }
        self.base_url = "https://api.github.com"

    def _request(self, url, max_retries=3, sleep_time=2):
        """Make an authenticated GET request with retries."""
        for attempt in range(max_retries):
            response = requests.get(url, headers=self.headers)
            
            # Rate limit hit
            if response.status_code == 403 and "X-RateLimit-Remaining" in response.headers:
                reset_time = int(response.headers.get("X-RateLimit-Reset", time.time() + 60))
                wait_for = int(reset_time - time.time()) + 1
                st.warning(f"⏳ Rate limit hit. Waiting {wait_for} seconds...")
                time.sleep(wait_for)
                continue
            
            # Data not ready yet (for stats endpoints)
            if response.status_code == 202:
                time.sleep(sleep_time)
                continue
            
            return response
        
        return response

    def get_release_downloads(self, owner, repo):
        """Fetch GitHub release download statistics."""
        url = f"{self.base_url}/repos/{owner}/{repo}/releases"
        response = self._request(url)
        
        if response.status_code != 200:
            st.warning(f"Failed to fetch releases: Status {response.status_code}")
            return pd.DataFrame()
        
        releases = response.json()
        downloads_data = []
        
        for release in releases:
            tag = release.get("tag_name", "N/A")
            for asset in release.get("assets", []):
                downloads_data.append({
                    "Release": tag,
                    "Asset": asset.get("name", "unknown"),
                    "Downloads": asset.get("download_count", 0),
                    "Uploaded": asset.get("created_at", "")[:10]
                })
        
        return pd.DataFrame(downloads_data)

    def get_weekly_contributions(self, owner, repo):
        """Fetch weekly contribution statistics from GitHub."""
        url = f"{self.base_url}/repos/{owner}/{repo}/stats/commit_activity"
        
        # This endpoint can return 202 until GitHub generates the stats
        for _ in range(10):
            response = self._request(url)
            if response.status_code == 202:
                time.sleep(3)
                continue
            elif response.status_code == 200:
                break
            else:
                st.warning(f"Failed to fetch contributions: Status {response.status_code}")
                return pd.DataFrame()
        
        data = response.json()
        if not data:
            return pd.DataFrame()
        
        df = pd.DataFrame([{
            "week": pd.to_datetime(item["week"], unit="s"),
            "commits": item["total"]
        } for item in data])
        
        return df

    def get_issues_over_time(self, owner, repo):
        """Fetch issues creation data over time."""
        issues = []
        page = 1
        
        while True:
            url = f"{self.base_url}/repos/{owner}/{repo}/issues?state=all&per_page=100&page={page}"
            response = self._request(url)
            
            if response.status_code != 200:
                st.warning(f"Issues API failed with status {response.status_code}")
                break
            
            page_data = response.json()
            if not page_data:
                break
            
            for issue in page_data:
                if "pull_request" not in issue:
                    issues.append({"created_at": issue.get("created_at", "")})
            
            page += 1
            if page > 10:
                break
        
        if not issues:
            return pd.DataFrame()
        
        df = pd.DataFrame(issues)
        df["date"] = pd.to_datetime(df["created_at"]).dt.tz_localize(None).dt.normalize()
        return df.groupby("date").size().reset_index(name="issue_count")

    def get_dependencies(self, owner, repo):
        """Fetch repository dependencies from manifest files."""
        possible_files = ["requirements.txt", "environment.yml", "package.json"]
        dependencies = []

        for file in possible_files:
            url = f"{self.base_url}/repos/{owner}/{repo}/contents/{file}"
            res = self._request(url)
            
            if res.status_code == 200:
                content_data = res.json()
                if content_data.get("encoding") == "base64":
                    decoded = base64.b64decode(content_data["content"]).decode("utf-8")
                    
                    if file == "package.json":
                        try:
                            package_json = json.loads(decoded)
                            deps = package_json.get("dependencies", {})
                            dependencies = [{"Dependency": k, "Version": v} for k, v in deps.items()]
                        except json.JSONDecodeError:
                            st.warning("Invalid package.json format.")
                    else:
                        for line in decoded.splitlines():
                            if line.strip() and not line.startswith("#"):
                                if "==" in line:
                                    pkg, ver = line.split("==", 1)
                                    dependencies.append({"Dependency": pkg.strip(), "Version": ver.strip()})
                                else:
                                    dependencies.append({"Dependency": line.strip(), "Version": "N/A"})
                    break
        
        return pd.DataFrame(dependencies)

    def get_dependents(self, owner, repo):
        """Fetch public GitHub dependents using github-dependents-info tool."""
        try:
            full_repo = f"{owner}/{repo}"
            result = subprocess.run(
                ["github-dependents-info", "--repo", full_repo, "--json"],
                capture_output=True,
                text=True,
                check=True
            )
            
            output_data = json.loads(result.stdout)
            dependents = output_data.get("all_public_dependent_repos", [])
            
            if dependents:
                return pd.DataFrame(dependents)[["name", "stars"]]
            else:
                return pd.DataFrame()
                
        except subprocess.CalledProcessError:
            st.error("❌ Failed to run `github-dependents-info`. Is it installed?")
            return pd.DataFrame()
        except json.JSONDecodeError:
            st.error("❌ Failed to parse JSON output from `github-dependents-info`.")
            return pd.DataFrame()