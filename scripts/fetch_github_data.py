import os
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()  # Load GITHUB_TOKEN from .env

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

if not GITHUB_TOKEN:
    raise Exception("GITHUB_TOKEN environment variable not found!")

    
HEADERS = {"Authorization": f"Bearer {GITHUB_TOKEN}"}
API_URL = "https://api.github.com"

def fetch_pull_requests(owner, repo):
    print(f"🔁 Fetching pull requests for {owner}/{repo}...")
    pr_counts = []
    page = 1
    per_page = 100

    while True:
        url = f"{API_URL}/repos/{owner}/{repo}/pulls?state=all&per_page={per_page}&page={page}"
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 401:
            print(f"❌ Error fetching PRs: 401 Unauthorized. Check your token.")
            return None
        if response.status_code != 200:
            print(f"❌ Error fetching PRs: {response.status_code} {response.text}")
            return None

        prs = response.json()
        if not prs:
            break

        # Count PRs by created_at date
        for pr in prs:
            created_date = pr["created_at"][:10]  # YYYY-MM-DD
            pr_counts.append(created_date)

        page += 1

    if not pr_counts:
        print("⚠️ No pull request data found.")
        return None

    df = pd.DataFrame(pr_counts, columns=["date"])
    df['pr_count'] = 1
    df = df.groupby("date").count().reset_index().sort_values("date")
    return df

def fetch_repo_info(owner, repo):
    print(f"📊 Fetching repo info for {owner}/{repo}...")
    url = f"{API_URL}/repos/{owner}/{repo}"
    response = requests.get(url, headers=HEADERS)
    if response.status_code == 401:
        print(f"❌ Error fetching repo info: 401 Unauthorized. Check your token.")
        return None
    if response.status_code != 200:
        print(f"❌ Error fetching repo info: {response.status_code} {response.text}")
        return None
    data = response.json()
    return data

def save_csv(df, filename):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    df.to_csv(filename, index=False)
    print(f"✅ Saved {filename}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("Usage: python fetch_github_data.py owner repo")
        exit(1)

    owner, repo = sys.argv[1], sys.argv[2]

    pr_df = fetch_pull_requests(owner, repo)
    if pr_df is not None:
        save_csv(pr_df, "data/github_pull_requests.csv")

    repo_info = fetch_repo_info(owner, repo)
    if repo_info:
        print(f"Repo info: Stars={repo_info['stargazers_count']}, Forks={repo_info['forks_count']}, Watchers={repo_info['watchers_count']}")
