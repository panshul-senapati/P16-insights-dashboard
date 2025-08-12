import requests
import os
import pandas as pd

# ---------------- CONFIG ----------------
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
OWNER = "scikit-learn"  # Change as needed
REPO = "scikit-learn"   # Change as needed
DATA_DIR = "data"       # Folder where your dashboard CSVs are stored
# ---------------------------------------

if not GITHUB_TOKEN:
    raise ValueError("Please set your GITHUB_TOKEN environment variable")

HEADERS = {"Authorization": f"Bearer {GITHUB_TOKEN.strip()}"}

def run_query(query, variables):
    response = requests.post(
        'https://api.github.com/graphql',
        json={'query': query, 'variables': variables},
        headers=HEADERS
    )
    if response.status_code != 200:
        raise Exception(f"Query failed ({response.status_code}): {response.text}")
    return response.json()

def fetch_stars():
    print("Fetching stars history...")
    results = {}
    cursor = None
    while True:
        query = """
        query($owner: String!, $repo: String!, $after: String) {
          repository(owner: $owner, name: $repo) {
            stargazers(first: 100, after: $after) {
              edges {
                starredAt
              }
              pageInfo {
                hasNextPage
                endCursor
              }
            }
          }
        }
        """
        variables = {"owner": OWNER, "repo": REPO, "after": cursor}
        data = run_query(query, variables)
        if "errors" in data:
            raise Exception(f"GraphQL errors: {data['errors']}")
        edges = data["data"]["repository"]["stargazers"]["edges"]
        for edge in edges:
            date = edge["starredAt"][:10]
            results[date] = results.get(date, 0) + 1
        page_info = data["data"]["repository"]["stargazers"]["pageInfo"]
        if page_info["hasNextPage"]:
            cursor = page_info["endCursor"]
        else:
            break

    df = pd.DataFrame(list(results.items()), columns=["date", "stars"])
    df = df.sort_values("date").reset_index(drop=True)
    df["stars"] = df["stars"].cumsum()
    return df

def fetch_forks():
    print("Fetching forks history...")
    results = {}
    cursor = None
    while True:
        query = """
        query($owner: String!, $repo: String!, $after: String) {
          repository(owner: $owner, name: $repo) {
            forks(first: 100, after: $after, orderBy: {field: CREATED_AT, direction: ASC}) {
              edges {
                node {
                  createdAt
                }
              }
              pageInfo {
                hasNextPage
                endCursor
              }
            }
          }
        }
        """
        variables = {"owner": OWNER, "repo": REPO, "after": cursor}
        data = run_query(query, variables)
        if "errors" in data:
            raise Exception(f"GraphQL errors: {data['errors']}")
        edges = data["data"]["repository"]["forks"]["edges"]
        for edge in edges:
            date = edge["node"]["createdAt"][:10]
            results[date] = results.get(date, 0) + 1
        page_info = data["data"]["repository"]["forks"]["pageInfo"]
        if page_info["hasNextPage"]:
            cursor = page_info["endCursor"]
        else:
            break

    df = pd.DataFrame(list(results.items()), columns=["date", "forks"])
    df = df.sort_values("date").reset_index(drop=True)
    df["forks"] = df["forks"].cumsum()
    return df

def fetch_pull_requests():
    print("Fetching pull requests history...")
    results = {}
    cursor = None
    while True:
        query = """
        query($owner: String!, $repo: String!, $after: String) {
          repository(owner: $owner, name: $repo) {
            pullRequests(first: 100, after: $after, orderBy: {field: CREATED_AT, direction: ASC}) {
              edges {
                node {
                  createdAt
                }
              }
              pageInfo {
                hasNextPage
                endCursor
              }
            }
          }
        }
        """
        variables = {"owner": OWNER, "repo": REPO, "after": cursor}
        data = run_query(query, variables)
        if "errors" in data:
            raise Exception(f"GraphQL errors: {data['errors']}")
        edges = data["data"]["repository"]["pullRequests"]["edges"]
        for edge in edges:
            date = edge["node"]["createdAt"][:10]
            results[date] = results.get(date, 0) + 1
        page_info = data["data"]["repository"]["pullRequests"]["pageInfo"]
        if page_info["hasNextPage"]:
            cursor = page_info["endCursor"]
        else:
            break

    df = pd.DataFrame(list(results.items()), columns=["date", "pr_count"])
    df = df.sort_values("date").reset_index(drop=True)
    df["pr_count"] = df["pr_count"].cumsum()
    return df

def fetch_issues():
    print("Fetching issues history...")
    results = {}
    cursor = None
    while True:
        query = """
        query($owner: String!, $repo: String!, $after: String) {
          repository(owner: $owner, name: $repo) {
            issues(first: 100, after: $after, orderBy: {field: CREATED_AT, direction: ASC}) {
              edges {
                node {
                  createdAt
                }
              }
              pageInfo {
                hasNextPage
                endCursor
              }
            }
          }
        }
        """
        variables = {"owner": OWNER, "repo": REPO, "after": cursor}
        data = run_query(query, variables)
        if "errors" in data:
            raise Exception(f"GraphQL errors: {data['errors']}")
        edges = data["data"]["repository"]["issues"]["edges"]
        for edge in edges:
            date = edge["node"]["createdAt"][:10]
            results[date] = results.get(date, 0) + 1
        page_info = data["data"]["repository"]["issues"]["pageInfo"]
        if page_info["hasNextPage"]:
            cursor = page_info["endCursor"]
        else:
            break

    df = pd.DataFrame(list(results.items()), columns=["date", "issue_count"])
    df = df.sort_values("date").reset_index(drop=True)
    df["issue_count"] = df["issue_count"].cumsum()
    return df

def merge_with_existing(filename, new_df):
    filepath = os.path.join(DATA_DIR, filename)
    if os.path.exists(filepath):
        old_df = pd.read_csv(filepath)
        combined = pd.concat([old_df, new_df])
        combined = combined.drop_duplicates(subset=["date"]).sort_values("date").reset_index(drop=True)
        combined = combined.reset_index(drop=True)
        # Ensure cumulative sums stay consistent (take max for each date)
        combined_grouped = combined.groupby("date").max().reset_index()
        # Re-cumulate if needed:
        cols = [col for col in combined_grouped.columns if col != "date"]
        for col in cols:
            combined_grouped[col] = combined_grouped[col].cummax()
        combined_grouped.to_csv(filepath, index=False)
        print(f"Updated {filename} with merged data.")
    else:
        new_df.to_csv(filepath, index=False)
        print(f"Saved new file {filename}.")

def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    # Only fetch stars and forks if data files do NOT exist
    stars_path = os.path.join(DATA_DIR, "stars.csv")
    if os.path.exists(stars_path):
        print("stars.csv exists, skipping stars fetch.")
        stars_df = pd.read_csv(stars_path)
    else:
        stars_df = fetch_stars()
    merge_with_existing("stars.csv", stars_df)

    forks_path = os.path.join(DATA_DIR, "forks.csv")
    if os.path.exists(forks_path):
        print("forks.csv exists, skipping forks fetch.")
        forks_df = pd.read_csv(forks_path)
    else:
        forks_df = fetch_forks()
    merge_with_existing("forks.csv", forks_df)

    # Always fetch PRs and issues to get latest data
    prs_df = fetch_pull_requests()
    merge_with_existing("prs.csv", prs_df)

    issues_df = fetch_issues()
    merge_with_existing("issues.csv", issues_df)

if __name__ == "__main__":
    main()