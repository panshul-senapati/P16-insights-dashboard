# 📊 GitHub Insights Dashboard

An interactive dashboard to analyze and visualize GitHub repository insights — including contributors, commit activity, and repository statistics — built with **Python**, **Streamlit**, and **GitHub API**.

---

## 🚀 Features

- **Contributor Insights** — View contributor statistics such as commits, additions, and deletions.
- **Commit Activity** — Analyze commit frequency over time.
- **Repository Statistics** — Stars, forks, issues, and pull request trends.
- **Interactive Filters** — Select time ranges and contributors.
- **Secure API Access** — Uses environment variables for GitHub tokens (no secrets stored in the repo).

---

## 🛠️ Tech Stack

- **Python** — Data processing and API integration.
- **Streamlit** — Interactive and responsive UI.
- **GitHub GraphQL and REST API** — Repository data source.
- **Pandas** — Data wrangling and transformations.
- **Matplotlib / Plotly** — Visualizations.

---

## 📂 Project Structure
github-insights-dashboard/
│
├── fetchers/                  # Scripts to fetch data from GitHub API
├── loaders/                   # Data loading and processing scripts
├── app.py                     # Streamlit dashboard entry point
├── requirements.txt           # Project dependencies
├── README.md                  # Project documentation
└── .gitignore                 # Ignored files

---

## ⚡ Installation & Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/panshul-senapati/P16-insights-dashboard.git
   cd P16-insights-dashboard

   #Create a virtual environment
   python3 -m venv venv
   source venv/bin/activate   # On Windows: venv\Scripts\activate

   #Install dependencies
   pip install -r requirements.txt

   #Run the dashboard
   streamlit run app.py
   ```



Screenshots:
<img width="1470" height="761" alt="Screenshot 2025-08-12 at 1 48 52 PM" src="https://github.com/user-attachments/assets/54a47519-06dd-431b-9ebf-27bb76767ba4" />
<img width="1470" height="761" alt="Screenshot 2025-08-12 at 1 49 16 PM" src="https://github.com/user-attachments/assets/c2e5be80-7011-4d91-8502-bcbd93479130" />


1️⃣ Overall Architecture

Here’s the big picture flow:
	1.	User interacts via Streamlit UI (dropdowns, date selectors, filters).
	2.	App decides whether to load cached CSVs or fetch fresh data from GitHub APIs.
	3.	Data Fetchers (in github_fetcher.py) call GitHub APIs (GraphQL or REST) to get stars, forks, PRs, downloads, contributions, and issues.
	4.	Data is saved to CSV files in the data/ folder for caching.
	5.	Pandas processes the CSV data → filters by date range → aggregates totals.
	6.	Plotly visualizes the data in charts.
	7.	Extra metrics (dependents, downloads) are fetched and displayed dynamically.

⸻

2️⃣ Main Components in the Code

A. Streamlit Frontend
	•	Where: The file you posted (app.py or similar).
	•	What it does:
	•	Configures the Streamlit page (st.set_page_config).
	•	Creates the sidebar with:
	•	Library selector (repo_map)
	•	Start & end date pickers
	•	Displays Summary metrics: Total Stars, Forks, PRs, Downloads.
	•	Plots time series charts using Plotly for each metric.
	•	Adds download buttons to export filtered CSV data.
	•	Shows dependents chart with star ranges.

Key benefit:
Keeps the interface interactive, and all filtering happens client-side after loading CSVs.

⸻

B. Data Layer

This is split into two helper classes:

1. DataLoader
	•	Loads local CSV data.
	•	Handles simple transformations (ensuring date columns are parsed).

2. GitHubFetcher
	•	The main connection to GitHub APIs.
	•	Implements methods:
	•	get_stars_over_time() → historical stars per day
	•	get_forks_over_time() → forks per day
	•	get_pull_requests_over_time() → PR counts over time
	•	get_downloads_over_time() → download stats (likely from GitHub releases API)
	•	get_weekly_contributions() → weekly commit counts
	•	get_issues_over_time() → issue counts over time
	•	get_dependents() → fetches public repositories that depend on the repo

Under the hood:
	•	GraphQL API → used for stars, forks, PRs, and issues (when you migrated to full historical data).
	•	REST API → might still be used for contributions and downloads depending on whether those are migrated.

