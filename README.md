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
   Create a virtual environment
   python3 -m venv venv
source venv/bin/activate   # On Windows: venv\Scripts\activate

	Install dependencies
 pip install -r requirements.txt

 Run the dashboard
streamlit run app.py




Screenshots:
<img width="1470" height="761" alt="Screenshot 2025-08-12 at 1 48 52 PM" src="https://github.com/user-attachments/assets/54a47519-06dd-431b-9ebf-27bb76767ba4" />
<img width="1470" height="761" alt="Screenshot 2025-08-12 at 1 49 16 PM" src="https://github.com/user-attachments/assets/c2e5be80-7011-4d91-8502-bcbd93479130" />

