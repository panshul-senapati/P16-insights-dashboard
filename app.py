import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import date, timedelta, datetime
import os

from loaders.data_loader import DataLoader
from fetchers.github_fetcher import GitHubFetcher

# ----------------------------
# PAGE CONFIGURATION
# ----------------------------
st.set_page_config(page_title="📊 GitHub Insights Dashboard", layout="wide")
st.title("📊 GitHub Insights Dashboard")

# ----------------------------
# SIDEBAR
# ----------------------------
st.sidebar.header("📁 Library Info & Filter")

repo_map = {
    "Skrub": ("skrub-data", "skrub"),
    "tslearn": ("tslearn-team", "tslearn"),
    "scikit-learn": ("scikit-learn", "scikit-learn"),
    "Aeon": ("aeon-toolkit", "aeon"),
    "Mapie": ("scikit-learn-contrib", "mapie"),
}

selected_lib = st.sidebar.selectbox(" Select Library:", list(repo_map.keys()))
owner, repo = repo_map[selected_lib]

today = date.today()
default_start = today - timedelta(days=180)

start_date = st.sidebar.date_input(
    " Select Start Date:", default_start, min_value=date(2010, 1, 1), max_value=today
)
end_date = st.sidebar.date_input(
    " Select End Date:", today, min_value=start_date, max_value=today
)

# ----------------------------
# INITIALIZE LOADERS AND FETCHERS
# ----------------------------
data_loader = DataLoader()
github_fetcher = GitHubFetcher()

# ----------------------------
# CSV PATHS & REFRESH SETTINGS
# ----------------------------
DATA_DIR = "data"
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

CSV_FILES = {
    "stars": os.path.join(DATA_DIR, "stars.csv"),
    "forks": os.path.join(DATA_DIR, "forks.csv"),
    "prs": os.path.join(DATA_DIR, "prs.csv"),
    "downloads": os.path.join(DATA_DIR, "github_downloads.csv"),
}

REFRESH_THRESHOLD_HOURS = 24

def csv_needs_refresh(filepath):
    if not os.path.exists(filepath):
        return True
    mod_time = datetime.fromtimestamp(os.path.getmtime(filepath))
    return (datetime.now() - mod_time).total_seconds() > REFRESH_THRESHOLD_HOURS * 3600

def fetch_and_save_csv(kind):
    # kind in ["stars", "forks", "prs", "downloads"]
    fetch_map = {
        "stars": github_fetcher.get_stars_over_time,
        "forks": github_fetcher.get_forks_over_time,
        "prs": github_fetcher.get_pull_requests_over_time,
        "downloads": github_fetcher.get_downloads_over_time,
    }
    fetch_func = fetch_map.get(kind)
    if fetch_func:
        df = fetch_func(owner, repo)
        if not df.empty:
            df.to_csv(CSV_FILES[kind], index=False)
        return df
    return pd.DataFrame()

def load_or_refresh_csv(kind):
    path = CSV_FILES[kind]
    if csv_needs_refresh(path):
        df = fetch_and_save_csv(kind)
        if df.empty and os.path.exists(path):
            return pd.read_csv(path)
        return df
    else:
        return pd.read_csv(path)

# Load data with refresh logic
stars_df = load_or_refresh_csv("stars")
forks_df = load_or_refresh_csv("forks")
prs_df = load_or_refresh_csv("prs")
downloads_df = load_or_refresh_csv("downloads")

def ensure_datetime(df, date_col="date"):
    if date_col in df.columns:
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    return df

stars_df = ensure_datetime(stars_df)
forks_df = ensure_datetime(forks_df)
prs_df = ensure_datetime(prs_df)
downloads_df = ensure_datetime(downloads_df)

# Dynamic fetch for contributions and issues (no CSV caching)
contributions_df = github_fetcher.get_weekly_contributions(owner, repo)
issues_df = github_fetcher.get_issues_over_time(owner, repo)

# ----------------------------
# FILTERING AND DISPLAY
# ----------------------------
if all([not df.empty for df in [stars_df, forks_df, prs_df, downloads_df]]):
    from_date = pd.to_datetime(start_date)
    to_date = pd.to_datetime(end_date)

    def filter_df(df, date_col="date"):
        return df[(df[date_col] >= from_date) & (df[date_col] <= to_date)]

    filtered_stars = filter_df(stars_df)
    filtered_forks = filter_df(forks_df)
    filtered_prs = filter_df(prs_df)
    filtered_downloads = filter_df(downloads_df)

    # Summary Metrics
    st.subheader(f"📊 Summary for {owner}/{repo}")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Stars", int(filtered_stars["stars"].sum()))
    col2.metric("Total Forks", int(filtered_forks["forks"].sum()))
    col3.metric("Total PRs", int(filtered_prs["pr_count"].sum()))
    col4.metric("Total Downloads", int(filtered_downloads["downloads"].sum()))

    # Charts Row 1
    col1, col2 = st.columns(2)
    with col1:
        fig_stars = px.line(
            filtered_stars,
            x="date",
            y="stars",
            title=" Stars Over Time",
            markers=True,
            template="plotly_white",
            color_discrete_sequence=["#FFD700"],
        )
        st.plotly_chart(fig_stars, use_container_width=True)

    with col2:
        fig_forks = px.line(
            filtered_forks,
            x="date",
            y="forks",
            title=" Forks Over Time",
            markers=True,
            template="plotly_white",
            color_discrete_sequence=["#1f77b4"],
        )
        st.plotly_chart(fig_forks, use_container_width=True)

    # Charts Row 2
    col3, col4 = st.columns(2)
    with col3:
        fig_prs = px.line(
            filtered_prs,
            x="date",
            y="pr_count",
            title=" Pull Requests Over Time",
            markers=True,
            template="plotly_white",
            color_discrete_sequence=["#FF7F0E"],
        )
        st.plotly_chart(fig_prs, use_container_width=True)

    with col4:
        fig_downloads = px.line(
            filtered_downloads,
            x="date",
            y="downloads",
            title=" Downloads Over Time",
            markers=True,
            template="plotly_white",
            color_discrete_sequence=["#9467bd"],
        )
        st.plotly_chart(fig_downloads, use_container_width=True)

    # Contributions Chart
    if not contributions_df.empty:
        filtered_contributions = contributions_df[
            (contributions_df["week"] >= from_date)
            & (contributions_df["week"] <= to_date)
        ]
        if not filtered_contributions.empty:
            contrib_fig = px.line(
                filtered_contributions,
                x="week",
                y="commits",
                title=" Weekly Contributions (Commits)",
                markers=True,
                template="plotly_white",
                color_discrete_sequence=["#2ca02c"],
            )
            st.plotly_chart(contrib_fig, use_container_width=True)
        else:
            st.info("ℹ️ No contribution data found in selected range.")
    else:
        st.info("ℹ️ No contribution data found.")

    # Issues Chart
    if not issues_df.empty:
        filtered_issues = issues_df[
            (issues_df["date"] >= from_date) & (issues_df["date"] <= to_date)
        ]
        if not filtered_issues.empty:
            issues_fig = px.line(
                filtered_issues,
                x="date",
                y="issue_count",
                title=" Issues Over Time",
                markers=True,
                template="plotly_white",
                color_discrete_sequence=["#d62728"],
            )
            st.plotly_chart(issues_fig, use_container_width=True)
        else:
            st.info("ℹ️ No issues data found in selected range.")
    else:
        st.info("ℹ️ No issues data found.")

    # Download Buttons
    st.markdown("###  Download Filtered Data")
    d1, d2, d3, d4 = st.columns(4)
    d1.download_button(
        "⬇️ Stars CSV",
        filtered_stars.to_csv(index=False),
        file_name="filtered_stars.csv",
    )
    d2.download_button(
        "⬇️ Forks CSV",
        filtered_forks.to_csv(index=False),
        file_name="filtered_forks.csv",
    )
    d3.download_button(
        "⬇️ PRs CSV",
        filtered_prs.to_csv(index=False),
        file_name="filtered_prs.csv",
    )
    d4.download_button(
        "⬇️ Downloads CSV",
        filtered_downloads.to_csv(index=False),
        file_name="filtered_downloads.csv",
    )

else:
    st.error(" One or more input CSV files are missing, empty, or invalid.")

# ----------------------------
# DEPENDENTS SECTION
# ----------------------------
st.markdown("## 🔗 Public GitHub Dependents")
dependents_df = github_fetcher.get_dependents(owner, repo)

if not dependents_df.empty:
    df_sorted = dependents_df.sort_values(by="stars", ascending=False)

    counts = {
        "Below 100 stars": (df_sorted["stars"] < 100).sum(),
        "100 to 1000 stars": (
            (df_sorted["stars"] >= 100) & (df_sorted["stars"] <= 1000)
        ).sum(),
        "1000+ stars": (df_sorted["stars"] > 1000).sum(),
    }

    final_df = pd.DataFrame(
        list(counts.items()), columns=["Star Range", "Library Count"]
    )
    st.dataframe(df_sorted, use_container_width=True)
    fig_dep = px.bar(
        final_df,
        x="Star Range",
        y="Library Count",
        title="Dependents by Star Range",
        template="plotly_white",
    )
    st.plotly_chart(fig_dep, use_container_width=True)
else:
    st.info("ℹ️ No public dependents found.")

# ----------------------------
# HIDE STREAMLIT DEFAULT UI
# ----------------------------
hide_st_style = """
    <style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    </style>
"""
st.markdown(hide_st_style, unsafe_allow_html=True)