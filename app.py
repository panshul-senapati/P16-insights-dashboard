import os
import pandas as pd
import plotly.express as px
import streamlit as st
from datetime import date, timedelta
from data_manager import DataManager
import time
from typing import Dict

# ----------------------------
# CONFIGURE GITHUB TOKEN (FROM ENV VARIABLE ONLY)
# ----------------------------
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", None)

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
    "Scikit-learn": ("scikit-learn", "scikit-learn"),
}
selected_lib = st.sidebar.selectbox(" Select Library:", list(repo_map.keys()))
owner, repo = repo_map[selected_lib]

today = date.today()
# Set default to cover more historical data since we have data from 2018
default_start = date(2018, 1, 1)

start_date = st.sidebar.date_input(" Select Start Date:", default_start, min_value=date(2010, 1, 1), max_value=today)
end_date = st.sidebar.date_input(" Select End Date:", today, min_value=start_date, max_value=today)

# ----------------------------
# LOAD CSV FUNCTION (decouple fetch from date changes)
# ----------------------------
@st.cache_data(show_spinner=False)
def get_data(owner: str, repo: str, force_refresh: bool = False) -> Dict[str, pd.DataFrame]:
    """Get all data for the repository, with proper refresh handling."""
    dm = DataManager(data_dir="data", refresh_threshold_hours=24)
    loader = dm.loader
    types_to_load = ["stars", "forks", "prs", "downloads", "issues", "contributions"]
    result = {}
    
    # Check if we have GitHub token for real data
    has_github_token = bool(os.getenv("GITHUB_TOKEN"))
    
    # Check each metric and load from cache if available
    for metric_type in types_to_load:
        try:
            if force_refresh:
                # Force refresh: always fetch fresh data from API
                if has_github_token:
                    try:
                        result[metric_type] = dm.get_data(metric_type, owner, repo, force_refresh=True)
                        st.sidebar.success(f"✅ {metric_type}: Fetched fresh data ({len(result[metric_type])} rows)")
                    except Exception as api_error:
                        st.sidebar.error(f"❌ {metric_type}: API failed - {str(api_error)}")
                        # Fall back to manual fetch
                        if metric_type == "stars":
                            result[metric_type] = dm.fetcher.stars_fetcher.fetch(owner, repo)
                        elif metric_type == "forks":
                            result[metric_type] = dm.fetcher.forks_fetcher.fetch(owner, repo)
                        elif metric_type == "prs":
                            result[metric_type] = dm.fetcher.prs_fetcher.fetch(owner, repo)
                        elif metric_type == "downloads":
                            result[metric_type] = dm.fetcher.downloads_fetcher.fetch(owner, repo)
                        elif metric_type == "issues":
                            result[metric_type] = dm.fetcher.issues_fetcher.fetch(owner, repo)
                        elif metric_type == "contributions":
                            result[metric_type] = dm.fetcher.contributions_fetcher.fetch(owner, repo)
                else:
                    # No GitHub token, use manual fetch
                    if metric_type == "stars":
                        result[metric_type] = dm.fetcher.stars_fetcher.fetch(owner, repo)
                    elif metric_type == "forks":
                        result[metric_type] = dm.fetcher.forks_fetcher.fetch(owner, repo)
                    elif metric_type == "prs":
                        result[metric_type] = dm.fetcher.prs_fetcher.fetch(owner, repo)
                    elif metric_type == "downloads":
                        result[metric_type] = dm.fetcher.downloads_fetcher.fetch(owner, repo)
                    elif metric_type == "issues":
                        result[metric_type] = dm.fetcher.issues_fetcher.fetch(owner, repo)
                    elif metric_type == "contributions":
                        result[metric_type] = dm.fetcher.contributions_fetcher.fetch(owner, repo)
            else:
                # Normal loading: check cache first
                cached_data = loader.get_for(metric_type, owner, repo)
                if not cached_data.empty:
                    result[metric_type] = cached_data
                    # st.sidebar.success(f"✅ {metric_type}: Using cached data ({len(cached_data)} rows)")
                else:
                    # If no cached data and we have GitHub token, try to fetch real data
                    if has_github_token and hasattr(dm, "get_data"):
                        try:
                            result[metric_type] = dm.get_data(metric_type, owner, repo, force_refresh=True)
                            # st.sidebar.success(f"✅ {metric_type}: Fetched fresh data ({len(result[metric_type])} rows)")
                        except Exception as api_error:
                            st.sidebar.error(f"❌ {metric_type}: API failed - {str(api_error)}")
                            # Fall back to manual fetch
                            if metric_type == "stars":
                                result[metric_type] = dm.fetcher.stars_fetcher.fetch(owner, repo)
                            elif metric_type == "forks":
                                result[metric_type] = dm.fetcher.forks_fetcher.fetch(owner, repo)
                            elif metric_type == "prs":
                                result[metric_type] = dm.fetcher.prs_fetcher.fetch(owner, repo)
                            elif metric_type == "downloads":
                                result[metric_type] = dm.fetcher.downloads_fetcher.fetch(owner, repo)
                            elif metric_type == "issues":
                                result[metric_type] = dm.fetcher.issues_fetcher.fetch(owner, repo)
                            elif metric_type == "contributions":
                                result[metric_type] = dm.fetcher.contributions_fetcher.fetch(owner, repo)
                    else:
                        # No GitHub token, use manual fetch
                        if metric_type == "stars":
                            result[metric_type] = dm.fetcher.stars_fetcher.fetch(owner, repo)
                        elif metric_type == "forks":
                            result[metric_type] = dm.fetcher.forks_fetcher.fetch(owner, repo)
                        elif metric_type == "prs":
                            result[metric_type] = dm.fetcher.prs_fetcher.fetch(owner, repo)
                        elif metric_type == "downloads":
                            result[metric_type] = dm.fetcher.downloads_fetcher.fetch(owner, repo)
                        elif metric_type == "issues":
                            result[metric_type] = dm.fetcher.issues_fetcher.fetch(owner, repo)
                        elif metric_type == "contributions":
                            result[metric_type] = dm.fetcher.contributions_fetcher.fetch(owner, repo)
        except Exception as e:
            # Show error message to user
            st.sidebar.error(f"❌ Error loading {metric_type}: {str(e)}")
            # Return empty DataFrame with correct schema
            if metric_type == "stars":
                result[metric_type] = pd.DataFrame(columns=["date", "stars"])
            elif metric_type == "forks":
                result[metric_type] = pd.DataFrame(columns=["date", "forks"])
            elif metric_type == "prs":
                result[metric_type] = pd.DataFrame(columns=["date", "pr_count"])
            elif metric_type == "downloads":
                result[metric_type] = pd.DataFrame(columns=["date", "downloads"])
            elif metric_type == "issues":
                result[metric_type] = pd.DataFrame(columns=["date", "issues"])
            elif metric_type == "contributions":
                result[metric_type] = pd.DataFrame(columns=["date", "commits"])
    
    return result

# ----------------------------
# GITHUB RELEASE DOWNLOAD FUNCTION
# ----------------------------
def to_plotly_xy(df: pd.DataFrame, x_col: str, y_col: str):
    if df.empty:
        return [], []
    return df[x_col].tolist(), df[y_col].tolist()

# ----------------------------
# GITHUB CONTRIBUTIONS FUNCTION
# ----------------------------
def summarize_total(df: pd.DataFrame, y_col: str, metric_type: str = None) -> int:
    if df.empty:
        return 0
    
    if metric_type in ["stars", "forks"]:
        # For stars and forks, we need to calculate the current total
        # Since we have daily events, we count all events up to the last date
        # This gives us the current total stars/forks
        return int(df[y_col].sum())
    else:
        # For PRs, issues, commits, downloads - sum the daily activity
        # This represents total activity in the selected period
        return int(df[y_col].sum())

def get_current_total(df: pd.DataFrame, y_col: str) -> int:
    """Get the current total for cumulative metrics like stars and forks."""
    if df.empty:
        return 0
    
    # For daily event data, sum all events to get current total
    return int(df[y_col].sum())

def get_period_activity(df: pd.DataFrame, y_col: str) -> int:
    """Get the total activity in the selected period for non-cumulative metrics."""
    if df.empty:
        return 0
    
    # Sum daily counts to get total activity in period
    return int(df[y_col].sum())

# ----------------------------
# GITHUB ISSUES FUNCTION
# ----------------------------
def filter_by_date(df: pd.DataFrame, start_d: date, end_d: date, date_col: str = "date") -> pd.DataFrame:
    if df.empty:
        return df
    # Convert date column to datetime if it's not already
    df_copy = df.copy()
    df_copy[date_col] = pd.to_datetime(df_copy[date_col], errors="coerce")
    df_copy = df_copy.dropna(subset=[date_col])
    
    # Convert start and end dates to datetime
    start_dt = pd.to_datetime(start_d)
    end_dt = pd.to_datetime(end_d)
    
    # Filter by date range
    mask = (df_copy[date_col] >= start_dt) & (df_copy[date_col] <= end_dt)
    filtered_df = df_copy.loc[mask]
    
    return filtered_df

# ----------------------------
# LOAD DATA
# ----------------------------
st.sidebar.markdown("---")

# GitHub API Configuration
github_token = os.getenv("GITHUB_TOKEN")

if github_token:
    st.sidebar.success("🔑 GitHub API authenticated")
    os.environ["GITHUB_TOKEN"] = github_token
else:
    st.sidebar.warning("⚠️ No GitHub token found. Set GITHUB_TOKEN environment variable for real data.")
    st.sidebar.info("💡 Create token at: GitHub.com → Settings → Developer settings → Personal access tokens")

use_graphql = st.sidebar.checkbox("Use GitHub GraphQL for stars/forks/PRs/issues", value=True)
os.environ["P16_USE_GRAPHQL"] = "1" if use_graphql else "0"



refresh = st.sidebar.button("🔄 Refresh Data")

# Add button to force fetch real data for downloads and commits
force_real_data = st.sidebar.button("🚀 Fetch Real Downloads & Commits")

# Initialize data_map
data_map = {}

if force_real_data:
    st.sidebar.info("🔄 Fetching real data from GitHub APIs...")
    try:
        dm = DataManager(data_dir="data", refresh_threshold_hours=24)
        real_data = dm.force_fetch_real_data(owner, repo, ["downloads", "contributions"])
        
        if real_data.get("downloads") is not None and not real_data["downloads"].empty:
            st.sidebar.success(f"✅ Downloads: {len(real_data['downloads'])} rows fetched")
        else:
            st.sidebar.warning("⚠️ Downloads: No data returned")
            
        if real_data.get("contributions") is not None and not real_data["contributions"].empty:
            st.sidebar.success(f"✅ Commits: {len(real_data['contributions'])} rows fetched")
        else:
            st.sidebar.warning("⚠️ Commits: No data returned")
            
        # Force refresh the data map to include new data
        data_map = get_data(owner, repo, force_refresh=True)
        
    except Exception as e:
        st.sidebar.error(f"❌ Error fetching real data: {str(e)}")
        # Fall back to normal data loading
        data_map = get_data(owner, repo, force_refresh=refresh)
else:
    # Normal data loading
    data_map = get_data(owner, repo, force_refresh=refresh)

# Fallback: if downloads are empty (common when no GitHub release assets),
# use PyPI metrics for the selected library/package if available
if data_map.get("downloads", pd.DataFrame()).empty:
    try:
        pypi_path = os.path.join("data", "pypi_metrics.csv")
        if os.path.exists(pypi_path):
            pypi_df = pd.read_csv(pypi_path)
            if {"date", "library", "downloads"}.issubset(set(pypi_df.columns)):
                pypi_df = pypi_df[pypi_df["library"] == repo][["date", "downloads"]].copy()
                pypi_df["date"] = pd.to_datetime(pypi_df["date"], errors="coerce")
                pypi_df = pypi_df.dropna(subset=["date"]).sort_values("date")
                data_map["downloads"] = pypi_df
    except Exception:
        pass
stars_df = data_map.get("stars", pd.DataFrame(columns=["date", "stars"]))
forks_df = data_map.get("forks", pd.DataFrame(columns=["date", "forks"]))
prs_df = data_map.get("prs", pd.DataFrame(columns=["date", "pr_count"]))
downloads_df = data_map.get("downloads", pd.DataFrame(columns=["date", "downloads"]))
issues_df = data_map.get("issues", pd.DataFrame(columns=["date", "issues"]))
contribs_df = data_map.get("contributions", pd.DataFrame(columns=["date", "commits"]))

# ----------------------------
# FILTERING AND DISPLAY (always reflect selected date range)
# ----------------------------
from_date = start_date
to_date = end_date

filtered_stars = filter_by_date(stars_df, from_date, to_date)
filtered_forks = filter_by_date(forks_df, from_date, to_date)
filtered_prs = filter_by_date(prs_df, from_date, to_date)
filtered_downloads = filter_by_date(downloads_df, from_date, to_date)
filtered_issues = filter_by_date(issues_df, from_date, to_date)
filtered_contribs = filter_by_date(contribs_df, from_date, to_date)

st.subheader(f"📊 Summary for {owner}/{repo}")

# Explanation of what each metric shows
# st.info("""
# **Summary Metrics Explanation:**
# - **All metrics show activity in your selected date range only**
# - **Stars/Forks**: Daily star/fork events in the selected period
# - **PRs/Issues/Downloads/Commits**: Daily activity in the selected period
# - **Graphs**: Show the same data over time
# """)

# Data validation - check for cumulative vs daily data issues
def validate_data_quality(df: pd.DataFrame, metric_name: str) -> bool:
    """Check if data appears to be daily counts (reasonable) vs cumulative (unreasonable)."""
    if df.empty:
        return True
    
    # Get the value column (exclude date column)
    value_cols = [col for col in df.columns if col != 'date']
    if not value_cols:
        return True
    
    value_col = value_cols[0]
    max_value = df[value_col].max()
    
    # If max value is extremely large, it might be cumulative
    if max_value > 1000000:  # More than 1 million seems unreasonable for daily counts
        # st.warning(f"⚠️ {metric_name}: Unusually large values detected (max: {max_value:,}). This might indicate cumulative data instead of daily counts.")
        return False
    
    # If max value is reasonable for daily counts, data looks good
    if max_value <= 10000:  # Daily counts should typically be under 10k
        return True
    
    # Moderate values - don't show info
    # st.info(f"ℹ️ {metric_name}: Values range from {df[value_col].min():,} to {max_value:,}")
    return True

# Validate each dataset
validate_data_quality(filtered_stars, "Stars")
validate_data_quality(filtered_forks, "Forks")
validate_data_quality(filtered_prs, "Pull Requests")
validate_data_quality(filtered_downloads, "Downloads")
validate_data_quality(filtered_issues, "Issues")
validate_data_quality(filtered_contribs, "Commits")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Stars in Period", get_period_activity(filtered_stars, "stars"))
col2.metric("Forks in Period", get_period_activity(filtered_forks, "forks"))
col3.metric("PRs in Period", get_period_activity(filtered_prs, "pr_count"))
col4.metric("Downloads in Period", get_period_activity(filtered_downloads, "downloads"))

col5, col6 = st.columns(2)
with col5:
    st.metric("Issues in Period", get_period_activity(filtered_issues, "issues"))
with col6:
    st.metric("Commits in Period", get_period_activity(filtered_contribs, "commits"))

col1, col2 = st.columns(2)
with col1:
    if filtered_stars.empty:
        st.info("No stars data in selected range.")
    else:
        x, y = to_plotly_xy(filtered_stars, "date", "stars")
        fig_stars = px.line(x=x, y=y, title=" Stars Over Time",
                            markers=True, template="plotly_white", color_discrete_sequence=["#FFD700"])
        st.plotly_chart(fig_stars, use_container_width=True)

with col2:
    if filtered_forks.empty:
        st.info("No forks data in selected range.")
    else:
        x, y = to_plotly_xy(filtered_forks, "date", "forks")
        fig_forks = px.line(x=x, y=y, title=" Forks Over Time",
                            markers=True, template="plotly_white", color_discrete_sequence=["#1f77b4"])
        st.plotly_chart(fig_forks, use_container_width=True)

col3, col4 = st.columns(2)
with col3:
    if filtered_prs.empty:
        st.info("No PR data in selected range.")
    else:
        x, y = to_plotly_xy(filtered_prs, "date", "pr_count")
        fig_prs = px.line(x=x, y=y, title=" Pull Requests Over Time",
                          markers=True, template="plotly_white", color_discrete_sequence=["#FF7F0E"])
        st.plotly_chart(fig_prs, use_container_width=True)

with col4:
    if filtered_downloads.empty:
        st.info("No downloads data in selected range.")
    else:
        x, y = to_plotly_xy(filtered_downloads, "date", "downloads")
        fig_downloads = px.line(x=x, y=y, title=" Downloads Over Time",
                                markers=True, template="plotly_white", color_discrete_sequence=["#2E8B57"])
        st.plotly_chart(fig_downloads, use_container_width=True)

col7, col8 = st.columns(2)
with col7:
    if filtered_issues.empty:
        st.info("No issues data in selected range.")
    else:
        x, y = to_plotly_xy(filtered_issues, "date", "issues")
        fig_issues = px.line(x=x, y=y, title=" Issues Opened Over Time",
                             markers=True, template="plotly_white", color_discrete_sequence=["#8A2BE2"])
        st.plotly_chart(fig_issues, use_container_width=True)

with col8:
    if filtered_contribs.empty:
        st.info("No commits data in selected range.")
    else:
        x, y = to_plotly_xy(filtered_contribs, "date", "commits")
        fig_commits = px.line(x=x, y=y, title=" Commits Over Time",
                              markers=True, template="plotly_white", color_discrete_sequence=["#2ca02c"])
        st.plotly_chart(fig_commits, use_container_width=True)

# DOWNLOAD BUTTON (single, merged by date and filtered by selection)
st.markdown("###  Download Filtered Data")

# Ensure all date columns are datetime before merging
def ensure_datetime(df, col_name="date"):
    if not df.empty and col_name in df.columns:
        # Create a copy to avoid SettingWithCopyWarning
        df_copy = df.copy()
        df_copy[col_name] = pd.to_datetime(df_copy[col_name], errors="coerce")
        df_copy = df_copy.dropna(subset=[col_name])
        return df_copy
    return df

# Convert all filtered DataFrames to have consistent datetime columns
filtered_stars = ensure_datetime(filtered_stars)
filtered_forks = ensure_datetime(filtered_forks)
filtered_prs = ensure_datetime(filtered_prs)
filtered_downloads = ensure_datetime(filtered_downloads)
filtered_issues = ensure_datetime(filtered_issues)
filtered_contribs = ensure_datetime(filtered_contribs)

try:
    # Merge all DataFrames on date column
    merged = filtered_stars.merge(filtered_forks, on="date", how="outer")
    merged = merged.merge(filtered_prs, on="date", how="outer")
    merged = merged.merge(filtered_downloads, on="date", how="outer")
    merged = merged.merge(filtered_issues, on="date", how="outer")
    merged = merged.merge(filtered_contribs, on="date", how="outer")
    
    # Sort by date and fill missing values
    merged = merged.sort_values("date").fillna(0)
    
    # Download button
    st.download_button(
        "⬇️ Download CSV (Selected Range)",
        merged.to_csv(index=False),
        file_name=f"{owner}_{repo}_metrics_{start_date}_to_{end_date}.csv",
    )
    
except Exception as e:
    st.error(f"❌ Error creating merged CSV: {str(e)}")
    st.info("💡 Try refreshing the data first to ensure all metrics are loaded properly.")

# Display data status for debugging
st.sidebar.markdown("---")
st.sidebar.markdown("**🔍 Data Status:**")
st.sidebar.markdown(f"Stars: {len(stars_df)} rows")
st.sidebar.markdown(f"Forks: {len(forks_df)} rows")
st.sidebar.markdown(f"PRs: {len(prs_df)} rows")
st.sidebar.markdown(f"Downloads: {len(downloads_df)} rows")
st.sidebar.markdown(f"Issues: {len(issues_df)} rows")
st.sidebar.markdown(f"Commits: {len(contribs_df)} rows")

# Show sample data for debugging
if not stars_df.empty:
    st.sidebar.markdown(f"**Sample Stars Data:**")
    st.sidebar.markdown(f"First row: {stars_df.iloc[0].to_dict()}")
    st.sidebar.markdown(f"Last row: {stars_df.iloc[-1].to_dict()}")
    st.sidebar.markdown(f"Total stars: {stars_df['stars'].sum()}")

# Extra sections remain removed for simplicity
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
