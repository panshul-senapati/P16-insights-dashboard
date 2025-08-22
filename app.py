"""
GitHub Insights Dashboard - Streamlit App
Uses DataManager for clean separation of data fetching and loading logic
"""

# ----------------------------
# ENVIRONMENT SETUP
# ----------------------------
from dotenv import load_dotenv
import os

# Load environment variables from .env
load_dotenv()
token = os.getenv("GITHUB_TOKEN")
if not token:
    raise ValueError("⚠️ GitHub token not found in .env! Please add GITHUB_TOKEN=<your_token>")

# ----------------------------
# IMPORTS
# ----------------------------
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date, timedelta

from data_manager import DataManager
from config import REPO_MAP as CONFIG_REPO_MAP

# ----------------------------
# PAGE CONFIGURATION
# ----------------------------
st.set_page_config(page_title="📊 GitHub Insights Dashboard", layout="wide")
st.title("📊 GitHub Insights Dashboard")

# ----------------------------
# SIDEBAR CONFIGURATION
# ----------------------------
st.sidebar.header("📁 Library Info & Filter")

# Repository mapping (Skrub and Scikit-learn)
REPO_MAP = CONFIG_REPO_MAP

# Library selection
selected_lib = st.sidebar.selectbox("🔍 Select Library:", list(REPO_MAP.keys()))
owner, repo = REPO_MAP[selected_lib]

# Date range selection
today = date.today()
default_start = today - timedelta(days=180)

start_date = st.sidebar.date_input(
    "📅 Select Start Date:", 
    default_start,
    min_value=date(2011, 1, 1),
    max_value=today
)
end_date = st.sidebar.date_input(
    "📅 Select End Date:", 
    today, 
    min_value=start_date, 
    max_value=today
)

# Data refresh options
st.sidebar.markdown("---")
st.sidebar.subheader("🔄 Data Management")

force_refresh = st.sidebar.button("🔄 Force Refresh All Data")
if st.sidebar.button("📊 Show Data Status"):
    st.session_state.show_status = True

# ----------------------------
# INITIALIZE DATA MANAGER
# ----------------------------
@st.cache_resource
def get_data_manager():
    """Initialize and cache the data manager."""
    return DataManager(data_dir="data", refresh_threshold_hours=24)

data_manager = get_data_manager()

# ----------------------------
# DATA STATUS DISPLAY (Optional)
# ----------------------------
if st.session_state.get('show_status', False):
    st.sidebar.markdown("### 📋 Data Status")
    status = data_manager.get_data_status()
    
    for data_type, info in status.items():
        status_icon = "🟢" if not info['is_stale'] else "🔴" if not info['exists'] else "🟡"
        st.sidebar.text(f"{status_icon} {data_type.title()}: {info['last_updated']}")

# ----------------------------
# UTILITY FUNCTIONS
# ----------------------------
@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_cached_data(owner: str, repo: str, force_refresh: bool = False):
    """Load all cached data (Polars) through data manager. On first run, force full backfill."""
    return data_manager.get_all_cached_data(owner, repo, force_refresh)

@st.cache_data(ttl=1800)  # Cache for 30 minutes
def load_real_time_data(owner: str, repo: str):
    """Load real-time data through data manager (Polars)."""
    return data_manager.get_real_time_data(owner, repo)

def ensure_datetime_column(df: pd.DataFrame, date_col: str = "date") -> pd.DataFrame:
    """Ensure date column is datetime type."""
    if date_col in df.columns:
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    return df

def filter_by_date_range(df: pd.DataFrame, start_date: date, end_date: date, date_col: str = "date") -> pd.DataFrame:
    """Filter DataFrame by date range (handles timezone-aware datetimes)."""
    if df.empty or date_col not in df.columns:
        return pd.DataFrame()
    
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    
    # Convert start/end to match column type
    if pd.api.types.is_datetime64_any_dtype(df[date_col]):
        if df[date_col].dt.tz is not None:
            start_ts = pd.Timestamp(start_date).tz_localize("UTC")
            end_ts = pd.Timestamp(end_date).tz_localize("UTC")
        else:
            start_ts = pd.Timestamp(start_date)
            end_ts = pd.Timestamp(end_date)
    else:
        start_ts = pd.Timestamp(start_date)
        end_ts = pd.Timestamp(end_date)
    
    return df[(df[date_col] >= start_ts) & (df[date_col] <= end_ts)]

# ----------------------------
# MAIN DATA LOADING
# ----------------------------
with st.spinner("Loading data..."):
    # Ensure full history exists on first run
    ranged_pl = data_manager.get_range_data(owner, repo, start_date, end_date, force_refresh=False)
    cached_pl = ranged_pl
    # Load real-time data (Polars)
    real_time_pl = load_real_time_data(owner, repo)


# ----------------------------
# DATA VALIDATION AND FILTERING
# ----------------------------
required_data = ['stars', 'forks', 'prs', 'downloads']
missing_data = [key for key in required_data if (cached_pl.get(key) is None or len(cached_pl.get(key)) == 0)]

if missing_data:
    st.error(f"❌ Missing or invalid data for: {', '.join(missing_data)}")
    st.info("💡 Try clicking 'Force Refresh All Data' to fetch new data.")
    st.stop()


# Filter data by date range
filtered_data = {}
for key, dfpl in cached_pl.items():
    if dfpl is not None and len(dfpl) > 0:
        filtered_data[key] = dfpl
    else:
        filtered_data[key] = None


# ----------------------------
# SUMMARY METRICS
# ----------------------------
st.subheader(f"📊 Summary for {owner}/{repo}")
st.caption(f"Data from {start_date} to {end_date}")

col1, col2, col3, col4 = st.columns(4)

with col1:
    total_stars = int(filtered_data["stars"]["count"].sum()) if filtered_data["stars"] is not None and len(filtered_data["stars"]) else 0
    st.metric("⭐ Total Stars", f"{total_stars:,}")

with col2:
    total_forks = int(filtered_data["forks"]["count"].sum()) if filtered_data["forks"] is not None and len(filtered_data["forks"]) else 0
    st.metric("🍴 Total Forks", f"{total_forks:,}")

with col3:
    total_prs = int(filtered_data["prs"]["count"].sum()) if filtered_data["prs"] is not None and len(filtered_data["prs"]) else 0
    st.metric("🔄 Total PRs", f"{total_prs:,}")

with col4:
    total_downloads = int(filtered_data["downloads"]["count"].sum()) if filtered_data["downloads"] is not None and len(filtered_data["downloads"]) else 0
    st.metric("⬇️ Total Downloads", f"{total_downloads:,}")

# ----------------------------
# CHARTS SECTION
# ----------------------------
st.markdown("---")
st.subheader("📈 Trends Over Time")

def create_line_chart(dfpl, x_col, y_col, title, color, y_label=None):
    """Create a standardized line chart."""
    if dfpl is None or len(dfpl) == 0:
        return None
    dfpl = dfpl.sort("date")
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=dfpl[x_col].to_list(),
        y=dfpl[y_col].to_list(),
        mode="lines+markers",
        name=y_label or y_col.replace('_', ' ').title(),
        line=dict(color=color)
    ))
    fig.update_layout(title=title, template="plotly_white", xaxis_title="Date", hovermode='x unified')
    return fig


# Stars and Forks
col1, col2 = st.columns(2)
with col1:
    fig_stars = create_line_chart(filtered_data["stars"], "date", "count", "⭐ Stars Over Time", "#FFD700", "Stars")
    if fig_stars:
        st.plotly_chart(fig_stars, use_container_width=True)
with col2:
    fig_forks = create_line_chart(filtered_data["forks"], "date", "count", "🍴 Forks Over Time", "#1f77b4", "Forks")
    if fig_forks:
        st.plotly_chart(fig_forks, use_container_width=True)

# PRs and Downloads
col3, col4 = st.columns(2)
with col3:
    fig_prs = create_line_chart(filtered_data["prs"], "date", "count", "🔄 Pull Requests Over Time", "#FF7F0E", "Pull Requests")
    if fig_prs:
        st.plotly_chart(fig_prs, use_container_width=True)
with col4:
    fig_downloads = create_line_chart(filtered_data["downloads"], "date", "count", "⬇️ Downloads Over Time", "#9467bd", "Downloads")
    if fig_downloads:
        st.plotly_chart(fig_downloads, use_container_width=True)

# ----------------------------
# REAL-TIME DATA CHARTS
# ----------------------------
st.markdown("---")
st.subheader("🔄 Real-time Activity")

# Contributions
contributions_pl = real_time_pl.get('contributions')
if contributions_pl is not None and len(contributions_pl) > 0:
    contrib_fig = create_line_chart(contributions_pl, "week", "commits", "💻 Weekly Contributions (Commits)", "#2ca02c", "Commits")
    if contrib_fig:
        st.plotly_chart(contrib_fig, use_container_width=True)
    else:
        st.info("ℹ️ No contribution data found in selected date range.")
else:
    st.info("ℹ️ No contribution data available.")

# Issues
issues_pl = real_time_pl.get('issues')
if issues_pl is not None and len(issues_pl) > 0:
    issues_fig = create_line_chart(issues_pl, "date", "issue_count", "🐛 Issues Over Time", "#d62728", "Issues")
    if issues_fig:
        st.plotly_chart(issues_fig, use_container_width=True)
    else:
        st.info("ℹ️ No issues data found in selected date range.")
else:
    st.info("ℹ️ No issues data available.")

# ----------------------------
# DEPENDENTS SECTION
# ----------------------------
st.markdown("---")
st.subheader("🔗 Public GitHub Dependents")

dependents_pl = real_time_pl.get('dependents')
if dependents_pl is not None and len(dependents_pl) > 0:
    # Convert to table directly from Polars
    st.dataframe(dependents_pl.sort("stars", descending=True).to_pandas(), use_container_width=True)
else:
    st.info("ℹ️ No public dependents found.")

# ----------------------------
# DATA EXPORT SECTION
# ----------------------------
st.markdown("---")
st.subheader("📥 Download Filtered Data")

import io
import polars as pl

# Build a single combined CSV for selected dates with all metrics
metric_frames = {
    "stars": filtered_data.get("stars"),
    "forks": filtered_data.get("forks"),
    "prs": filtered_data.get("prs"),
    "downloads": filtered_data.get("downloads"),
}

# Base date frame from selected start/end to avoid duplicate key columns
base_dates = pl.date_range(start_date, end_date, interval="1d", eager=True).alias("date")
combined_pl = pl.DataFrame({"date": base_dates})

for name, dfpl in metric_frames.items():
    if dfpl is None or len(dfpl) == 0:
        continue
    # Ensure proper schema and aggregate by date
    tmp = (
        dfpl.with_columns(pl.col("date").cast(pl.Date, strict=False))
            .group_by("date")
            .agg(pl.col("count").sum().alias(name))
            .select(["date", name])
    )
    combined_pl = combined_pl.join(tmp, on="date", how="left")

# Fill missing metric values with 0 and sort
fill_cols = [c for c in combined_pl.columns if c != "date"]
if fill_cols:
    combined_pl = combined_pl.sort("date").with_columns([pl.col(fill_cols).fill_null(0)])

if len(combined_pl) > 0:
    buf = io.StringIO()
    combined_pl.write_csv(buf)
    csv_data = buf.getvalue()
    fname = f"{repo}_metrics_{start_date}_{end_date}.csv"
    st.download_button("⬇️ Download Combined CSV", data=csv_data, file_name=fname, mime="text/csv")
else:
    st.info("No data available for the selected date range.")

# ----------------------------
# FOOTER
# ----------------------------
st.markdown("---")
st.caption(f"Last updated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')} | Repository: {owner}/{repo}")

# ----------------------------
# HIDE STREAMLIT DEFAULT UI
# ----------------------------
hide_st_style = """
    <style>
    #MainMenu {visibility: hidden;}
    </style>
    """