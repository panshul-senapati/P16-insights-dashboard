"""
GitHub Insights Dashboard - Streamlit App
Uses DataManager for clean separation of data fetching and loading logic
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import date, timedelta

from data_manager import DataManager


# ----------------------------
# PAGE CONFIGURATION
# ----------------------------
st.set_page_config(page_title="📊 GitHub Insights Dashboard", layout="wide")
st.title("📊 GitHub Insights Dashboard")

# ----------------------------
# SIDEBAR CONFIGURATION
# ----------------------------
st.sidebar.header("📁 Library Info & Filter")

# Repository mapping
REPO_MAP = {
    "Skrub": ("skrub-data", "skrub"),
    "tslearn": ("tslearn-team", "tslearn"),
    "scikit-learn": ("scikit-learn", "scikit-learn"),
    "Aeon": ("aeon-toolkit", "aeon"),
    "Mapie": ("scikit-learn-contrib", "mapie"),
}

# Library selection
selected_lib = st.sidebar.selectbox("🔍 Select Library:", list(REPO_MAP.keys()))
owner, repo = REPO_MAP[selected_lib]

# Date range selection
today = date.today()
default_start = today - timedelta(days=180)

start_date = st.sidebar.date_input(
    "📅 Select Start Date:", 
    default_start, 
    min_value=date(2010, 1, 1), 
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
    """Load all cached data through data manager."""
    return data_manager.get_all_cached_data(owner, repo, force_refresh)

@st.cache_data(ttl=1800)  # Cache for 30 minutes
def load_real_time_data(owner: str, repo: str):
    """Load real-time data through data manager."""
    return data_manager.get_real_time_data(owner, repo)

def ensure_datetime_column(df: pd.DataFrame, date_col: str = "date") -> pd.DataFrame:
    """Ensure date column is datetime type."""
    if date_col in df.columns:
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    return df

def filter_by_date_range(df: pd.DataFrame, start_date: date, end_date: date, date_col: str = "date") -> pd.DataFrame:
    """Filter DataFrame by date range."""
    from_date = pd.to_datetime(start_date)
    to_date = pd.to_datetime(end_date)
    
    return df[(df[date_col] >= from_date) & (df[date_col] <= to_date)]

# ----------------------------
# MAIN DATA LOADING
# ----------------------------
with st.spinner("Loading data..."):
    # Load cached data (stars, forks, PRs, downloads)
    cached_data = load_cached_data(owner, repo, force_refresh)
    
    # Ensure datetime columns
    for key, df in cached_data.items():
        cached_data[key] = ensure_datetime_column(df)
    
    # Load real-time data (contributions, issues, dependents)
    real_time_data = load_real_time_data(owner, repo)

# ----------------------------
# DATA VALIDATION AND FILTERING
# ----------------------------
required_data = ['stars', 'forks', 'prs', 'downloads']
missing_data = [key for key in required_data if cached_data[key].empty]

if missing_data:
    st.error(f"❌ Missing or invalid data for: {', '.join(missing_data)}")
    st.info("💡 Try clicking 'Force Refresh All Data' to fetch new data.")
    st.stop()

# Filter data by date range
filtered_data = {}
for key, df in cached_data.items():
    if not df.empty:
        filtered_data[key] = filter_by_date_range(df, start_date, end_date)

# ----------------------------
# SUMMARY METRICS
# ----------------------------
st.subheader(f"📊 Summary for {owner}/{repo}")
st.caption(f"Data from {start_date} to {end_date}")

col1, col2, col3, col4 = st.columns(4)

with col1:
    total_stars = int(filtered_data["stars"]["stars"].sum()) if not filtered_data["stars"].empty else 0
    st.metric("⭐ Total Stars", f"{total_stars:,}")

with col2:
    total_forks = int(filtered_data["forks"]["forks"].sum()) if not filtered_data["forks"].empty else 0
    st.metric("🍴 Total Forks", f"{total_forks:,}")

with col3:
    total_prs = int(filtered_data["prs"]["pr_count"].sum()) if not filtered_data["prs"].empty else 0
    st.metric("🔄 Total PRs", f"{total_prs:,}")

with col4:
    total_downloads = int(filtered_data["downloads"]["downloads"].sum()) if not filtered_data["downloads"].empty else 0
    st.metric("⬇️ Total Downloads", f"{total_downloads:,}")

# ----------------------------
# CHARTS SECTION
# ----------------------------
st.markdown("---")
st.subheader("📈 Trends Over Time")

# Chart creation function
def create_line_chart(df, x_col, y_col, title, color, y_label=None):
    """Create a standardized line chart."""
    if df.empty:
        return None
    
    fig = px.line(
        df,
        x=x_col,
        y=y_col,
        title=title,
        markers=True,
        template="plotly_white",
        color_discrete_sequence=[color],
        labels={y_col: y_label or y_col.replace('_', ' ').title()}
    )
    fig.update_layout(
        xaxis_title="Date",
        hovermode='x unified'
    )
    return fig

# Row 1: Stars and Forks
col1, col2 = st.columns(2)

with col1:
    fig_stars = create_line_chart(
        filtered_data["stars"], "date", "stars", 
        "⭐ Stars Over Time", "#FFD700", "Stars"
    )
    if fig_stars:
        st.plotly_chart(fig_stars, use_container_width=True)

with col2:
    fig_forks = create_line_chart(
        filtered_data["forks"], "date", "forks", 
        "🍴 Forks Over Time", "#1f77b4", "Forks"
    )
    if fig_forks:
        st.plotly_chart(fig_forks, use_container_width=True)

# Row 2: PRs and Downloads
col3, col4 = st.columns(2)

with col3:
    fig_prs = create_line_chart(
        filtered_data["prs"], "date", "pr_count", 
        "🔄 Pull Requests Over Time", "#FF7F0E", "Pull Requests"
    )
    if fig_prs:
        st.plotly_chart(fig_prs, use_container_width=True)

with col4:
    fig_downloads = create_line_chart(
        filtered_data["downloads"], "date", "downloads", 
        "⬇️ Downloads Over Time", "#9467bd", "Downloads"
    )
    if fig_downloads:
        st.plotly_chart(fig_downloads, use_container_width=True)

# ----------------------------
# REAL-TIME DATA CHARTS
# ----------------------------
st.markdown("---")
st.subheader("🔄 Real-time Activity")

# Contributions Chart
contributions_df = real_time_data.get('contributions', pd.DataFrame())
if not contributions_df.empty:
    contributions_df = ensure_datetime_column(contributions_df, "week")
    filtered_contributions = filter_by_date_range(contributions_df, start_date, end_date, "week")
    
    if not filtered_contributions.empty:
        contrib_fig = create_line_chart(
            filtered_contributions, "week", "commits",
            "💻 Weekly Contributions (Commits)", "#2ca02c", "Commits"
        )
        if contrib_fig:
            st.plotly_chart(contrib_fig, use_container_width=True)
    else:
        st.info("ℹ️ No contribution data found in selected date range.")
else:
    st.info("ℹ️ No contribution data available.")

# Issues Chart
issues_df = real_time_data.get('issues', pd.DataFrame())
if not issues_df.empty:
    issues_df = ensure_datetime_column(issues_df)
    filtered_issues = filter_by_date_range(issues_df, start_date, end_date)
    
    if not filtered_issues.empty:
        issues_fig = create_line_chart(
            filtered_issues, "date", "issue_count",
            "🐛 Issues Over Time", "#d62728", "Issues"
        )
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

dependents_df = real_time_data.get('dependents', pd.DataFrame())

if not dependents_df.empty:
    # Sort by stars
    df_sorted = dependents_df.sort_values(by="stars", ascending=False)
    
    # Create star range categories
    star_ranges = {
        "Below 100 stars": (df_sorted["stars"] < 100).sum(),
        "100 to 1000 stars": ((df_sorted["stars"] >= 100) & (df_sorted["stars"] <= 1000)).sum(),
        "1000+ stars": (df_sorted["stars"] > 1000).sum(),
    }
    
    # Display dependents table
    st.dataframe(df_sorted, use_container_width=True)
    
    # Create bar chart for star ranges
    if any(star_ranges.values()):
        star_range_df = pd.DataFrame(
            list(star_ranges.items()), 
            columns=["Star Range", "Library Count"]
        )
        
        fig_dep = px.bar(
            star_range_df,
            x="Star Range",
            y="Library Count",
            title="📊 Dependents by Star Range",
            template="plotly_white",
            color_discrete_sequence=["#17becf"]
        )
        st.plotly_chart(fig_dep, use_container_width=True)
else:
    st.info("ℹ️ No public dependents found.")

# ----------------------------
# DATA EXPORT SECTION
# ----------------------------
st.markdown("---")
st.subheader("📥 Download Filtered Data")

download_cols = st.columns(4)
download_data = [
    ("⬇️ Stars CSV", "filtered_stars.csv", filtered_data["stars"]),
    ("⬇️ Forks CSV", "filtered_forks.csv", filtered_data["forks"]),
    ("⬇️ PRs CSV", "filtered_prs.csv", filtered_data["prs"]),
    ("⬇️ Downloads CSV", "filtered_downloads.csv", filtered_data["downloads"]),
]

for i, (button_text, filename, df) in enumerate(download_data):
    with download_cols[i]:
        if not df.empty:
            st.download_button(
                button_text,
                df.to_csv(index=False),
                file_name=filename,
                mime="text/csv"
            )
        else:
            st.button(button_text, disabled=True)

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
    footer {visibility: hidden;}
    header {visibility: hidden;}
    </style>
"""
st.markdown(hide_st_style, unsafe_allow_html=True)
