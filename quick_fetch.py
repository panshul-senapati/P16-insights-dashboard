#!/usr/bin/env python3
"""
Quick GitHub Data Fetcher
Fetches current real GitHub data and creates realistic historical data for the dashboard.
"""

import os
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time


def fetch_current_repo_stats(owner, repo):
    """Fetch current repository statistics."""
    github_token = os.getenv("GITHUB_TOKEN", None)
    headers = {"Authorization": f"token {github_token}"} if github_token else {}
    
    url = f"https://api.github.com/repos/{owner}/{repo}"
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        print(f"❌ Failed to fetch {owner}/{repo}: {response.status_code}")
        return None
    
    data = response.json()
    return {
        'name': data['name'],
        'stars': data['stargazers_count'],
        'forks': data['forks_count'],
        'open_issues': data['open_issues_count'],
        'created_at': data['created_at']
    }


def generate_realistic_history(current_value, days_back=365, growth_type='exponential'):
    """Generate realistic historical data based on current values."""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    
    # Create realistic growth curve
    if growth_type == 'exponential':
        # Most GitHub repos have exponential-like growth
        x = np.linspace(0, 1, len(dates))
        # Exponential curve: starts slow, grows faster
        growth_curve = np.exp(x * 3) - 1  # Exponential growth
        growth_curve = growth_curve / growth_curve[-1]  # Normalize to 0-1
    else:  # linear
        growth_curve = np.linspace(0, 1, len(dates))
    
    # Add some realistic noise
    noise = np.random.normal(0, 0.05, len(dates))
    growth_curve = np.maximum(0, growth_curve + noise)
    
    # Scale to current value
    values = (growth_curve * current_value).astype(int)
    values[-1] = current_value  # Ensure last value matches current
    
    return dates, values


def create_historical_data():
    """Create historical data for all repositories."""
    
    repos = {
        "pandas": ("pandas-dev", "pandas"),
        "scikit-learn": ("scikit-learn", "scikit-learn"),
        "matplotlib": ("matplotlib", "matplotlib"),
        "tensorflow": ("tensorflow", "tensorflow"),
    }
    
    print("🔍 Fetching current repository statistics...")
    
    # Fetch current stats for all repos
    repo_stats = {}
    for name, (owner, repo) in repos.items():
        print(f"   📊 {name}...")
        stats = fetch_current_repo_stats(owner, repo)
        if stats:
            repo_stats[name] = stats
        time.sleep(1)  # Be nice to API
    
    if not repo_stats:
        print("❌ No repository data fetched!")
        return
    
    print(f"✅ Fetched data for {len(repo_stats)} repositories")
    
    # Combine all repo stats
    total_stars = sum(stats['stars'] for stats in repo_stats.values())
    total_forks = sum(stats['forks'] for stats in repo_stats.values())
    avg_issues = int(np.mean([stats['open_issues'] for stats in repo_stats.values()]))
    
    print(f"📈 Combined Stats:")
    print(f"   ⭐ Total Stars: {total_stars:,}")
    print(f"   🍴 Total Forks: {total_forks:,}")
    print(f"   🔍 Avg Open Issues: {avg_issues:,}")
    
    # Generate historical data
    print("\n📊 Generating historical data...")
    
    os.makedirs('data', exist_ok=True)
    
    # Stars history (365 days)
    dates, star_values = generate_realistic_history(total_stars, 365, 'exponential')
    stars_df = pd.DataFrame({
        'date': dates.strftime('%Y-%m-%d'),
        'stars': star_values
    })
    stars_df.to_csv('data/github_stars.csv', index=False)
    print(f"   ⭐ Stars: {len(stars_df)} data points")
    
    # Forks history (365 days) 
    dates, fork_values = generate_realistic_history(total_forks, 365, 'exponential')
    forks_df = pd.DataFrame({
        'date': dates.strftime('%Y-%m-%d'),
        'forks': fork_values
    })
    forks_df.to_csv('data/github_forks.csv', index=False)
    print(f"   🍴 Forks: {len(forks_df)} data points")
    
    # Pull requests history (180 days, more volatile)
    dates_pr, pr_values = generate_realistic_history(avg_issues * 2, 180, 'linear')
    # Make PR data more volatile (daily fluctuations)
    pr_values = np.maximum(0, pr_values + np.random.normal(0, 5, len(pr_values)))
    prs_df = pd.DataFrame({
        'date': dates_pr.strftime('%Y-%m-%d'),
        'pr_count': pr_values.astype(int)
    })
    prs_df.to_csv('data/github_pull_requests.csv', index=False)
    print(f"   🔄 Pull Requests: {len(prs_df)} data points")
    
    # Downloads history (90 days, high volatility)
    dates_dl, download_values = generate_realistic_history(50000, 90, 'linear')
    # Add weekly patterns (higher on weekdays)
    weekly_pattern = np.array([1.2, 1.1, 1.0, 1.0, 0.9, 0.7, 0.6])  # Mon-Sun multipliers
    for i in range(len(download_values)):
        day_of_week = dates_dl[i].weekday()
        download_values[i] = int(download_values[i] * weekly_pattern[day_of_week])
    
    downloads_df = pd.DataFrame({
        'date': dates_dl.strftime('%Y-%m-%d'),
        'downloads': download_values
    })
    downloads_df.to_csv('data/github_downloads.csv', index=False)
    print(f"   📥 Downloads: {len(downloads_df)} data points")
    
    print("\n✅ All CSV files created successfully!")
    print("\n📋 Files created:")
    print("   📁 data/github_stars.csv")
    print("   📁 data/github_forks.csv") 
    print("   📁 data/github_pull_requests.csv")
    print("   📁 data/github_downloads.csv")
    
    # Show sample data
    print(f"\n📊 Sample data preview:")
    print(f"Stars (last 5 days):")
    print(stars_df.tail().to_string(index=False))


if __name__ == "__main__":
    print("🚀 Quick GitHub Data Fetcher")
    print("=" * 50)
    create_historical_data()
    print("\n🎉 Ready! Run 'streamlit run app.py' to see your dashboard!")
