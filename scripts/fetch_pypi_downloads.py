import requests
import pandas as pd

def fetch_pypi_downloads(package_name):
    url = f"https://pypistats.org/api/packages/{package_name}/recent"
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Error fetching PyPI downloads: {response.status_code}")
        return None
    
    data = response.json().get('data', {})
    
    # If the data contains aggregates (last_day, last_week, last_month)
    if all(key in data for key in ['last_day', 'last_week', 'last_month']):
        print(f"⚠️ Aggregate downloads data for {package_name}: {data}")
        # Return a DataFrame with these aggregates to display simple bars
        df = pd.DataFrame([
            {"period": "Last Day", "downloads": data['last_day']},
            {"period": "Last Week", "downloads": data['last_week']},
            {"period": "Last Month", "downloads": data['last_month']}
        ])
        return df
    
    # Otherwise, if detailed daily downloads data exists, parse it here
    # (Unlikely with the current API)
    downloads = data.get('downloads', [])
    if not downloads:
        print(f"⚠️ No detailed downloads data found for {package_name}")
        return None
    
    df = pd.DataFrame(downloads)
    df['date'] = pd.to_datetime(df['date'])
    return df
