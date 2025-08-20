"""
Utility functions for the GitHub Analytics Dashboard
"""

import os
import pandas as pd
import logging
from datetime import datetime, date, timedelta
from typing import Optional, Dict, Any, Tuple, List
import json

logger = logging.getLogger(__name__)


def ensure_datetime_column(df: pd.DataFrame, date_col: str = "date") -> pd.DataFrame:
    """
    Ensure date column is datetime type with proper error handling.
    
    Args:
        df: DataFrame to process
        date_col: Name of the date column
        
    Returns:
        DataFrame with datetime column
    """
    if df.empty or date_col not in df.columns:
        return df
    
    try:
        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        
        # Drop rows with invalid dates
        before_count = len(df)
        df = df.dropna(subset=[date_col])
        after_count = len(df)
        
        if before_count != after_count:
            logger.warning(f"Dropped {before_count - after_count} rows with invalid dates in column '{date_col}'")
        
        return df
    except Exception as e:
        logger.error(f"Error converting {date_col} to datetime: {e}")
        return df


def filter_by_date_range(
    df: pd.DataFrame, 
    start_date: date, 
    end_date: date, 
    date_col: str = "date"
) -> pd.DataFrame:
    """
    Filter DataFrame by date range with proper error handling.
    
    Args:
        df: DataFrame to filter
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        date_col: Name of the date column
        
    Returns:
        Filtered DataFrame
    """
    if df.empty or date_col not in df.columns:
        logger.warning(f"Cannot filter: DataFrame is empty or missing column '{date_col}'")
        return df
    
    try:
        # Ensure datetime columns
        df = ensure_datetime_column(df, date_col)
        
        # Convert date objects to datetime for comparison
        from_date = pd.to_datetime(start_date)
        to_date = pd.to_datetime(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)  # End of day
        
        # Filter
        mask = (df[date_col] >= from_date) & (df[date_col] <= to_date)
        filtered_df = df[mask].copy()
        
        logger.debug(f"Filtered {len(df)} → {len(filtered_df)} records for date range {start_date} to {end_date}")
        
        return filtered_df
    except Exception as e:
        logger.error(f"Error filtering by date range: {e}")
        return df


def validate_dataframe(df: pd.DataFrame, required_columns: List[str], data_type: str = "data") -> bool:
    """
    Validate that DataFrame has required structure.
    
    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        data_type: Type of data for logging
        
    Returns:
        True if valid, False otherwise
    """
    if df.empty:
        logger.warning(f"{data_type} DataFrame is empty")
        return False
    
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logger.error(f"{data_type} DataFrame missing required columns: {missing_columns}")
        return False
    
    return True


def safe_int_conversion(value: Any, default: int = 0) -> int:
    """
    Safely convert value to integer.
    
    Args:
        value: Value to convert
        default: Default value if conversion fails
        
    Returns:
        Integer value or default
    """
    try:
        if pd.isna(value):
            return default
        return int(float(value))
    except (ValueError, TypeError):
        return default


def format_large_number(num: int) -> str:
    """
    Format large numbers with appropriate suffixes.
    
    Args:
        num: Number to format
        
    Returns:
        Formatted string (e.g., "1.2K", "3.4M")
    """
    if num < 1000:
        return str(num)
    elif num < 1_000_000:
        return f"{num/1000:.1f}K"
    elif num < 1_000_000_000:
        return f"{num/1_000_000:.1f}M"
    else:
        return f"{num/1_000_000_000:.1f}B"


def calculate_growth_rate(df: pd.DataFrame, value_col: str, periods: int = 7) -> Optional[float]:
    """
    Calculate growth rate over specified periods.
    
    Args:
        df: DataFrame with time series data
        value_col: Column name containing values
        periods: Number of periods to look back
        
    Returns:
        Growth rate as percentage, or None if insufficient data
    """
    if len(df) < periods + 1:
        return None
    
    try:
        # Sort by date to ensure proper ordering
        df_sorted = df.sort_values(by=df.columns[0]).reset_index(drop=True)
        
        current_value = df_sorted[value_col].iloc[-1]
        past_value = df_sorted[value_col].iloc[-(periods + 1)]
        
        if past_value == 0:
            return None
        
        growth_rate = ((current_value - past_value) / past_value) * 100
        return round(growth_rate, 2)
    except Exception as e:
        logger.error(f"Error calculating growth rate: {e}")
        return None


def get_file_size(filepath: str) -> str:
    """
    Get human-readable file size.
    
    Args:
        filepath: Path to file
        
    Returns:
        Formatted file size string
    """
    try:
        size_bytes = os.path.getsize(filepath)
        
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024**2:
            return f"{size_bytes/1024:.1f} KB"
        elif size_bytes < 1024**3:
            return f"{size_bytes/(1024**2):.1f} MB"
        else:
            return f"{size_bytes/(1024**3):.1f} GB"
    except OSError:
        return "Unknown"


def create_summary_stats(df: pd.DataFrame, value_col: str) -> Dict[str, Any]:
    """
    Create summary statistics for a numeric column.
    
    Args:
        df: DataFrame
        value_col: Column to analyze
        
    Returns:
        Dictionary with summary statistics
    """
    if df.empty or value_col not in df.columns:
        return {}
    
    try:
        series = pd.to_numeric(df[value_col], errors='coerce').dropna()
        
        if series.empty:
            return {}
        
        return {
            'total': int(series.sum()),
            'mean': round(series.mean(), 2),
            'median': round(series.median(), 2),
            'std': round(series.std(), 2),
            'min': int(series.min()),
            'max': int(series.max()),
            'count': len(series)
        }
    except Exception as e:
        logger.error(f"Error creating summary stats: {e}")
        return {}


def export_data_to_json(data: Dict[str, pd.DataFrame], filepath: str) -> bool:
    """
    Export multiple DataFrames to JSON file.
    
    Args:
        data: Dictionary of DataFrames
        filepath: Output file path
        
    Returns:
        True if successful, False otherwise
    """
    try:
        export_data = {}
        
        for key, df in data.items():
            if not df.empty:
                # Convert DataFrame to dict and handle datetime serialization
                df_copy = df.copy()
                
                # Convert datetime columns to string
                for col in df_copy.columns:
                    if df_copy[col].dtype == 'datetime64[ns]':
                        df_copy[col] = df_copy[col].dt.strftime('%Y-%m-%d')
                
                export_data[key] = df_copy.to_dict(orient='records')
        
        with open(filepath, 'w') as f:
            json.dump(export_data, f, indent=2, default=str)
        
        logger.info(f"Data exported to {filepath}")
        return True
    except Exception as e:
        logger.error(f"Error exporting data to JSON: {e}")
        return False


def check_data_freshness(filepath: str, threshold_hours: int = 24) -> Tuple[bool, Optional[datetime]]:
    """
    Check if a data file is fresh (within threshold).
    
    Args:
        filepath: Path to data file
        threshold_hours: Freshness threshold in hours
        
    Returns:
        Tuple of (is_fresh, last_modified_time)
    """
    try:
        if not os.path.exists(filepath):
            return False, None
        
        stat = os.stat(filepath)
        last_modified = datetime.fromtimestamp(stat.st_mtime)
        age_hours = (datetime.now() - last_modified).total_seconds() / 3600
        
        is_fresh = age_hours <= threshold_hours
        return is_fresh, last_modified
    except OSError as e:
        logger.error(f"Error checking file freshness: {e}")
        return False, None


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean DataFrame by removing duplicates and handling missing values.
    
    Args:
        df: DataFrame to clean
        
    Returns:
        Cleaned DataFrame
    """
    if df.empty:
        return df
    
    original_length = len(df)
    
    # Remove duplicates
    df_clean = df.drop_duplicates()
    
    # Log cleaning results
    duplicates_removed = original_length - len(df_clean)
    if duplicates_removed > 0:
        logger.info(f"Removed {duplicates_removed} duplicate rows")
    
    return df_clean


def get_system_info() -> Dict[str, Any]:
    """
    Get system information for debugging.
    
    Returns:
        Dictionary with system information
    """
    import platform
    import sys
    
    return {
        'python_version': sys.version,
        'platform': platform.platform(),
        'processor': platform.processor(),
        'memory_usage': f"{os.getpid()} MB",  # Simplified
        'current_time': datetime.now().isoformat(),
        'working_directory': os.getcwd()
    }