import os
import pandas as pd
import streamlit as st


class DataLoader:
    """Handles loading and parsing of CSV data files."""
    
    @staticmethod
    @st.cache_data
    def load_csv(path, expected_col):
        """
        Load and parse CSV file with validation.
        
        Args:
            path (str): Path to the CSV file
            expected_col (str): Expected column name for validation
            
        Returns:
            pd.DataFrame: Loaded and validated dataframe
        """
        if not os.path.exists(path):
            st.warning(f"⚠️ File not found: {path}")
            return pd.DataFrame(columns=["date", expected_col])
        
        try:
            df = pd.read_csv(path)
            
            # Validate required columns
            if "date" not in df.columns or expected_col not in df.columns:
                st.error(f"⚠️ The file {path} must contain 'date' and '{expected_col}' columns.")
                return pd.DataFrame(columns=["date", expected_col])
            
            # Parse and clean date column
            df["date"] = pd.to_datetime(df["date"], errors='coerce')
            df.dropna(subset=["date"], inplace=True)
            
            return df
            
        except Exception as e:
            st.error(f"⚠️ Error loading {path}: {str(e)}")
            return pd.DataFrame(columns=["date", expected_col])
    
    @staticmethod
    def validate_dataframe(df, required_columns):
        """
        Validate that dataframe contains required columns.
        
        Args:
            df (pd.DataFrame): Dataframe to validate
            required_columns (list): List of required column names
            
        Returns:
            bool: True if valid, False otherwise
        """
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            st.error(f"⚠️ Missing required columns: {missing_cols}")
            return False
        return True
    
    @staticmethod
    def filter_by_date_range(df, start_date, end_date, date_col="date"):
        """
        Filter dataframe by date range.
        
        Args:
            df (pd.DataFrame): Dataframe to filter
            start_date: Start date for filtering
            end_date: End date for filtering
            date_col (str): Name of the date column
            
        Returns:
            pd.DataFrame: Filtered dataframe
        """
        from_date = pd.to_datetime(start_date)
        to_date = pd.to_datetime(end_date)
        
        return df[(df[date_col] >= from_date) & (df[date_col] <= to_date)]
