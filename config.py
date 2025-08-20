"""
Configuration settings for the GitHub Analytics Dashboard
"""

import os
from typing import Dict, Any

# Data Management Configuration
DATA_CONFIG = {
    'data_directory': 'data',
    'refresh_threshold_hours': 24,  # Data older than this is considered stale
    'real_time_threshold_hours': 6,  # Real-time data refresh threshold
    'cleanup_threshold_days': 30,   # Clean up files older than this
}

# API Configuration
API_CONFIG = {
    'github_api_base': 'https://api.github.com',
    'pypi_api_base': 'https://pypistats.org/api',
    'request_timeout': 30,
    'rate_limit_wait': 60,
    'max_retries': 3,
}

# Logging Configuration
LOGGING_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'file_logging': False,
    'log_file': 'github_analytics.log'
}

# Repository Mapping
REPO_MAP = {
    "Skrub": ("skrub-data", "skrub"),
    "tslearn": ("tslearn-team", "tslearn"),
    "scikit-learn": ("scikit-learn", "scikit-learn"),
    "Aeon": ("aeon-toolkit", "aeon"),
    "Mapie": ("scikit-learn-contrib", "mapie"),
}

# Data Types Configuration
DATA_TYPES_CONFIG = {
    'cached_data': ['stars', 'forks', 'prs', 'downloads'],
    'real_time_data': ['contributions', 'issues', 'dependents'],
    'file_extensions': {
        'stars': 'csv',
        'forks': 'csv',
        'prs': 'csv',
        'downloads': 'csv',
        'contributions': 'csv',
        'issues': 'csv',
        'dependents': 'json'
    }
}

# Dashboard Configuration
DASHBOARD_CONFIG = {
    'default_date_range_days': 180,
    'cache_ttl_seconds': 3600,  # Streamlit cache TTL
    'real_time_cache_ttl_seconds': 1800,
    'max_dependents_display': 100,
    'chart_colors': {
        'stars': '#FFD700',
        'forks': '#1f77b4',
        'prs': '#FF7F0E',
        'downloads': '#9467bd',
        'contributions': '#2ca02c',
        'issues': '#d62728',
        'dependents': '#17becf'
    }
}

# Environment Variables
ENV_VARS = {
    'github_token': os.getenv('GITHUB_TOKEN'),
    'debug_mode': os.getenv('DEBUG', 'False').lower() == 'true',
    'log_level': os.getenv('LOG_LEVEL', 'INFO'),
}


def get_config() -> Dict[str, Any]:
    """
    Get complete configuration dictionary.
    
    Returns:
        Dictionary containing all configuration settings
    """
    return {
        'data': DATA_CONFIG,
        'api': API_CONFIG,
        'logging': LOGGING_CONFIG,
        'repositories': REPO_MAP,
        'data_types': DATA_TYPES_CONFIG,
        'dashboard': DASHBOARD_CONFIG,
        'environment': ENV_VARS
    }


def validate_config() -> bool:
    """
    Validate configuration settings.
    
    Returns:
        True if configuration is valid, False otherwise
    """
    issues = []
    
    # Check data directory
    data_dir = DATA_CONFIG['data_directory']
    if not os.path.exists(data_dir):
        try:
            os.makedirs(data_dir, exist_ok=True)
        except Exception as e:
            issues.append(f"Cannot create data directory '{data_dir}': {e}")
    
    # Check thresholds are positive
    if DATA_CONFIG['refresh_threshold_hours'] <= 0:
        issues.append("refresh_threshold_hours must be positive")
    
    if DATA_CONFIG['real_time_threshold_hours'] <= 0:
        issues.append("real_time_threshold_hours must be positive")
    
    # Check repository mapping
    for lib_name, (owner, repo) in REPO_MAP.items():
        if not owner or not repo:
            issues.append(f"Invalid repository mapping for {lib_name}: {owner}/{repo}")
    
    # Check data types configuration
    cached_types = set(DATA_TYPES_CONFIG['cached_data'])
    real_time_types = set(DATA_TYPES_CONFIG['real_time_data'])
    
    if cached_types & real_time_types:
        issues.append(f"Data types overlap between cached and real-time: {cached_types & real_time_types}")
    
    # Check file extensions
    all_types = cached_types | real_time_types
    extension_types = set(DATA_TYPES_CONFIG['file_extensions'].keys())
    
    if all_types != extension_types:
        missing = all_types - extension_types
        extra = extension_types - all_types
        if missing:
            issues.append(f"Missing file extensions for data types: {missing}")
        if extra:
            issues.append(f"Extra file extensions for unknown data types: {extra}")
    
    if issues:
        print("Configuration validation issues:")
        for issue in issues:
            print(f"  - {issue}")
        return False
    
    return True


def setup_logging():
    """Set up logging based on configuration."""
    import logging
    
    config = LOGGING_CONFIG
    log_level = getattr(logging, ENV_VARS.get('log_level', config['level']).upper())
    
    # Configure logging
    logging.basicConfig(
        level=log_level,
        format=config['format'],
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Add file handler if enabled
    if config['file_logging']:
        file_handler = logging.FileHandler(config['log_file'])
        file_handler.setLevel(log_level)
        formatter = logging.Formatter(config['format'])
        file_handler.setFormatter(formatter)
        
        # Add to root logger
        logging.getLogger().addHandler(file_handler)
    
    logger = logging.getLogger(__name__)
    logger.info("Logging configured successfully")


# Initialize configuration on import
if not validate_config():
    raise RuntimeError("Configuration validation failed")

setup_logging()