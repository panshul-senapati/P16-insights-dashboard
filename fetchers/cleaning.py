import polars as pl

try:
    import skrub  # type: ignore # noqa: F401
except Exception:
    skrub = None


def clean_polars_df(df: pl.DataFrame) -> pl.DataFrame:
    """Placeholder Skrub cleaning hook for Polars DataFrames (no-op by default)."""
    return df


