import re
from datetime import datetime


def normalize_to_first_of_month(date: datetime) -> datetime:
    """Normalize date to the 1st of the month"""
    return date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def clean_col_name(col_name: str) -> str:
    """Cleans up column names."""
    col_name = re.sub(r"[^\w\s]", "", col_name)  # Remove special characters
    col_name = col_name.replace(" ", "_")  # Replace spaces with underscores
    col_name = col_name.strip().lower()  # Convert to lowercase and strip whitespace
    return col_name
