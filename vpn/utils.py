"""
Module for utility functions.
"""
from datetime import datetime
from pathlib import Path
import os
import pandas as pd


def normalize_timestamp(value) -> datetime:
    """
    Normalize timestamps to second precision (no timezone information).
    
    Args:
        value: datetime-like value (datetime, Timestamp, string ISO, etc.)
        
    Returns:
        Normalized datetime without timezone
    """
    if isinstance(value, datetime):
        dt = value
    else:
        dt = pd.Timestamp(value).to_pydatetime()
    
    if dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)
    
    return dt

def get_relative_path(file_path: str, base_path: str) -> str:
    """
    Get the relative path of a file in relation to the base path.
    
    Args:
        file_path: Absolute path of the file
        base_path: Base path
        
    Returns:
        Relative path in POSIX format
    """
    return Path(os.path.relpath(file_path, base_path)).as_posix()