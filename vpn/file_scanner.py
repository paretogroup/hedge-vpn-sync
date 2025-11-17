"""
Module for file scanning.
"""
import os
from datetime import datetime
from pathlib import Path
from typing import Iterator
import logging

logger = logging.getLogger(__name__)


def scan_files(base_path: str) -> list[dict]:
    """
    Scan a directory recursively and return information about the files.
    
    Args:
        base_path: Base path to scan
        
    Returns:
        List of dictionaries with 'file_path' (absolute path) and 'updated_at' (ISO format)
    """
    result = []
    base_path_obj = Path(base_path).resolve()
    
    if not base_path_obj.exists():
        raise FileNotFoundError(f"Base path does not exist: {base_path}")
    
    if not base_path_obj.is_dir():
        raise NotADirectoryError(f"Base path is not a directory: {base_path}")
    
    logger.info(f"Scanning directory: {base_path}")
    
    try:
        for root, dirs, files in os.walk(base_path):
            # Ignore hidden and temporary directories
            dirs[:] = [d for d in dirs if not d.startswith('.')]
            
            for file in files:
                # Ignore hidden and temporary files
                if file.startswith('.') or file.startswith('~'):
                    continue
                
                try:
                    full_path = os.path.join(root, file)
                    stat = os.stat(full_path)
                    
                    result.append({
                        "file_path": os.path.abspath(full_path),
                        "updated_at": datetime.fromtimestamp(stat.st_mtime).isoformat()
                    })
                except (OSError, PermissionError) as e:
                    logger.warning(f"Error accessing file {full_path}: {e}")
                    continue
        
        logger.info(f"Scanning completed: {len(result)} files found")
        return result
    
    except Exception as e:
        logger.error(f"Error during scanning: {e}")
        raise


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


def prepare_file_entries(file_entries: list[dict], base_path: str) -> dict[str, dict]:
    """
    Prepare file entries for synchronization.
    
    Args:
        file_entries: List of dictionaries with 'file_path' and 'updated_at'
        base_path: Base path to calculate relative paths
        
    Returns:
        Dictionary mapping relative path -> {'file_path': str, 'updated_at': datetime}
    """
    result = {}
    for entry in file_entries:
        file_path = entry["file_path"]
        relative_path = get_relative_path(file_path, base_path)
        result[relative_path] = {
            "file_path": file_path,
            "updated_at": entry["updated_at"]
        }
    return result