"""
Module for file scanning.
"""
import os
from pathlib import Path
import logging
from datetime import datetime

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
                    
                    timestamp = datetime.fromtimestamp(stat.st_mtime).replace(microsecond=0)
                    result.append({
                        "file_path": os.path.abspath(full_path),
                        "updated_at": timestamp.isoformat()
                    })
                except (OSError, PermissionError) as e:
                    logger.warning(f"Error accessing file {full_path}: {e}")
                    continue
        
        logger.info(f"Scanning completed: {len(result)} files found")
        return result
    
    except Exception as e:
        logger.error(f"Error during scanning: {e}")
        raise

