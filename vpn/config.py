"""
Project settings using environment variables.
"""
import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()


class Config:
    """Centralized project settings."""
    
    # Paths
    VPN_BASE_PATH: str = os.getenv("VPN_BASE_PATH", "/mnt/pareto")
    
    # Google Cloud Platform
    GCP_PROJECT_ID: Optional[str] = os.getenv("GCP_PROJECT_ID")
    GCS_BUCKET_NAME: str = os.getenv("GCS_BUCKET_NAME")
    GCS_TEMP_BUCKET: str = os.getenv("GCS_TEMP_BUCKET")
    
    # BigQuery
    BIGQUERY_DATASET_ID: str = os.getenv("BIGQUERY_DATASET_ID")
    BIGQUERY_TABLE_ID: str = os.getenv("BIGQUERY_TABLE_ID")
    BIGQUERY_LOG_TABLE_ID: str = os.getenv("BIGQUERY_LOG_TABLE_ID")
    
    # Synchronization settings
    SYNC_BATCH_SIZE: int = int(os.getenv("SYNC_BATCH_SIZE"))
    SYNC_USE_JSONL_THRESHOLD: int = int(os.getenv("SYNC_USE_JSONL_THRESHOLD"))
    SYNC_TIME_TOLERANCE_SECONDS: float = float(os.getenv("SYNC_TIME_TOLERANCE_SECONDS"))
    SYNC_PROGRESS_INTERVAL: int = int(os.getenv("SYNC_PROGRESS_INTERVAL"))
    
    # Upload settings
    UPLOAD_RETRY_ATTEMPTS: int = int(os.getenv("UPLOAD_RETRY_ATTEMPTS"))
    UPLOAD_RETRY_DELAY: float = float(os.getenv("UPLOAD_RETRY_DELAY"))
    
    @classmethod
    def validate(cls) -> list[str]:
        """
        Validate the required settings.
        
        Returns:
            List of errors found (empty if everything is OK)
        """
        errors = []
        
        if not os.path.exists(cls.VPN_BASE_PATH):
            errors.append(f"VPN path does not exist: {cls.VPN_BASE_PATH}")
        
        if not cls.GCS_BUCKET_NAME:
            errors.append("GCS_BUCKET_NAME not configured")
        
        if not cls.BIGQUERY_DATASET_ID:
            errors.append("BIGQUERY_DATASET_ID not configured")
        
        if not cls.BIGQUERY_TABLE_ID:
            errors.append("BIGQUERY_TABLE_ID not configured")
        
        return errors

