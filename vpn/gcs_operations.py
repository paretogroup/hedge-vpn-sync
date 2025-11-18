"""
Module for GCS operations.
"""
import os
import logging
import time
from pathlib import Path
from typing import Iterable, Optional
from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError

from .config import Config

logger = logging.getLogger(__name__)


class GCSUploader:
    """Class to manage uploads to GCS with retry and error handling."""
    
    def __init__(self, bucket_name: str, project_id: Optional[str] = None):
        """
        Initialize the GCS uploader.
        
        Args:
            bucket_name: Name of the GCS bucket
            project_id: GCP project ID (optional)
        """
        self.bucket_name = bucket_name
        self.client = storage.Client(project=project_id)
        self.bucket = self.client.bucket(bucket_name)
        self._verify_bucket_exists()
    
    def _verify_bucket_exists(self):
        """Check if the bucket exists."""
        try:
            self.bucket.reload()
            logger.info(f"Bucket verified: {self.bucket_name}")
        except GoogleCloudError as e:
            logger.error(f"Error accessing bucket {self.bucket_name}: {e}")
            raise
    
    def upload_file(
        self,
        local_file_path: str,
        gcs_blob_path: str,
        retry_attempts: int = None,
        retry_delay: float = None
    ) -> bool:
        """
        Upload a file to GCS with retry.
        
        Args:
            local_file_path: Local file path
            gcs_blob_path: GCS blob path
            retry_attempts: Number of attempts (uses Config if None)
            retry_delay: Delay between attempts in seconds (uses Config if None)
            
        Returns:
            True if the upload was successful, False otherwise
        """
        retry_attempts = retry_attempts or Config.UPLOAD_RETRY_ATTEMPTS
        retry_delay = retry_delay or Config.UPLOAD_RETRY_DELAY
        
        if not os.path.exists(local_file_path):
            logger.error(f"File not found: {local_file_path}")
            return False
        
        blob = self.bucket.blob(gcs_blob_path)
        
        for attempt in range(1, retry_attempts + 1):
            try:
                blob.upload_from_filename(local_file_path)
                logger.debug(f"Upload successful: {local_file_path} -> gs://{self.bucket_name}/{gcs_blob_path}")
                return True
            except GoogleCloudError as e:
                if attempt < retry_attempts:
                    logger.warning(
                        f"Attempt {attempt}/{retry_attempts} failed for {local_file_path}: {e}. "
                        f"Trying again in {retry_delay}s..."
                    )
                    time.sleep(retry_delay)
                else:
                    logger.error(f"Upload failed after {retry_attempts} attempts: {local_file_path}")
                    return False
            except Exception as e:
                logger.error(f"Unexpected error uploading {local_file_path}: {e}")
                return False
        
        return False
    
    def upload_files(
        self,
        file_entries: Iterable[dict],
        base_path: str,
        progress_interval: int = None
    ) -> tuple[int, int, list[str]]:
        """
        Upload multiple files to GCS.
        
        Args:
            file_entries: Iterable of dictionaries with 'file_path' (local path) and 'updated_at'
            base_path: Base path to calculate relative paths
            progress_interval: Progress log interval (uses Config if None)
            
        Returns:
            Tuple (successes, failures, successful_paths) where successful_paths is a list of relative paths
        """
        progress_interval = progress_interval or Config.SYNC_PROGRESS_INTERVAL
        file_list = list(file_entries)
        total = len(file_list)
        successes = 0
        failures = 0
        successful_paths = []
        
        logger.info(f"Starting upload of {total} files to gs://{self.bucket_name}")
        
        for idx, entry in enumerate(file_list, 1):
            file_path = entry["file_path"]
            relative_path = Path(os.path.relpath(file_path, base_path)).as_posix()
            
            if self.upload_file(file_path, relative_path):
                successes += 1
                successful_paths.append(relative_path)
            else:
                failures += 1
            
            if idx % progress_interval == 0 or idx == total:
                logger.info(f"Progress: {idx}/{total} files processed ({successes} successes, {failures} failures)")
        
        logger.info(f"Upload completed: {successes} successes, {failures} failures of {total} files")
        return successes, failures, successful_paths
    
    def delete_file(self, gcs_blob_path: str) -> bool:
        """
        Delete a file from GCS.
        
        Args:
            gcs_blob_path: GCS blob path
            
        Returns:
            True if the file was deleted successfully, False otherwise
        """
        try:
            blob = self.bucket.blob(gcs_blob_path)
            if blob.exists():
                blob.delete()
                logger.debug(f"File deleted from GCS: gs://{self.bucket_name}/{gcs_blob_path}")
                return True
            else:
                logger.debug(f"File does not exist in GCS: gs://{self.bucket_name}/{gcs_blob_path}")
                return True  # Consider success if it already doesn't exist
        except GoogleCloudError as e:
            logger.error(f"Error deleting file from GCS {gcs_blob_path}: {e}")
            return False
    
    def delete_files(
        self,
        blob_paths: Iterable[str],
        progress_interval: int = None
    ) -> tuple[int, int]:
        """
        Delete multiple files from GCS.
        
        Args:
            blob_paths: Iterable of GCS blob paths
            progress_interval: Progress log interval (uses Config if None)
            
        Returns:
            Tuple (successes, failures)
        """
        progress_interval = progress_interval or Config.SYNC_PROGRESS_INTERVAL
        paths_list = list(blob_paths)
        total = len(paths_list)
        successes = 0
        failures = 0
        
        logger.info(f"Deleting {total} files from GCS...")
        
        for idx, blob_path in enumerate(paths_list, 1):
            if self.delete_file(blob_path):
                successes += 1
            else:
                failures += 1
            
            if idx % progress_interval == 0 or idx == total:
                logger.info(f"Progress: {idx}/{total} files processed ({successes} successes, {failures} failures)")
        
        logger.info(f"Deletion completed: {successes} successes, {failures} failures of {total} files")
        return successes, failures
    
    def list_files(self) -> set[str]:
        """
        List all files in the GCS bucket.
        
        Returns:
            Set of relative file paths (blob names)
        """
        try:
            blobs = self.client.list_blobs(self.bucket_name)
            file_paths = {blob.name for blob in blobs}
            logger.info(f"Found {len(file_paths)} files in GCS bucket")
            return file_paths
        except GoogleCloudError as e:
            logger.error(f"Error listing files from GCS: {e}")
            raise
    
    def file_exists(self, gcs_blob_path: str) -> bool:
        """
        Check if a file exists in GCS.
        
        Args:
            gcs_blob_path: GCS blob path
            
        Returns:
            True if the file exists, False otherwise
        """
        try:
            blob = self.bucket.blob(gcs_blob_path)
            return blob.exists()
        except GoogleCloudError as e:
            logger.error(f"Error checking file existence in GCS {gcs_blob_path}: {e}")
            return False