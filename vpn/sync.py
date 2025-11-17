"""
Module for synchronization between VPN, BigQuery and GCS.
"""
import logging
import traceback
from datetime import datetime
from typing import Optional
import pandas as pd

from .config import Config
from .file_scanner import scan_files, get_relative_path, prepare_file_entries
from .gcs_operations import GCSUploader
from .bigquery_operations import BigQueryManager

logger = logging.getLogger(__name__)


class VPNSynchronizer:
    """Class for synchronization of files from the VPN."""
    
    def __init__(
        self,
        dataset_id: str = None,
        table_id: str = None,
        gcs_bucket: str = None,
        project_id: Optional[str] = None,
        base_path: str = None,
        log_table_id: str = None
    ):
        """
        Initialize the synchronizer.
        
        Args:
            dataset_id: ID of the dataset in BigQuery (uses Config if None)
            table_id: ID of the table (uses Config if None)
            gcs_bucket: Name of the GCS bucket (uses Config if None)
            project_id: ID of the GCP project (uses Config if None)
            base_path: Base path of the VPN (uses Config if None)
            log_table_id: ID of the log table (uses Config if None)
        """
        self.dataset_id = dataset_id or Config.BIGQUERY_DATASET_ID
        self.table_id = table_id or Config.BIGQUERY_TABLE_ID
        self.gcs_bucket = gcs_bucket or Config.GCS_BUCKET_NAME
        self.project_id = project_id or Config.GCP_PROJECT_ID
        self.base_path = base_path or Config.VPN_BASE_PATH
        self.log_table_id = log_table_id or Config.BIGQUERY_LOG_TABLE_ID
        
        # Initialize clients
        self.bq_manager = BigQueryManager(project_id=self.project_id)
        self.gcs_uploader = GCSUploader(self.gcs_bucket, project_id=self.project_id)
        
        logger.info("VPNSynchronizer initialized")
    
    def sync(self, dry_run: bool = False) -> dict:
        """
        Execute the complete synchronization.
        
        Identifies and applies:
        (A) Files in the VPN that are not in BigQuery → add to BigQuery and send to GCS
        (B) Files in BigQuery that are not in the VPN → delete from BigQuery and GCS
        (C) Files in both but with different dates → update in BigQuery and re-send to GCS
        
        Args:
            dry_run: If True, only show what would be done without executing
            
        Returns:
            Dictionary with statistics of the synchronization
        """
        sync_start_time = datetime.now()
        error_occurred = False
        error_message = None
        files_added = 0
        files_deleted = 0
        files_updated = 0
        
        try:
            # Scan files from the VPN
            logger.info("Scanning files from the VPN...")
            vpn_files = scan_files(self.base_path)
            
            # Check if any files were found
            if not vpn_files:
                logger.warning("=" * 60)
                logger.warning(f"⚠ No files found in {self.base_path}!")
                logger.warning("Operation interrupted.")
                logger.warning("=" * 60)
                return {
                    "success": False,
                    "files_added": 0,
                    "files_deleted": 0,
                    "files_updated": 0,
                    "error_message": "No files found in the mount point"
                }
            
            # Prepare data from the VPN
            vpn_data = {}
            vpn_file_map = {}
            for entry in vpn_files:
                file_path = entry["file_path"]
                relative_path = get_relative_path(file_path, self.base_path)
                updated_at = pd.to_datetime(entry["updated_at"], format='ISO8601').tz_localize(None)
                vpn_data[relative_path] = updated_at
                vpn_file_map[relative_path] = file_path
            
            logger.info(f"Files in the VPN: {len(vpn_data)}")
            
            # Read data from BigQuery
            logger.info("Reading data from BigQuery...")
            bq_df = self.bq_manager.get_table_data(self.dataset_id, self.table_id)
            bq_data = dict(zip(bq_df['file_path'], pd.to_datetime(bq_df['updated_at'])))
            
            logger.info(f"Files in BigQuery: {len(bq_data)}")
            
            # Identify differences
            vpn_paths = set(vpn_data.keys())
            bq_paths = set(bq_data.keys())
            
            # (A) Files in the VPN that are not in BigQuery
            to_add = vpn_paths - bq_paths
            
            # (B) Files in BigQuery that are not in the VPN
            to_delete = bq_paths - vpn_paths
            
            # (C) Files in both but with different dates
            common_paths = vpn_paths & bq_paths
            to_update = [
                path for path in common_paths
                if abs((vpn_data[path] - bq_data[path]).total_seconds()) >= Config.SYNC_TIME_TOLERANCE_SECONDS
            ]
            
            # Summary
            logger.info("=" * 60)
            logger.info("SYNCHRONIZATION SUMMARY")
            logger.info("=" * 60)
            logger.info(f"(A) Files to ADD: {len(to_add)}")
            logger.info(f"(B) Files to DELETE: {len(to_delete)}")
            logger.info(f"(C) Files to UPDATE: {len(to_update)}")
            logger.info("=" * 60)
            
            files_added = len(to_add)
            files_deleted = len(to_delete)
            files_updated = len(to_update)
            
            if dry_run:
                logger.info("DRY-RUN MODE: No changes will be made")
                return {
                    "success": True,
                    "files_added": files_added,
                    "files_deleted": files_deleted,
                    "files_updated": files_updated,
                    "dry_run": True
                }
            
            if len(to_add) == 0 and len(to_delete) == 0 and len(to_update) == 0:
                logger.info("✓ No changes necessary. Everything synchronized!")
                return {
                    "success": True,
                    "files_added": 0,
                    "files_deleted": 0,
                    "files_updated": 0,
                    "message": "No changes necessary"
                }
            
            # Execute operations
            
            # (A) Add new files
            if len(to_add) > 0:
                logger.info(f"Adding {len(to_add)} files...")
                self._add_files(to_add, vpn_data, vpn_file_map)
            
            # (B) Delete files that are not in the VPN anymore
            if len(to_delete) > 0:
                logger.info(f"Deleting {len(to_delete)} files...")
                self._delete_files(to_delete)
            
            # (C) Update files with different dates
            if len(to_update) > 0:
                logger.info(f"Updating {len(to_update)} files...")
                self._update_files(to_update, vpn_data, vpn_file_map)
            
            logger.info("=" * 60)
            logger.info("✓ Synchronization completed successfully!")
            logger.info("=" * 60)
            
        except Exception as e:
            error_occurred = True
            error_message = str(e)
            error_traceback = traceback.format_exc()
            logger.error("=" * 60)
            logger.error("✗ ERROR during synchronization!")
            logger.error("=" * 60)
            logger.error(f"Error: {error_message}")
            logger.error(f"Full traceback:\n{error_traceback}")
            logger.error("=" * 60)
        
        finally:
            # Register log of the synchronization
            if self.log_table_id:
                try:
                    success = not error_occurred
                    self.bq_manager.log_sync(
                        self.dataset_id, self.log_table_id, sync_start_time,
                        files_added, files_deleted, files_updated,
                        success=success, error_message=error_message
                    )
                except Exception as log_error:
                    logger.warning(f"⚠ Error registering log: {log_error}")
        
        return {
            "success": not error_occurred,
            "files_added": files_added,
            "files_deleted": files_deleted,
            "files_updated": files_updated,
            "error_message": error_message
        }
    
    def _add_files(self, to_add: set, vpn_data: dict, vpn_file_map: dict):
        """Add new files to BigQuery and GCS."""
        # Prepare data for insertion
        add_data = [
            {
                "file_path": path,
                "updated_at": vpn_data[path]
            }
            for path in to_add
        ]
        
        # Insert into BigQuery
        use_jsonl = len(add_data) >= Config.SYNC_USE_JSONL_THRESHOLD
        self.bq_manager.insert_files(
            add_data,
            self.dataset_id,
            self.table_id,
            use_jsonl=use_jsonl,
            temp_bucket=Config.GCS_TEMP_BUCKET
        )
        
        # Send files to GCS
        file_entries = [
            {"file_path": vpn_file_map[path], "updated_at": vpn_data[path].isoformat()}
            for path in to_add
        ]
        successes, failures = self.gcs_uploader.upload_files(
            file_entries,
            self.base_path,
            progress_interval=Config.SYNC_PROGRESS_INTERVAL
        )
        
        if failures > 0:
            logger.warning(f"⚠ {failures} files failed to upload to GCS")
        
        logger.info(f"✓ {len(to_add)} files added")
    
    def _delete_files(self, to_delete: set):
        """Delete files from BigQuery and GCS."""
        delete_list = list(to_delete)
        
        # Delete from BigQuery
        self.bq_manager.delete_files(delete_list, self.dataset_id, self.table_id)
        
        # Delete from GCS
        successes, failures = self.gcs_uploader.delete_files(
            delete_list,
            progress_interval=Config.SYNC_PROGRESS_INTERVAL
        )
        
        if failures > 0:
            logger.warning(f"⚠ {failures} files failed to delete from GCS")
        
        logger.info(f"✓ {len(to_delete)} files deleted")
    
    def _update_files(self, to_update: list, vpn_data: dict, vpn_file_map: dict):
        """Update files in BigQuery and re-send to GCS."""
        # Prepare updates
        file_updates = {path: vpn_data[path] for path in to_update}
        
        # Update in BigQuery
        self.bq_manager.update_files(file_updates, self.dataset_id, self.table_id)
        
        # Re-send updated files to GCS
        file_entries = [
            {"file_path": vpn_file_map[path], "updated_at": vpn_data[path].isoformat()}
            for path in to_update
        ]
        successes, failures = self.gcs_uploader.upload_files(
            file_entries,
            self.base_path,
            progress_interval=Config.SYNC_PROGRESS_INTERVAL
        )
        
        if failures > 0:
            logger.warning(f"⚠ {failures} files failed to re-upload to GCS")
        
        logger.info(f"✓ {len(to_update)} files updated")