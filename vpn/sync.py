"""
Module for synchronization between VPN, BigQuery and GCS.
"""
import logging
import traceback
from datetime import datetime
from typing import Optional

from .config import Config
from .file_scanner import scan_files
from .gcs_operations import GCSUploader
from .bigquery_operations import BigQueryManager
from .utils import normalize_timestamp, get_relative_path

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
                updated_at = normalize_timestamp(entry["updated_at"])
                vpn_data[relative_path] = updated_at
                vpn_file_map[relative_path] = file_path
            
            logger.info(f"Files in the VPN: {len(vpn_data)}")
            
            # Read data from BigQuery
            logger.info("Reading data from BigQuery...")
            bq_df = self.bq_manager.get_table_data(self.dataset_id, self.table_id)
            bq_data = {}
            if not bq_df.empty:
                for _, row in bq_df.iterrows():
                    bq_data[row['file_path']] = normalize_timestamp(row['updated_at'])
            
            logger.info(f"Files in BigQuery: {len(bq_data)}")
            
            # Verify consistency between GCS and BigQuery
            logger.info("Verifying consistency between GCS and BigQuery...")
            gcs_files = self.gcs_uploader.list_files()
            gcs_paths = set(gcs_files)
            bq_paths = set(bq_data.keys())
            
            # Identify inconsistencies between GCS and BigQuery
            gcs_only = gcs_paths - bq_paths  # Files in GCS but not in BQ
            bq_only = bq_paths - gcs_paths   # Files in BQ but not in GCS
            
            if gcs_only or bq_only:
                logger.warning("=" * 60)
                logger.warning("⚠ INCONSISTENCIES DETECTED BETWEEN GCS AND BIGQUERY")
                logger.warning("=" * 60)
                logger.warning(f"Files in GCS but not in BQ: {len(gcs_only)}")
                logger.warning(f"Files in BQ but not in GCS: {len(bq_only)}")
                logger.warning("These will be reconciled during synchronization")
                logger.warning("=" * 60)
            
            # Identify differences between VPN and BigQuery
            vpn_paths = set(vpn_data.keys())
            
            # (1) Files in the VPN that are not in BigQuery
            to_add = vpn_paths - bq_paths
            
            # (2) Files in BigQuery that are not in the VPN
            to_delete = bq_paths - vpn_paths
            
            # (3) Files in both but with different timestamps (comparação exata)
            common_paths = vpn_paths & bq_paths
            to_update = [
                path for path in common_paths
                if vpn_data[path] != bq_data[path]
            ]
            
            # (4) Reconciliation: Files in GCS but not in BQ
            # - If in VPN: add to BQ (will be handled by to_add)
            # - If not in VPN: delete from GCS (orphaned)
            gcs_orphans = gcs_only - vpn_paths  # In GCS but not in VPN or BQ
            if gcs_orphans:
                logger.info(f"Found {len(gcs_orphans)} orphaned files in GCS (not in VPN or BQ)")
                to_delete.extend(gcs_orphans)
            
            # Files in GCS and VPN but not in BQ will be handled by to_add (already calculated)
            gcs_in_vpn_not_bq = gcs_only & vpn_paths
            if gcs_in_vpn_not_bq:
                logger.info(f"Found {len(gcs_in_vpn_not_bq)} files in GCS and VPN but not in BQ (will be added to BQ)")
            
            # (5) Reconciliation: Files in BQ but not in GCS
            # - If in VPN: re-upload to GCS (add to to_update)
            # - If not in VPN: delete from BQ (already in to_delete)
            bq_missing_gcs = bq_only & vpn_paths  # In BQ and VPN but not in GCS
            if bq_missing_gcs:
                logger.info(f"Found {len(bq_missing_gcs)} files in BQ and VPN but missing from GCS (will be re-uploaded)")
                to_update.extend(bq_missing_gcs)  # Re-upload to GCS
            
            # Remove duplicates from to_delete and to_update
            to_delete = list(set(to_delete))
            to_update = list(set(to_update))
            
            # Summary
            logger.info("=" * 60)
            logger.info("SYNCHRONIZATION SUMMARY")
            logger.info("=" * 60)
            logger.info(f"Files to ADD: {len(to_add)}")
            logger.info(f"Files to DELETE: {len(to_delete)}")
            logger.info(f"Files to UPDATE: {len(to_update)}")
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

            # Execute operations in optimized order for consistency
                        
            # (1) Delete files first (cleanup before adding/updating)
            if len(to_delete) > 0:
                logger.info(f"Deleting {len(to_delete)} files...")
                delete_success = self._delete_files(to_delete)
                if not delete_success:
                    logger.warning("Some deletions failed, but continuing with other operations...")
            
            # (2) Add new files
            if len(to_add) > 0:
                logger.info(f"Adding {len(to_add)} files...")
                add_success = self._add_files(to_add, vpn_data, vpn_file_map)
                if not add_success:
                    logger.warning("Some additions failed, but continuing with other operations...")
            
            # (3) Update files with different dates
            if len(to_update) > 0:
                logger.info(f"Updating {len(to_update)} files...")
                update_success = self._update_files(to_update, vpn_data, vpn_file_map)
                if not update_success:
                    logger.warning("Some updates failed, but continuing with other operations...")
            
            # Final consistency check
            logger.info("Performing final consistency verification...")
            self._verify_final_consistency(vpn_data, vpn_paths)
            
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
                        self.dataset_id, self.log_table_id,
                        sync_start_time, datetime.now(),
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
    
    def _add_files(self, to_add: list, vpn_data: dict, vpn_file_map: dict) -> bool:
        """
        Add new files to BigQuery and GCS.
        
        Returns:
            True if all operations succeeded, False otherwise
        """
        if not to_add:
            return True
        
        # Send files to GCS first
        file_entries = [
            {
                "file_path": vpn_file_map[path],
                "updated_at": vpn_data[path].isoformat(timespec='seconds')
            }
            for path in to_add
        ]
        successes, failures, successful_gcs_paths = self.gcs_uploader.upload_files(
            file_entries,
            self.base_path,
            progress_interval=Config.SYNC_PROGRESS_INTERVAL
        )
        
        # Track which files were successfully uploaded (match by relative path)
        successfully_uploaded = []
        successful_gcs_paths_set = set(successful_gcs_paths)
        for path in to_add:
            relative_path = get_relative_path(vpn_file_map[path], self.base_path)
            if relative_path in successful_gcs_paths_set:
                successfully_uploaded.append(path)
        
        if failures > 0:
            logger.warning(f"⚠ {failures} files failed to upload to GCS")
            # Only add to BQ files that were successfully uploaded to GCS
            if not successfully_uploaded:
                logger.error("No files were successfully uploaded to GCS, skipping BQ insertion")
                return False
        
        # Prepare data for insertion in BigQuery (only successfully uploaded files)
        add_data = [
            {
                "file_path": path,
                "updated_at": vpn_data[path]
            }
            for path in successfully_uploaded
        ]
        
        try:
            # Always use JSONL via temporary bucket
            self.bq_manager.insert_files(
                add_data,
                self.dataset_id,
                self.table_id,
                use_jsonl=True,
                temp_bucket=Config.GCS_TEMP_BUCKET
            )
            
            if len(successfully_uploaded) == len(to_add):
                logger.info(f"✓ {len(to_add)} files added successfully")
                return True
            else:
                logger.warning(
                    f"⚠ {len(successfully_uploaded)}/{len(to_add)} files added "
                    f"({len(to_add) - len(successfully_uploaded)} failed in GCS)"
                )
                return False
        except Exception as e:
            logger.error(f"Error inserting files into BigQuery: {e}")
            # Files are in GCS but not in BQ - this will be reconciled in next sync
            return False
    
    def _delete_files(self, to_delete: list) -> bool:
        """
        Delete files from BigQuery and GCS.
        
        Returns:
            True if all operations succeeded, False otherwise
        """
        if not to_delete:
            return True
        
        delete_list = list(set(to_delete))
        
        # Delete from BigQuery first (safer - metadata before data)
        try:
            self.bq_manager.delete_files(delete_list, self.dataset_id, self.table_id)
        except Exception as e:
            logger.error(f"Error deleting files from BigQuery: {e}")
            # Continue with GCS deletion even if BQ fails
        
        # Delete from GCS
        successes, failures = self.gcs_uploader.delete_files(
            delete_list,
            progress_interval=Config.SYNC_PROGRESS_INTERVAL
        )
        
        if failures > 0:
            logger.warning(f"⚠ {failures} files failed to delete from GCS")
            # Files deleted from BQ but not from GCS - will be reconciled in next sync
        
        if successes == len(delete_list):
            logger.info(f"✓ {len(delete_list)} files deleted successfully")
            return True
        else:
            logger.warning(
                f"⚠ {successes}/{len(delete_list)} files deleted from GCS "
                f"({len(delete_list) - successes} failed)"
            )
            return False
    
    def _update_files(self, to_update: list, vpn_data: dict, vpn_file_map: dict) -> bool:
        """
        Update files in BigQuery and re-send to GCS.
        
        Returns:
            True if all operations succeeded, False otherwise
        """
        if not to_update:
            return True
        
        to_update = list(set(to_update))  # Remove duplicates
        
        # Re-send updated files to GCS
        file_entries = [
            {
                "file_path": vpn_file_map[path],
                "updated_at": vpn_data[path].isoformat(timespec='seconds')
            }
            for path in to_update
        ]
        successes, failures, successful_gcs_paths = self.gcs_uploader.upload_files(
            file_entries,
            self.base_path,
            progress_interval=Config.SYNC_PROGRESS_INTERVAL
        )
        
        # Track which files were successfully uploaded (match by relative path)
        successfully_uploaded = []
        successful_gcs_paths_set = set(successful_gcs_paths)
        for path in to_update:
            relative_path = get_relative_path(vpn_file_map[path], self.base_path)
            if relative_path in successful_gcs_paths_set:
                successfully_uploaded.append(path)
        
        if failures > 0:
            logger.warning(f"⚠ {failures} files failed to re-upload to GCS")
        
        # Apply updates in BigQuery via temp table + merge (only for successfully uploaded)
        if successfully_uploaded:
            file_updates = {path: vpn_data[path] for path in successfully_uploaded}
            try:
                self.bq_manager.update_files(
                    file_updates,
                    self.dataset_id,
                    self.table_id,
                    temp_bucket=Config.GCS_TEMP_BUCKET
                )
            except Exception as e:
                logger.error(f"Error updating files in BigQuery: {e}")
                return False
        
        if len(successfully_uploaded) == len(to_update):
            logger.info(f"✓ {len(to_update)} files updated successfully")
            return True
        else:
            logger.warning(
                f"⚠ {len(successfully_uploaded)}/{len(to_update)} files updated "
                f"({len(to_update) - len(successfully_uploaded)} failed in GCS)"
            )
            return False
    
    def _verify_final_consistency(self, vpn_data: dict, vpn_paths: set):
        """
        Perform a final consistency check between VPN, GCS, and BigQuery.
        
        Args:
            vpn_data: Dictionary of VPN file paths and timestamps
            vpn_paths: Set of VPN file paths
        """
        try:
            # Re-read BQ data
            bq_df = self.bq_manager.get_table_data(self.dataset_id, self.table_id)
            bq_paths = set(bq_df['file_path'].tolist()) if not bq_df.empty else set()
            
            # Re-read GCS data
            gcs_paths = self.gcs_uploader.list_files()
            
            # Check consistency
            inconsistencies = []
            
            # Files in BQ but not in GCS
            bq_not_in_gcs = bq_paths - gcs_paths
            if bq_not_in_gcs:
                inconsistencies.append(f"{len(bq_not_in_gcs)} files in BQ but not in GCS")
            
            # Files in GCS but not in BQ
            gcs_not_in_bq = gcs_paths - bq_paths
            if gcs_not_in_bq:
                inconsistencies.append(f"{len(gcs_not_in_bq)} files in GCS but not in BQ")
            
            # Files in VPN but not in BQ or GCS
            vpn_not_in_bq = vpn_paths - bq_paths
            vpn_not_in_gcs = vpn_paths - gcs_paths
            if vpn_not_in_bq:
                inconsistencies.append(f"{len(vpn_not_in_bq)} files in VPN but not in BQ")
            if vpn_not_in_gcs:
                inconsistencies.append(f"{len(vpn_not_in_gcs)} files in VPN but not in GCS")
            
            if inconsistencies:
                logger.warning("=" * 60)
                logger.warning("⚠ FINAL CONSISTENCY CHECK - INCONSISTENCIES FOUND")
                logger.warning("=" * 60)
                for issue in inconsistencies:
                    logger.warning(f"  - {issue}")
                logger.warning("These will be resolved in the next synchronization")
                logger.warning("=" * 60)
            else:
                logger.info("✓ Final consistency check passed: VPN, GCS, and BigQuery are synchronized")
        except Exception as e:
            logger.warning(f"Could not perform final consistency check: {e}")
            # Don't fail the sync if verification fails