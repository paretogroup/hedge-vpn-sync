"""
Module for BigQuery operations.
"""
import logging
import tempfile
import os
from datetime import datetime
from typing import Optional
import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, WriteDisposition, SourceFormat
from google.cloud.exceptions import GoogleCloudError

from .config import Config
from .utils import normalize_timestamp

logger = logging.getLogger(__name__)


class BigQueryManager:
    """Class to manage BigQuery operations."""
    
    def __init__(self, project_id: Optional[str] = None):
        """
        Initialize the BigQuery manager.
        
        Args:
            project_id: GCP project ID (optional, uses the default client if None)
        """
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id or self.client.project
        logger.info(f"BigQuery initialized for project: {self.project_id}")
    
    def get_table_schema(self) -> list[bigquery.SchemaField]:
        """
        Return the default schema of the files table.
        
        Returns:
            List of SchemaFields
        """
        return [
            bigquery.SchemaField("file_path", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("updated_at", "DATETIME", mode="REQUIRED"),
        ]
    
    def get_log_table_schema(self) -> list[bigquery.SchemaField]:
        """
        Return the schema of the log table.
        
        Returns:
            List of SchemaFields
        """
        return [
            bigquery.SchemaField("sync_date", "DATETIME", mode="REQUIRED"),
            bigquery.SchemaField("files_added", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("files_deleted", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("files_updated", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("success", "BOOLEAN", mode="REQUIRED"),
            bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
        ]
    
    def create_or_overwrite_table(
        self,
        file_entries: list[dict],
        dataset_id: str,
        table_id: str,
        base_path: str
    ):
        """
        Create or overwrite a table in BigQuery with the file data.
        
        Args:
            file_entries: List of dictionaries with 'file_path' and 'updated_at'
            dataset_id: ID of the dataset in BigQuery
            table_id: ID of the table to be created/overwritten
            base_path: Base path to calculate the relative path
        """
        from .utils import get_relative_path
        
        logger.info(f"Creating/overwriting table {self.project_id}.{dataset_id}.{table_id}...")
        
        # Prepare data for DataFrame
        data = []
        for entry in file_entries:
            file_path = entry["file_path"]
            relative_path = get_relative_path(file_path, base_path)
            data.append({
                "file_path": relative_path,
                "updated_at": entry["updated_at"]
            })
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        # Convert updated_at to datetime without sub-seconds
        if not df.empty:
            df["updated_at"] = pd.to_datetime(
                df["updated_at"].apply(normalize_timestamp)
            )
        
        # Configure load job
        job_config = LoadJobConfig(
            schema=self.get_table_schema(),
            write_disposition=WriteDisposition.WRITE_TRUNCATE,
            source_format=SourceFormat.PARQUET,
        )
        
        # Create table reference
        table_ref = self.client.dataset(dataset_id).table(table_id)
        
        # Upload
        job = self.client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Wait for completion
        
        # Verify result
        table = self.client.get_table(table_ref)
        logger.info(
            f"✓ Table created/updated: {table.num_rows} rows, "
            f"{table.num_bytes / 1024 / 1024:.2f} MB"
        )
    
    def get_table_data(
        self,
        dataset_id: str,
        table_id: str
    ) -> pd.DataFrame:
        """
        Read the complete table from BigQuery and return as DataFrame.
        
        Args:
            dataset_id: ID of the dataset in BigQuery
            table_id: ID of the table
            
        Returns:
            DataFrame with the columns file_path and updated_at
        """
        query = f"""
        SELECT 
            file_path,
            updated_at
        FROM `{self.project_id}.{dataset_id}.{table_id}`
        """
        
        logger.info(f"Reading table {self.project_id}.{dataset_id}.{table_id}...")
        
        try:
            results = self.client.query(query).result()
            data = []
            for row in results:
                data.append({
                    'file_path': row.file_path,
                    'updated_at': row.updated_at
                })
            
            df = pd.DataFrame(data)
            
            if not df.empty:
                df['updated_at'] = pd.to_datetime(
                    df['updated_at'].apply(normalize_timestamp)
                )
            
            logger.info(f"✓ {len(df)} rows read from BigQuery")
            return df
        
        except GoogleCloudError as e:
            logger.error(f"Error reading table from BigQuery: {e}")
            raise
    
    def insert_files(
        self,
        file_data: list[dict],
        dataset_id: str,
        table_id: str,
        use_jsonl: bool = False,
        temp_bucket: str = None
    ):
        """
        Insert files into the BigQuery table.
        
        Args:
            file_data: List of dictionaries with 'file_path' and 'updated_at'
            dataset_id: ID of the dataset in BigQuery
            table_id: ID of the table
            use_jsonl: If True, use JSONL in GCS for large volumes
            temp_bucket: Temporary bucket for JSONL (uses Config if None)
        """
        if not file_data:
            logger.info("No files to insert")
            return
        
        table_ref = self.client.dataset(dataset_id).table(table_id)
        temp_bucket = temp_bucket or Config.GCS_TEMP_BUCKET
        
        if use_jsonl and len(file_data) >= Config.SYNC_USE_JSONL_THRESHOLD:
            # Use JSONL in GCS for large volumes
            logger.info(f"Using JSONL to insert {len(file_data)} files...")
            self._insert_via_jsonl(file_data, table_ref, temp_bucket)
        else:
            # Use DataFrame directly
            logger.info(f"Inserting {len(file_data)} files via DataFrame...")
            self._insert_via_dataframe(file_data, table_ref)
    
    def _insert_via_dataframe(self, file_data: list[dict], table_ref: bigquery.TableReference):
        """Insert data via DataFrame."""
        df = pd.DataFrame(file_data)
        if not df.empty:
            df["updated_at"] = pd.to_datetime(
                df["updated_at"].apply(normalize_timestamp)
            )
        
        job_config = LoadJobConfig(
            schema=self.get_table_schema(),
            write_disposition=WriteDisposition.WRITE_APPEND,
        )
        
        job = self.client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        logger.info(f"✓ {len(file_data)} files inserted via DataFrame")
    
    def _insert_via_jsonl(
        self,
        file_data: list[dict],
        table_ref: bigquery.TableReference,
        temp_bucket: str,
        write_disposition: WriteDisposition = WriteDisposition.WRITE_APPEND
    ):
        """Insert data via JSONL in GCS."""
        import json
        from google.cloud import storage
        
        # Create temporary JSONL file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            for row in file_data:
                f.write(json.dumps({
                    "file_path": row["file_path"],
                    "updated_at": normalize_timestamp(value)(row["updated_at"])
                }) + "\n")
            temp_file = f.name
        
        try:
            # Upload to temporary GCS
            storage_client = storage.Client()
            bucket_obj = storage_client.bucket(temp_bucket)
            blob_name = f"sync_add_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
            blob = bucket_obj.blob(blob_name)
            blob.upload_from_filename(temp_file)
            
            # Load from GCS
            job_config = LoadJobConfig(
                schema=self.get_table_schema(),
                source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=write_disposition,
            )
            
            uri = f"gs://{temp_bucket}/{blob_name}"
            job = self.client.load_table_from_uri(uri, table_ref, job_config=job_config)
            job.result()
            
            # Clear temporary file
            blob.delete()
            logger.info(f"✓ {len(file_data)} files inserted via JSONL")
        
        finally:
            if os.path.exists(temp_file):
                os.unlink(temp_file)
    
    def delete_files(
        self,
        file_paths: list[str],
        dataset_id: str,
        table_id: str
    ):
        """
        Delete files from the BigQuery table.
        
        Args:
            file_paths: List of file paths to delete
            dataset_id: ID of the dataset in BigQuery
            table_id: ID of the table
        """
        if not file_paths:
            logger.info("No files to delete")
            return
        
        # Process in batches
        batch_size = Config.SYNC_BATCH_SIZE
        total_batches = (len(file_paths) + batch_size - 1) // batch_size
        
        logger.info(f"Deleting {len(file_paths)} files from BigQuery in {total_batches} batches...")
        
        for i in range(0, len(file_paths), batch_size):
            batch = file_paths[i:i+batch_size]
            batch_num = (i // batch_size) + 1
            
            escaped_paths = [path.replace("'", "''") for path in batch]
            placeholders = ", ".join([f"'{path}'" for path in escaped_paths])
            
            query = f"""
            DELETE FROM `{self.project_id}.{dataset_id}.{table_id}`
            WHERE file_path IN ({placeholders})
            """
            
            try:
                job = self.client.query(query)
                job.result()
                logger.debug(f"Batch {batch_num}/{total_batches}: {len(batch)} files deleted")
            except GoogleCloudError as e:
                logger.error(f"Error deleting batch {batch_num}: {e}")
                raise
        
        logger.info(f"✓ {len(file_paths)} files deleted from BigQuery")
    
    def update_files(
        self,
        file_updates: dict[str, datetime],
        dataset_id: str,
        table_id: str,
        temp_bucket: Optional[str] = None
    ):
        """
        Update the modification dates of the files in BigQuery via temporary table + merge.
        
        Args:
            file_updates: Dictionary mapping file_path -> new updated_at
            dataset_id: ID of the dataset in BigQuery
            table_id: ID of the table
            temp_bucket: Temporary bucket for JSONL uploads (uses Config if None)
        """
        if not file_updates:
            logger.info("No files to update")
            return
        
        temp_bucket = temp_bucket or Config.GCS_TEMP_BUCKET
        file_data = [
            {
                "file_path": path,
                "updated_at": normalize_timestamp(updated_at)
            }
            for path, updated_at in file_updates.items()
        ]
        
        temp_table_id = f"{table_id}_updates_tmp_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        temp_table_ref = self.client.dataset(dataset_id).table(temp_table_id)
        
        logger.info(
            f"Updating {len(file_updates)} files in BigQuery via temporary table {temp_table_id}..."
        )
        
        self._insert_via_jsonl(
            file_data,
            temp_table_ref,
            temp_bucket,
            write_disposition=WriteDisposition.WRITE_TRUNCATE
        )
        
        merge_query = f"""
        MERGE `{self.project_id}.{dataset_id}.{table_id}` T
        USING `{self.project_id}.{dataset_id}.{temp_table_id}` S
        ON T.file_path = S.file_path
        WHEN MATCHED THEN
          UPDATE SET updated_at = S.updated_at
        """
        
        try:
            job = self.client.query(merge_query)
            job.result()
            logger.info(f"✓ {len(file_updates)} files updated in BigQuery via merge")
        except GoogleCloudError as e:
            logger.error(f"Error merging updates from {temp_table_id}: {e}")
            raise
        finally:
            try:
                self.client.delete_table(temp_table_ref, not_found_ok=True)
                logger.debug(f"Temporary table deleted: {temp_table_id}")
            except Exception as cleanup_error:
                logger.warning(f"Error deleting temporary table {temp_table_id}: {cleanup_error}")
    
    def log_sync(
        self,
        dataset_id: str,
        log_table_id: str,
        sync_date: datetime,
        files_added: int,
        files_deleted: int,
        files_updated: int,
        success: bool = True,
        error_message: Optional[str] = None
    ):
        """
        Register a sync log in BigQuery.
        
        Args:
            dataset_id: ID of the dataset in BigQuery
            log_table_id: ID of the log table
            sync_date: Date/time of the sync
            files_added: Number of files added
            files_deleted: Number of files deleted
            files_updated: Number of files updated
            success: If the sync was successful
            error_message: Error message (if any)
        """
        table_ref = self.client.dataset(dataset_id).table(log_table_id)
        
        # Check if the table exists, if not, create it
        try:
            self.client.get_table(table_ref)
        except Exception:
            # Table does not exist, create it
            table = bigquery.Table(table_ref, schema=self.get_log_table_schema())
            table = self.client.create_table(table)
            logger.info(f"Log table created: {self.project_id}.{dataset_id}.{log_table_id}")
        
        # Prepare data for insertion
        log_data = {
            "sync_date": sync_date,
            "files_added": files_added,
            "files_deleted": files_deleted,
            "files_updated": files_updated,
            "success": success,
            "error_message": error_message
        }
        
        log_df = pd.DataFrame([log_data])
        
        # Insert record (append)
        job_config = LoadJobConfig(
            schema=self.get_log_table_schema(),
            write_disposition=WriteDisposition.WRITE_APPEND,
        )
        
        job = self.client.load_table_from_dataframe(log_df, table_ref, job_config=job_config)
        job.result()
        
        logger.info(f"✓ Log registered in {self.project_id}.{dataset_id}.{log_table_id}")