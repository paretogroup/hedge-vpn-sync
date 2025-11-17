#!/usr/bin/env python3
"""
Main script for synchronization of files from the VPN with BigQuery and GCS.
Usage:
    python main.py [--dry-run] [--log-level LEVEL]
"""
import argparse
import logging
import sys
from pathlib import Path

from vpn.config import Config
from vpn.sync import VPNSynchronizer


def setup_logging(log_level: str = "INFO"):
    """
    Configure the logging system.
    
    Args:
        log_level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {log_level}')
    
    logging.basicConfig(
        level=numeric_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description='Sincroniza arquivos da VPN com BigQuery e GCS',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Execute normal synchronization
  python main.py
  
  # Dry-run mode (only show what would be done)
  python main.py --dry-run
  
  # Detailed log level
  python main.py --log-level DEBUG
        """
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Only show what would be done without making changes'
    )
    
    parser.add_argument(
        '--log-level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Log level (default: INFO)'
    )
    
    args = parser.parse_args()
    
    # Configure logging
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)
    
    # Validate settings
    logger.info("Validating settings...")
    errors = Config.validate()
    if errors:
        logger.error("Configuration errors found:")
        for error in errors:
            logger.error(f"  - {error}")
        sys.exit(1)
    
    logger.info("Settings validated successfully")
    logger.info(f"VPN Base Path: {Config.VPN_BASE_PATH}")
    logger.info(f"GCS Bucket: {Config.GCS_BUCKET_NAME}")
    logger.info(f"BigQuery Dataset: {Config.BIGQUERY_DATASET_ID}")
    logger.info(f"BigQuery Table: {Config.BIGQUERY_TABLE_ID}")
    
    # Initialize synchronizer
    try:
        synchronizer = VPNSynchronizer()
    except Exception as e:
        logger.error(f"Error initializing synchronizer: {e}")
        sys.exit(1)
    
    # Execute synchronization
    try:
        result = synchronizer.sync(dry_run=args.dry_run)
        
        if result["success"]:
            logger.info("Synchronization completed successfully!")
            logger.info(f"  - Files added: {result['files_added']}")
            logger.info(f"  - Files deleted: {result['files_deleted']}")
            logger.info(f"  - Files updated: {result['files_updated']}")
            sys.exit(0)
        else:
            logger.error(f"Synchronization failed: {result.get('error_message', 'Unknown error')}")
            sys.exit(1)
    
    except KeyboardInterrupt:
        logger.warning("Synchronization interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Unexpected error during synchronization: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()