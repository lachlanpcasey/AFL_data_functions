"""
Main pipeline module for AFL Data Pipeline.
Coordinates the entire data pipeline process.
"""

import os
import logging
import sys
from google.cloud import storage, bigquery

from .storage import StorageManager
from .bigquery import BigQueryManager
from .data_processor import preprocess_player_stats, create_sample_data
from . import settings
from Data_Pipeline.Functions.get_data_functions import func_initialise, fetch_player_stats

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('afl_pipeline.pipeline')


class AFLDataPipeline:
    """Main class for coordinating the AFL data pipeline."""

    def __init__(self, service_account_file=None, gcs_bucket_name=None):
        """
        Initialize the AFL Data Pipeline.

        Args:
            service_account_file: Path to the service account file for authentication
            gcs_bucket_name: Name of the GCS bucket to use
        """
        # Use values from settings or override with provided values
        self.bucket_name = gcs_bucket_name or settings.GCS_BUCKET_NAME
        service_account_path = service_account_file or settings.DEFAULT_SERVICE_ACCOUNT_PATH

        # Handle authentication
        if service_account_path:
            if not os.path.exists(service_account_path):
                logger.error(
                    f"Service account file not found: {service_account_path}")
                raise FileNotFoundError(
                    f"Service account file not found: {service_account_path}")

            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_account_path
            logger.info(f"Using service account file: {service_account_path}")
        else:
            logger.info("Using default application credentials")

        # Initialize Google Cloud clients
        try:
            self.storage_client = storage.Client()
            self.bigquery_client = bigquery.Client()

            # Initialize managers
            self.storage = StorageManager(
                self.storage_client, self.bucket_name)
            self.bigquery = BigQueryManager(self.bigquery_client)

            logger.info(
                f"AFLDataPipeline initialized with bucket: {self.bucket_name}")
        except Exception as e:
            logger.error(f"Error initializing AFLDataPipeline: {str(e)}")
            raise

    def process_year(self, year, use_sample_data=False, skip_gcs=False):
        """
        Process a single year of data.

        Args:
            year: Year to process
            use_sample_data: Whether to use sample data instead of fetching real data
            skip_gcs: Whether to skip the GCS upload step

        Returns:
            bool: Success status
        """
        logger.info(f"Processing year {year}")
        try:
            gcs_path = settings.GCS_PLAYER_STATS_PATH_TEMPLATE.format(
                year=year)

            # Step 1: Upload to GCS (if not skipped)
            if not skip_gcs:
                if use_sample_data:
                    # Create sample data
                    logger.info(f"Using sample data for year {year}")
                    df = create_sample_data(year)
                else:
                    # Import fitzRoy functions here to avoid circular imports
                    logger.info(f"Fetching real data for year {year}")

                    # Add parent directory to path to find Functions module
                    parent_dir = os.path.dirname(
                        os.path.dirname(os.path.abspath(__file__)))
                    if parent_dir not in sys.path:
                        sys.path.append(parent_dir)

                    # Import the function to fetch player stats
                    try:
                        df = fetch_player_stats(
                            season=year, source='afltables')
                    except ImportError:
                        logger.error(
                            "Could not import fetch_player_stats. Make sure AFL_data_functions is in your Python path.")
                        return False

                # Preprocess data before uploading
                df = preprocess_player_stats(df, year)

                # Upload to GCS
                gcs_success = self.storage.upload_dataframe(df, gcs_path)
                if not gcs_success:
                    logger.error(
                        f"Failed to upload data to GCS for year {year}")
                    return False

            # Step 2: Upload from GCS to BigQuery
            gcs_uri = f'gs://{self.bucket_name}/{gcs_path}'
            table_id = settings.BIGQUERY_PLAYER_STATS_TABLE_TEMPLATE.format(
                year=year)

            bq_success = self.bigquery.upload_from_gcs(
                gcs_uri,
                table_id,
                dataset_id=settings.PLAYER_STATS_DATASET_ID
            )
            if not bq_success:
                logger.error(
                    f"Failed to upload data to BigQuery for year {year}")
                return False

            logger.info(f"Successfully processed year {year}")
            return True
        except Exception as e:
            logger.error(f"Error processing year {year}: {str(e)}")
            return False

    def run_pipeline(self, years, use_sample_data=False, skip_gcs=False, create_combined=True):
        """
        Run the complete data pipeline for multiple years.

        Args:
            years: List of years to process
            use_sample_data: Whether to use sample data
            skip_gcs: Whether to skip the GCS upload step
            create_combined: Whether to create a combined table

        Returns:
            list: List of successfully processed years
        """
        # Initialise the R environment
        logger.info("Initialising R environment")

        func_initialise()

        logger.info(f"Running pipeline for years: {years}")

        # Process each year
        successful_years = []
        for year in years:
            success = self.process_year(year, use_sample_data, skip_gcs)
            if success:
                successful_years.append(year)

        # Create combined table if requested
        if create_combined and successful_years:
            logger.info("Creating combined table")
            self.bigquery.create_combined_table(
                successful_years,
                source_dataset_id=settings.PLAYER_STATS_DATASET_ID,
                destination_dataset_id=settings.COMBINED_STATS_DATASET_ID,
                destination_table=settings.COMBINED_STATS_TABLE_ID
            )

        logger.info(
            f"Pipeline completed. Successfully processed years: {successful_years}")
        return successful_years
