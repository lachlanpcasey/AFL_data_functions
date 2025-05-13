"""
BigQuery module for AFL Data Pipeline.
Handles all Google BigQuery operations.
"""

import logging
from google.cloud.exceptions import NotFound
from google.cloud import bigquery

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('afl_pipeline.bigquery')


class BigQueryManager:
    """Manages interactions with Google BigQuery."""

    def __init__(self, client):
        """
        Initialize the BigQuery Manager.

        Args:
            client: Google BigQuery client
        """
        self.client = client
        logger.info("BigQueryManager initialized")

    def ensure_dataset_exists(self, dataset_id):
        """
        Check if a dataset exists, create it if it doesn't.

        Args:
            dataset_id: ID of the dataset to check/create

        Returns:
            bool: Success status
        """
        try:
            dataset_ref = f"{self.client.project}.{dataset_id}"
            try:
                self.client.get_dataset(dataset_id)
                logger.info(f"Dataset {dataset_id} already exists")
            except NotFound:
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = "US"  # Change if needed
                self.client.create_dataset(dataset)
                logger.info(f"Created dataset {dataset_id}")
            return True
        except Exception as e:
            logger.error(f"Error ensuring dataset exists: {str(e)}")
            return False

    def upload_from_gcs(self, gcs_uri, table_id, dataset_id='afl_player_data'):
        """
        Load data from GCS to BigQuery.

        Args:
            gcs_uri: URI of the GCS file to load
            table_id: ID of the target table
            dataset_id: ID of the dataset to use (default: afl_player_data)

        Returns:
            bool: Success status
        """
        try:
            # Ensure dataset exists
            self.ensure_dataset_exists(dataset_id)

            # Construct the full table reference
            full_table_id = f"{self.client.project}.{dataset_id}.{table_id}"

            # Configure the load job
            job_config = bigquery.LoadJobConfig(
                autodetect=True,  # Let BigQuery autodetect schema
                skip_leading_rows=1,
                source_format=bigquery.SourceFormat.CSV,
                field_delimiter=',',
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # Overwrite if exists
            )

            # Start the load job
            logger.info(
                f"Starting load job for {full_table_id} from {gcs_uri}...")
            load_job = self.client.load_table_from_uri(
                gcs_uri,
                full_table_id,
                job_config=job_config
            )

            # Wait for job to complete
            load_job.result()
            logger.info(
                f"Table {full_table_id} successfully created and data loaded.")
            return True
        except Exception as e:
            logger.error(f"Error uploading to BigQuery: {str(e)}")
            return False

    def create_combined_table(self, years, source_dataset_id='afl_player_data',
                              destination_dataset_id='afl_data',
                              table_prefix='player_stats_',
                              suffix='_bq',
                              destination_table='combined_player_stats_bq'):
        """
        Create a combined table from multiple years of data.

        Args:
            years: List of years to combine
            source_dataset_id: Source dataset ID (default: afl_player_data)
            destination_dataset_id: Destination dataset ID (default: afl_data)
            table_prefix: Prefix for source tables (default: player_stats_)
            suffix: Suffix for source tables (default: _bq)
            destination_table: Name of the destination table (default: combined_player_stats_bq)

        Returns:
            bool: Success status
        """
        try:
            # Ensure destination dataset exists
            self.ensure_dataset_exists(destination_dataset_id)

            # Construct source table references
            source_tables = [
                f"{self.client.project}.{source_dataset_id}.{table_prefix}{year}{suffix}"
                for year in years
            ]

            if not source_tables:
                logger.error("No source tables provided")
                return False

            # Construct destination table reference
            destination_table_ref = f"{self.client.project}.{destination_dataset_id}.{destination_table}"

            # Construct query to combine tables
            union_clauses = ""
            if len(source_tables) > 1:
                union_clauses = " UNION ALL ".join(
                    [f"SELECT * FROM `{table}`" for table in source_tables[1:]])

            query = f"""
            CREATE OR REPLACE TABLE `{destination_table_ref}` AS
            SELECT * FROM `{source_tables[0]}`
            {f" UNION ALL {union_clauses}" if union_clauses else ""}
            """

            # Execute query
            logger.info(
                f"Running query to combine tables into {destination_table_ref}...")
            query_job = self.client.query(query)
            query_job.result()

            logger.info(
                f"Combined table '{destination_table_ref}' created with data from: {', '.join(source_tables)}")
            return True
        except Exception as e:
            logger.error(f"Error creating combined table: {str(e)}")
            return False
