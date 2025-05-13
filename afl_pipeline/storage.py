"""
Storage module for AFL Data Pipeline.
Handles all Google Cloud Storage (GCS) operations.
"""

import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('afl_pipeline.storage')


class StorageManager:
    """Manages interactions with Google Cloud Storage."""

    def __init__(self, client, bucket_name):
        """
        Initialize the Storage Manager.

        Args:
            client: Google Cloud Storage client
            bucket_name: Name of the GCS bucket to use
        """
        self.client = client
        self.bucket_name = bucket_name
        logger.info(f"StorageManager initialized with bucket: {bucket_name}")

    def upload_dataframe(self, dataframe, file_path):
        """
        Upload a pandas DataFrame to GCS as a CSV file.

        Args:
            dataframe: pandas DataFrame to upload
            file_path: Path within the bucket to store the file

        Returns:
            bool: Success status
        """
        try:
            # Convert DataFrame to CSV string
            csv_data = dataframe.to_csv(index=False)

            # Get the GCS bucket
            bucket = self.client.get_bucket(self.bucket_name)

            # Upload the CSV data to the bucket
            blob = bucket.blob(file_path)
            blob.upload_from_string(csv_data, content_type='text/csv')

            logger.info(
                f"DataFrame uploaded as CSV to '{file_path}' in '{self.bucket_name}' bucket successfully.")
            return True
        except Exception as e:
            logger.error(f"Error uploading to GCS: {str(e)}")
            return False

    def check_blob_exists(self, file_path):
        """
        Check if a blob exists in the bucket.

        Args:
            file_path: Path to the blob

        Returns:
            bool: True if exists, False otherwise
        """
        try:
            bucket = self.client.get_bucket(self.bucket_name)
            blob = bucket.blob(file_path)
            return blob.exists()
        except Exception as e:
            logger.error(f"Error checking if blob exists: {str(e)}")
            return False
