import unittest
import pandas as pd
from unittest.mock import patch, MagicMock

from afl_pipeline.storage import StorageManager
from afl_pipeline.bigquery import BigQueryManager


class TestStorageManager(unittest.TestCase):
    """Test cases for StorageManager class."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_client = MagicMock()
        self.bucket_name = "test-bucket"
        self.storage_manager = StorageManager(
            self.mock_client, self.bucket_name)

    def test_init(self):
        """Test initialization."""
        self.assertEqual(self.storage_manager.client, self.mock_client)
        self.assertEqual(self.storage_manager.bucket_name, self.bucket_name)

    def test_upload_dataframe_success(self):
        """Test uploading a dataframe successfully."""
        # Create a test dataframe
        df = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': ['a', 'b', 'c']
        })

        # Set up mocks
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        self.mock_client.get_bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        # Call the method
        result = self.storage_manager.upload_dataframe(df, 'test/path.csv')

        # Assertions
        self.assertTrue(result)
        self.mock_client.get_bucket.assert_called_once_with(self.bucket_name)
        mock_bucket.blob.assert_called_once_with('test/path.csv')
        mock_blob.upload_from_string.assert_called_once()

    def test_upload_dataframe_exception(self):
        """Test uploading a dataframe with an exception."""
        df = pd.DataFrame({'col1': [1, 2, 3]})

        # Make the get_bucket method raise an exception
        self.mock_client.get_bucket.side_effect = Exception("Test error")

        # Call the method
        result = self.storage_manager.upload_dataframe(df, 'test/path.csv')

        # Assertions
        self.assertFalse(result)
        self.mock_client.get_bucket.assert_called_once_with(self.bucket_name)

    def test_check_blob_exists_true(self):
        """Test checking if a blob exists when it does."""
        # Set up mocks
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        self.mock_client.get_bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        # Call the method
        result = self.storage_manager.check_blob_exists('test/path.csv')

        # Assertions
        self.assertTrue(result)
        self.mock_client.get_bucket.assert_called_once_with(self.bucket_name)
        mock_bucket.blob.assert_called_once_with('test/path.csv')
        mock_blob.exists.assert_called_once()

    def test_check_blob_exists_false(self):
        """Test checking if a blob exists when it doesn't."""
        # Set up mocks
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        self.mock_client.get_bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        # Call the method
        result = self.storage_manager.check_blob_exists('test/path.csv')

        # Assertions
        self.assertFalse(result)
        self.mock_client.get_bucket.assert_called_once_with(self.bucket_name)
        mock_bucket.blob.assert_called_once_with('test/path.csv')
        mock_blob.exists.assert_called_once()


class TestBigQueryManager(unittest.TestCase):
    """Test cases for BigQueryManager class."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_client = MagicMock()
        self.mock_client.project = "test-project"
        self.bigquery_manager = BigQueryManager(self.mock_client)

    def test_init(self):
        """Test initialization."""
        self.assertEqual(self.bigquery_manager.client, self.mock_client)

    @patch('google.cloud.bigquery.Dataset')
    def test_ensure_dataset_exists_already_exists(self, mock_dataset_class):
        """Test ensuring a dataset exists when it already does."""
        dataset_id = "test_dataset"

        # Set up mocks
        self.mock_client.get_dataset.return_value = MagicMock()

        # Call the method
        result = self.bigquery_manager.ensure_dataset_exists(dataset_id)

        # Assertions
        self.assertTrue(result)
        self.mock_client.get_dataset.assert_called_once_with(dataset_id)
        self.mock_client.create_dataset.assert_not_called()

    @patch('google.cloud.bigquery.Dataset')
    def test_ensure_dataset_exists_create_new(self, mock_dataset_class):
        """Test ensuring a dataset exists when it doesn't."""
        dataset_id = "test_dataset"

        # Set up mocks
        from google.cloud.exceptions import NotFound
        self.mock_client.get_dataset.side_effect = NotFound("Not found")
        mock_dataset = MagicMock()
        mock_dataset_class.return_value = mock_dataset

        # Call the method
        result = self.bigquery_manager.ensure_dataset_exists(dataset_id)

        # Assertions
        self.assertTrue(result)
        self.mock_client.get_dataset.assert_called_once_with(dataset_id)
        mock_dataset_class.assert_called_once_with(
            f"test-project.{dataset_id}")
        self.mock_client.create_dataset.assert_called_once_with(mock_dataset)

    @patch('google.cloud.bigquery.LoadJobConfig')
    def test_upload_from_gcs(self, mock_job_config_class):
        """Test uploading from GCS to BigQuery."""
        # Set up mocks
        mock_job_config = MagicMock()
        mock_job_config_class.return_value = mock_job_config
        mock_job = MagicMock()
        self.mock_client.load_table_from_uri.return_value = mock_job

        # Make ensure_dataset_exists patch
        with patch.object(self.bigquery_manager, 'ensure_dataset_exists', return_value=True) as mock_ensure:
            # Call the method
            gcs_uri = "gs://test-bucket/test/path.csv"
            table_id = "test_table"
            dataset_id = "test_dataset"
            result = self.bigquery_manager.upload_from_gcs(
                gcs_uri, table_id, dataset_id)

            # Assertions
            self.assertTrue(result)
            mock_ensure.assert_called_once_with(dataset_id)
            self.mock_client.load_table_from_uri.assert_called_once_with(
                gcs_uri,
                f"test-project.{dataset_id}.{table_id}",
                job_config=mock_job_config
            )
            mock_job.result.assert_called_once()

    def test_create_combined_table(self):
        """Test creating a combined table from multiple sources."""
        # Set up mocks
        mock_query_job = MagicMock()
        self.mock_client.query.return_value = mock_query_job

        # Make ensure_dataset_exists patch
        with patch.object(self.bigquery_manager, 'ensure_dataset_exists', return_value=True) as mock_ensure:
            # Call the method
            years = [2019, 2020, 2021]
            result = self.bigquery_manager.create_combined_table(years)

            # Assertions
            self.assertTrue(result)
            mock_ensure.assert_called_once()
            self.mock_client.query.assert_called_once()
            mock_query_job.result.assert_called_once()

    def test_create_combined_table_no_years(self):
        """Test creating a combined table with no years provided."""
        # Call the method with empty years list
        result = self.bigquery_manager.create_combined_table([])

        # Assertions
        self.assertFalse(result)
        self.mock_client.query.assert_not_called()


if __name__ == '__main__':
    unittest.main()
