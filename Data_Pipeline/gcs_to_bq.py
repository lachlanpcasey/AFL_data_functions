from config import gcs_bucket_name, service_account_file
import os
import sys
from google.cloud import bigquery, storage
from google.api_core import retry
from google.cloud.exceptions import NotFound

# Add the current directory to path to find config
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

# Set environment variable for authentication
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_account_file

# Print authentication information for debugging
print(f"Using service account file: {service_account_file}")
print(f"File exists: {os.path.exists(service_account_file)}")


def upload_player_stats_to_bigquery(year):
    # Set up GCS client object
    storage_client = storage.Client()  # Use default credentials from environment

    # Instantiate a client object
    client = bigquery.Client()  # Use default credentials from environment

    # Construct GCS URI for the file to be loaded
    gcs_uri = f'gs://{gcs_bucket_name}/player_stats/player_stats_{year}'

    # Construct table ID for BigQuery table
    dataset_id = 'afl_player_data'
    table_id = f'{client.project}.{dataset_id}.player_stats_{year}_bq'

    # Construct job configuration
    job_config = bigquery.LoadJobConfig(
        autodetect=True,  # Let BigQuery autodetect schema
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        field_delimiter=',',
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # Overwrite if exists
    )

    # Construct the BigQuery load job
    load_job = client.load_table_from_uri(
        gcs_uri,
        table_id,
        job_config=job_config
    )

    # Start the load job
    print(f'Starting load job for {table_id}...')

    try:
        # Wait for the load job to finish
        load_job.result()
        print(
            f'Table {table_id} successfully created and data loaded in BigQuery.')
    except Exception as e:
        print(f'Error loading data: {str(e)}')


def create_combined_player_stats_table(years):
    # Instantiate the client
    client = bigquery.Client()

    # Define the source dataset and tables
    source_dataset_id = 'afl_player_data'
    source_tables = [
        f'{client.project}.{source_dataset_id}.player_stats_{year}_bq' for year in years]

    # Define the destination dataset and table for the combined data
    destination_dataset_id = 'afl_data'
    destination_table_id = f'{client.project}.{destination_dataset_id}.combined_player_stats_bq'

    try:
        # Check if destination dataset exists, create if it doesn't
        try:
            client.get_dataset(destination_dataset_id)
        except NotFound:
            dataset = bigquery.Dataset(
                f'{client.project}.{destination_dataset_id}')
            dataset.location = "US"  # Change if needed
            client.create_dataset(dataset)
            print(f"Created dataset {destination_dataset_id}")

        # Construct the query to union the data from the source tables
        query = f"""
        CREATE OR REPLACE TABLE `{destination_table_id}` AS
        SELECT *
        FROM `{source_tables[0]}` 
        { 'UNION ALL '.join([f"SELECT * FROM `{table}`" for table in source_tables[1:]]) if len(source_tables) > 1 else '' }
        """

        # Start the query job
        print(f"Running query to combine tables...")
        query_job = client.query(query)

        # Wait for the query job to complete
        query_job.result()

        print(
            f"Combined table '{destination_table_id}' created with data from tables: {', '.join(source_tables)}")

    except Exception as e:
        print(f"Error creating combined table: {str(e)}")
