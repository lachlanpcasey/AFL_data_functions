from google.cloud import bigquery, storage
from google.api_core import retry
from google.cloud.exceptions import NotFound
from config import gcs_bucket_name, service_account_file


def upload_player_stats_to_bigquery(year):
    # Set up GCS client object
    storage_client = storage.Client.from_service_account_json(
        service_account_file)

    # Instantiate a client object
    client = bigquery.Client.from_service_account_json(service_account_file)

    # Construct GCS URI for the file to be loaded
    gcs_uri = f'gs://{gcs_bucket_name}/player_stats/player_stats_{year}'

    # Construct table ID for BigQuery table
    table_id = f'player_stats_{year}_bq'

    # Construct reference to the BigQuery dataset
    dataset_ref = client.dataset('afl_player_data')

    # Define schema
    target_schema_player_stats = [
        bigquery.SchemaField('Season', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('Round', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('Local_start_time', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('Venue', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('Attendance', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('Home_team', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('HQ1G', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('HQ1B', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('HQ2G', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('HQ2B', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('HQ3G', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('HQ3B', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('HQ4G', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('HQ4B', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('Home_score', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('Away_team', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('AQ1G', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('AQ1B', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('AQ2G', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('AQ2B', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('AQ3G', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('AQ3B', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('AQ4G', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('AQ4B', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('Away_score', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('First_name', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('Surname', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('ID', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('Jumper_No_', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('Playing_for', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('Kicks', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('Marks', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('Handballs', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('Goals', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('Behinds', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('Hit_Outs', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('Tackles', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('Rebounds', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('Inside_50s', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('Clearances', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('Clangers', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('Frees_For', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('Frees_Against', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('Brownlow_Votes', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('Contested_Possessions',
                             'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('Uncontested_Possessions',
                             'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('Contested_Marks', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('Marks_Inside_50', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('One_Percenters', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('Bounces', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('Goal_Assists', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('Time_on_Ground__', 'FLOAT', mode='NULLABLE'),
        bigquery.SchemaField('Substitute', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('Umpire_1', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('Umpire_2', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('Umpire_3', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('Umpire_4', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('group_id', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('year', 'INTEGER', mode='NULLABLE')
    ]

    # Check if table exists, delete it if it does
    table_ref = dataset_ref.table(table_id)
    table = None
    try:
        client.delete_table(table_ref)
        print(
            f'Table {table_id} already exists in {table_ref.dataset_id}. Deleted existing table.')
    except NotFound:
        pass

    # Create the table with the specified schema
    table = bigquery.Table(table_ref, schema=target_schema_player_stats)
    table = client.create_table(table)
    print(f'Created table {table.table_id} in {table_ref.dataset_id}.')

    # Construct job configuration
    job_config = bigquery.LoadJobConfig(
        schema=target_schema_player_stats,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        field_delimiter=','
    )

    # Construct the BigQuery load job
    load_job = client.load_table_from_uri(
        gcs_uri,
        dataset_ref.table(table_id),
        job_config=job_config
    )

    # Wait for the load job to finish
    load_job.result()

    # Print message upon completion
    print(f'Table {table_id} successfully created and data loaded in BigQuery.')
