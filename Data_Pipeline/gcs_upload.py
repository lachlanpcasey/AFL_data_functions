import os
from google.cloud import storage
from Functions.get_data_functions import fetch_player_stats
from config import service_account_file, gcs_bucket_name
import time


def upload_dataframe_to_gcs(service_account_file, bucket_name, file_name, dataframe):
    # Set Google Application Credentials environment variable
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_account_file

    # Convert DataFrame to CSV string
    csv_data = dataframe.to_csv(index=False)

    # Initialize the GCS client
    client = storage.Client()

    # Get the GCS bucket
    bucket = client.get_bucket(bucket_name)

    # Upload the CSV data to the bucket
    blob = bucket.blob(file_name)
    blob.upload_from_string(csv_data, content_type='text/csv')

    print(
        f"DataFrame uploaded as CSV to '{file_name}' in '{bucket_name}' bucket successfully.")


def player_data_upload_to_gcs(year: int):
    df = fetch_player_stats(season=year, source='afltables')
    df['year'] = year
    df['Season'].astype('float')
    df['Round'].astype('str')
    df['Local.start.time'].astype('int')
    df['Attendance'].astype('float')
    df['ID'].astype('int')
    df['Jumper.No.'].astype('str')
    # Drop the 'Date' column from the DataFrame
    df = df.drop('Date', axis=1)
    upload_dataframe_to_gcs(
        service_account_file, gcs_bucket_name, f'player_stats/player_stats_{year}', df)
