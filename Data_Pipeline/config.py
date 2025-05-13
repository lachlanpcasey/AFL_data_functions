import os

# Path to the current file
current_dir = os.path.dirname(os.path.abspath(__file__))

gcs_bucket_name = 'afl-data'
service_account_file = os.path.join(
    current_dir, 'service_account_json_afl_pipeline.json')
