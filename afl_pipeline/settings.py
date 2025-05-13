"""
Settings module for AFL Data Pipeline.
Contains configuration options for the pipeline.
"""

import os
from pathlib import Path

# Default service account file path
DEFAULT_SERVICE_ACCOUNT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'AFL_data_functions',
    'Data_Pipeline',
    'service_account_json_afl_pipeline.json'
)

# GCS bucket name
GCS_BUCKET_NAME = 'afl-data'

# Paths for GCS and BigQuery tables
GCS_PLAYER_STATS_PATH_TEMPLATE = 'player_stats/player_stats_{year}'
BIGQUERY_PLAYER_STATS_TABLE_TEMPLATE = 'player_stats_{year}_bq'

# Dataset IDs
PLAYER_STATS_DATASET_ID = 'afl_player_data'
COMBINED_STATS_DATASET_ID = 'afl_data'
COMBINED_STATS_TABLE_ID = 'combined_player_stats_bq'
