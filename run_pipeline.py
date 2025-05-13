from Data_Pipeline.config import gcs_bucket_name, service_account_file
import os
from afl_pipeline.pipeline import AFLDataPipeline
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_account_file
pipeline = AFLDataPipeline(
    service_account_file=service_account_file,
    gcs_bucket_name=gcs_bucket_name
)
pipeline.run_pipeline(years=[2025])
