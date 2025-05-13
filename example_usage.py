#!/usr/bin/env python
"""
Example script for using the AFL Data Pipeline.
This script demonstrates how to fetch, process, and store AFL player statistics.
"""

import os
import sys
import logging
from afl_pipeline.pipeline import AFLDataPipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('example_usage')


def main():
    """Run the example pipeline."""
    # Path to the service account file
    service_account_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        'Data_Pipeline',
        'service_account_json_afl_pipeline.json'
    )

    # Set up the pipeline (using custom bucket name if needed)
    pipeline = AFLDataPipeline(
        service_account_file=service_account_path,
        gcs_bucket_name='afl-data'
    )

    # EXAMPLE 1: Process a single year using sample data
    logger.info("EXAMPLE 1: Processing a single year with sample data")
    success = pipeline.process_year(
        year=2022,
        use_sample_data=True
    )
    logger.info(
        f"Processing 2022 with sample data {'succeeded' if success else 'failed'}")

    # EXAMPLE 2: Process a single year with real data
    logger.info("EXAMPLE 2: Processing a single year with real data")
    try:
        success = pipeline.process_year(
            year=2021,
            use_sample_data=False
        )
        logger.info(
            f"Processing 2021 with real data {'succeeded' if success else 'failed'}")
    except ImportError:
        logger.error(
            "Could not import fetch_player_stats. This example requires the AFL_data_functions package.")

    # EXAMPLE 3: Skip GCS upload (if data is already in GCS)
    logger.info("EXAMPLE 3: Processing a year but skipping GCS upload")
    success = pipeline.process_year(
        year=2020,
        skip_gcs=True
    )
    logger.info(
        f"Processing 2020 (skip GCS) {'succeeded' if success else 'failed'}")

    # EXAMPLE 4: Process multiple years and create a combined table
    logger.info("EXAMPLE 4: Processing multiple years")
    years_to_process = [2018, 2019, 2020]

    # Use sample data for this example
    processed_years = pipeline.run_pipeline(
        years=years_to_process,
        use_sample_data=True,
        create_combined=True
    )

    logger.info(
        f"Successfully processed {len(processed_years)} out of {len(years_to_process)} years")
    logger.info(f"Processed years: {processed_years}")


if __name__ == "__main__":
    main()
