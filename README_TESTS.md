# AFL Pipeline Tests

This folder contains tests for the AFL data pipeline module. The tests verify that each component of the pipeline works correctly and that they work together as expected.

## Test Structure

The test suite is divided into three main files:

1. `afl_pipeline_test.py`: Tests the main `AFLDataPipeline` class and the data processing functions.
2. `afl_pipeline_managers_test.py`: Tests the `StorageManager` and `BigQueryManager` classes.
3. `run_tests.py`: A script to run all tests in one go.

## Requirements

Before running the tests, make sure you have the following dependencies installed:

```
pandas
google-cloud-storage
google-cloud-bigquery
unittest (included in Python's standard library)
```

## Running the Tests

There are two ways to run the tests:

### 1. Run all tests at once

```bash
python run_tests.py
```

This will discover and run all test files in the project.

### 2. Run individual test files

```bash
python -m unittest afl_pipeline_test.py
python -m unittest afl_pipeline_managers_test.py
```

## Test Coverage

The tests cover the following functionality:

- Initialization of the pipeline with and without service account files
- Processing a single year of data, with various options:
  - Using sample data
  - Skipping GCS upload
  - Handling failures
- Running the complete pipeline for multiple years
- Creating sample data
- Preprocessing player statistics
- Storage operations:
  - Uploading dataframes to GCS
  - Checking if blobs exist
- BigQuery operations:
  - Creating and ensuring datasets exist
  - Uploading data from GCS
  - Creating combined tables

## Mocking

The tests use Python's `unittest.mock` module to mock external dependencies, such as Google Cloud Storage and BigQuery. This ensures that:

1. Tests can run without actual Google Cloud credentials
2. Tests don't make actual API calls to Google Cloud services
3. Tests can simulate both success and failure scenarios

## Notes

- One test (`test_process_year_success`) is skipped due to import complexity. This test attempts to test fetching real data from external sources, which would require more complex mocking.
- All other tests should pass when run in the correct environment.
