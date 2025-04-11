# Reddit ETL Pipeline - Test Runner

This document provides instructions on how to use the `test_run.py` script to test the Reddit ETL pipeline with a small subsample of data from BigQuery.

## Overview

The `test_run.py` script follows the same flow as `simple_run.py` but:

1. Extracts a limited subsample of data from BigQuery (not directly from Reddit)
2. Processes this data through the entire ETL pipeline
3. Optionally uses a test dataset in BigQuery for isolation
4. Uses a separate Firestore collection for state management to avoid affecting production timestamps

This approach allows you to test the pipeline with real data but in a controlled and isolated way.

## Prerequisites

Before running the test script, make sure you have:

1. Python 3.8+ installed
2. Google Cloud SDK installed and configured
3. Required Python packages installed (see `requirements.txt`)
4. Appropriate Google Cloud credentials set up
5. Data already available in BigQuery tables (raw_messages)

## Setting Up Google Cloud Credentials

To authenticate with Google Cloud services:

```bash
# Set up application default credentials
gcloud auth application-default login

# Set your project ID
gcloud config set project alex-stocks
```

## Running the Test Script

The `test_run.py` script provides a way to run the pipeline with a limited dataset:

```bash
# Run with default settings (100 records from BigQuery)
python test_run.py

# Limit the number of records to retrieve
python test_run.py --data-limit 50

# Specify time range (in hours)
python test_run.py --hours-back 48

# Use test dataset instead of production
python test_run.py --test-dataset

# Skip certain steps
python test_run.py --skip-extraction --skip-analysis
```

## Command Line Arguments

The script supports the following command line arguments:

- `--data-limit`: Maximum number of records to retrieve from BigQuery (default: 100)
- `--hours-back`: Hours to look back for data if no last run timestamp exists (default: 24)
- `--test-dataset`: Use test dataset instead of production
- `--production-state`: Use production state collection (WARNING: affects production timestamps)
- `--skip-extraction`: Skip the extraction step
- `--skip-analysis`: Skip the analysis step
- `--skip-stock-persistence`: Skip saving stock mentions
- `--skip-aggregation`: Skip the aggregation step
- `--skip-aggregation-persistence`: Skip saving aggregated data

## Pipeline Stages

The test runner follows the same ETL pipeline as the production version:

1. **Extraction**: Retrieves a limited number of Reddit posts/comments from BigQuery
2. **Analysis**: Identifies stock mentions and analyzes sentiment
3. **Stock Persistence**: Saves stock mentions to BigQuery
4. **Aggregation**: Creates daily, hourly, and weekly summaries
5. **Aggregation Persistence**: Saves summaries to BigQuery

You can skip any of these stages using the command-line arguments to test specific parts of the pipeline.

## How It Works

The test script uses several isolation mechanisms to protect your production environment:

1. **Data Limiting**: It uses monkey patching to override the `get_reddit_data` method in the `BigQueryExtractor` class, allowing it to limit the amount of data retrieved from BigQuery.

2. **Separate State Collection**: By default, it uses a separate Firestore collection (`pipeline_state_test`) for state management instead of the production collection (`pipeline_state`). This ensures that your testing doesn't affect production timestamps.

3. **Test Dataset**: When using the `--test-dataset` flag, it uses a separate BigQuery dataset for storage.

For testing and debugging the hourly timestamp issue specifically, you can also use the `test_hourly_persistence.py` script, which directly tests the hourly aggregation and persistence components.

## Important Safety Note

**NEVER use the `--production-state` flag unless you specifically want to update production timestamps.**

By default, the test script uses a separate Firestore collection for state management to avoid affecting production timestamps. Using the `--production-state` flag would cause the script to update the real production timestamps, which could disrupt your production ETL runs.

## Test Dataset

When using the `--test-dataset` flag, the script creates a separate dataset (`reddit_stocks_test`) in BigQuery to avoid affecting your production data.

## Troubleshooting

### Common Issues

1. **Authentication Errors**: Make sure you have set up Google Cloud credentials correctly.
   ```
   gcloud auth application-default login
   ```

2. **Missing Dependencies**: Install required packages:
   ```
   pip install -r requirements.txt
   ```

3. **BigQuery Schema Errors**: If you encounter schema errors, you may need to update the schema in `src/utils/bigquery_utils.py`.

4. **Timestamp Format Errors**: If you encounter timestamp format errors, check the `to_dict()` methods in the model classes.

### Debugging

For more detailed logging, you can modify the logging level in `test_run.py`:

```python
logging.basicConfig(
    level=logging.DEBUG,  # Change from INFO to DEBUG
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
```

## Next Steps

After successfully running the test script, you can:

1. Run the full pipeline in production
2. Deploy the pipeline to Google Cloud Run
3. Set up scheduled runs using Cloud Scheduler 