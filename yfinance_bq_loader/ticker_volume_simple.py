import yfinance as yf
import pandas as pd
import logging
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from tqdm import tqdm
import time
import io
import os # Added for dotenv
from dotenv import load_dotenv # Added for dotenv
from typing import List, Dict, Any, Set
import re
from datetime import datetime
# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# BigQuery settings from environment variables
PROJECT_ID = os.getenv('PROJECT_ID', 'default-project-id') # Provide default if not set
DATASET_ID = os.getenv('DATASET_ID', 'default-dataset-id')
SOURCE_TABLE_ID = os.getenv('SOURCE_TABLE_ID', 'default-source-table')
TARGET_TABLE_ID = os.getenv('TARGET_TABLE_ID', 'default-target-table') 

# Script settings from environment variables
HISTORY_PERIOD = os.getenv('HISTORY_PERIOD', '1y') 
# Load BATCH_SIZE as integer, with default
try:
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100')) 
except ValueError:
    logger.warning(f"Invalid BATCH_SIZE in .env file. Using default value 100.")
    BATCH_SIZE = 100

def load_tickers_from_bigquery(client: bigquery.Client, project_id: str, dataset_id: str, table_id: str) -> list[str]:
    """Load unique ticker symbols from a BigQuery table."""
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    logger.info(f"Loading tickers from BigQuery table: {table_ref}")
    
    try:
        # Updated query to match original validation logic
        query = f"""
            SELECT DISTINCT ticker 
            FROM `{table_ref}`
            WHERE ticker IS NOT NULL AND LENGTH(ticker) BETWEEN 1 AND 5 AND REGEXP_CONTAINS(ticker, r'^[A-Z]+$')
        """
        query_job = client.query(query)
        results = query_job.result()
        tickers = [row.ticker for row in results]
        logger.info(f"Loaded {len(tickers)} unique valid tickers from BigQuery")
        return tickers
    except NotFound:
        logger.error(f"Source table {table_ref} not found.")
        return []
    except Exception as e:
        logger.error(f"Error loading tickers from BigQuery: {e}")
        return []

def fetch_stock_data(tickers: list[str], period: str) -> pd.DataFrame:
    """Fetch historical stock data for a list of tickers using yfinance."""
    if not tickers:
        logger.warning("No tickers provided to fetch_stock_data.")
        return pd.DataFrame()

    logger.info(f"Fetching {period} historical data for {len(tickers)} tickers...")
    
    try:
        # Set auto_adjust=False to ensure 'Adj Close' column is included
        data = yf.download(tickers, period=period, progress=False, threads=True, auto_adjust=False)
        
        if data.empty:
            logger.warning(f"No data returned by yfinance for tickers: {tickers}")
            return pd.DataFrame()

        # Handle the case where only one ticker was downloaded successfully
        if isinstance(data.columns, pd.Index) and not isinstance(data.columns, pd.MultiIndex):
             if len(tickers) == 1:
                 data['Ticker'] = tickers[0]
                 data.reset_index(inplace=True)
                 # Ensure column names are consistent
                 data.rename(columns={'Adj Close': 'Adj_Close'}, inplace=True)
                 # Ensure 'Date' is present after reset_index
                 if 'Date' not in data.columns and 'index' in data.columns:
                      data.rename(columns={'index': 'Date'}, inplace=True)
                 elif 'Date' not in data.columns and data.index.name == 'Date':
                     data.reset_index(inplace=True) # Try resetting index again if 'Date' was the index name

                 # Check final columns before returning
                 required_cols = ['Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Adj_Close', 'Volume']
                 if all(col in data.columns for col in required_cols):
                    return data[required_cols]
                 else:
                    logger.warning(f"Missing required columns for single ticker {tickers[0]}. Columns found: {data.columns}")
                    return pd.DataFrame()

             else:
                 logger.warning(f"yfinance returned single-index data for multiple tickers: {tickers}. This usually means most failed. Returning empty DataFrame.")
                 return pd.DataFrame()

        # If multiple tickers are successful, stack the multi-index columns ('Open', 'High', ..., 'Ticker')
        data = data.stack(level=1) 
        data.index.names = ['Date', 'Ticker'] # Name the index levels
        data.reset_index(inplace=True) # Convert index levels to columns
        
        # Rename columns for BigQuery compatibility (no spaces)
        data.rename(columns={'Adj Close': 'Adj_Close'}, inplace=True)
        
        logger.info(f"Successfully fetched data for {data['Ticker'].nunique()} tickers in this batch.")
        # Select and order columns
        return data[['Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Adj_Close', 'Volume']]

    except Exception as e:
        # Log the specific tickers that caused the error if possible
        logger.error(f"Error fetching data with yfinance for tickers ({', '.join(tickers)}): {e}")
        return pd.DataFrame() # Return empty DataFrame on error

def save_to_bigquery(client: bigquery.Client, df: pd.DataFrame, project_id: str, dataset_id: str, table_id: str):
    """Save DataFrame to a BigQuery table, overwriting existing data."""
    if df.empty:
        logger.warning("DataFrame is empty. Nothing to save to BigQuery.")
        return

    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    logger.info(f"Attempting to save {len(df)} rows to BigQuery table: {table_ref}")

    # Define BigQuery schema explicitly to ensure correct types
    schema = [
        bigquery.SchemaField("Date", "DATE"),
        bigquery.SchemaField("Ticker", "STRING"),
        bigquery.SchemaField("Open", "FLOAT64"), # Use FLOAT64 for more precision
        bigquery.SchemaField("High", "FLOAT64"),
        bigquery.SchemaField("Low", "FLOAT64"),
        bigquery.SchemaField("Close", "FLOAT64"),
        bigquery.SchemaField("Adj_Close", "FLOAT64"),
        bigquery.SchemaField("Volume", "INT64"), # Use INT64 for potentially large volumes
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, # Overwrite table
        source_format=bigquery.SourceFormat.PARQUET,
        # Allow schema updates if needed (e.g., new fields added, though unlikely here)
        # schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION] 
    )

    try:
        # Data type conversions and validation before saving
        df['Date'] = pd.to_datetime(df['Date']).dt.date
        float_cols = ['Open', 'High', 'Low', 'Close', 'Adj_Close']
        df[float_cols] = df[float_cols].astype(float)
        df['Volume'] = df['Volume'].astype('Int64') # Use pandas nullable integer type

        # Convert DataFrame to Parquet bytes for loading
        parquet_bytes = df.to_parquet(engine='pyarrow', index=False) # Ensure index is not included
        
        load_job = client.load_table_from_file(
            io.BytesIO(parquet_bytes), table_ref, job_config=job_config
        )
        
        logger.info(f"Starting BigQuery load job {load_job.job_id} for table {table_ref}...")
        load_job.result()  # Wait for the job to complete
        
        # Verify load job status
        if load_job.errors:
             logger.error(f"BigQuery load job failed for table {table_ref}. Errors: {load_job.errors}")
             raise Exception(f"BigQuery load job failed: {load_job.errors}")
        else:
            destination_table = client.get_table(table_ref)
            logger.info(f"Successfully loaded {destination_table.num_rows} rows to {table_ref}")

    except Exception as e:
        logger.error(f"Error saving data to BigQuery table {table_ref}: {e}")
        if hasattr(e, 'errors'):
             logger.error(f"BigQuery Job Errors: {e.errors}")

def _scrape_exchange_tickers() -> List[Dict[str, Any]]:

        sources = {
            "NASDAQ": "ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt",
            "NYSE": "ftp://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt"
        }

        all_stocks = []

        for exchange, url in sources.items():
            try:
                df = pd.read_csv(url, sep='|')
                if exchange == "NASDAQ":
                    symbols = df[df['Test Issue'] == 'N']['Symbol']
                    names = df[df['Test Issue'] == 'N']['Security Name']
                else:
                    symbols = df[df['Test Issue'] == 'N']['ACT Symbol']
                    names = df[df['Test Issue'] == 'N']['Security Name']

                for ticker, name in zip(symbols, names):
                    if isinstance(ticker, str) and re.match(r'^[A-Z]{1,5}$', ticker):
                        all_stocks.append({
                            'ticker': ticker.strip(),
                            'exchange': exchange,
                            'company_name': name.strip(),
                            'last_updated': datetime.now().isoformat()
                        })

                logger.info(f"Downloaded {len(symbols)} {exchange} stocks")
            except Exception as e:
                logger.error(f"Error downloading {exchange} stocks: {str(e)}")

        return all_stocks

def _batch_insert_tickers( client: bigquery.Client, table_id: str, 
                            stocks: List[Dict[str, Any]]) -> None:
        """
        Insert stock tickers into BigQuery in efficient batches.
        """
        if not stocks:
            logger.warning("No stocks to insert")
            return
        
        # Insert in batches for better performance
        batch_size = 1000
        for i in range(0, len(stocks), batch_size):
            batch = stocks[i:i+batch_size]
            errors = client.insert_rows_json(table_id, batch)
            if errors:
                logger.error(f"Error inserting stock tickers: {errors}")
            else:
                logger.info(f"Inserted batch of {len(batch)} stock tickers")
        
        logger.info(f"Completed inserting {len(stocks)} stock tickers")

def _ensure_table_exists(client: bigquery.Client, project_id: str, 
                             dataset_id: str, table_id: str) -> bool:
    """
    Check if stock_tickers table exists, create if it doesn't.
    
    Returns:
        bool: True if table already existed, False if it was newly created
    """
    try:
        client.get_table(f"{project_id}.{dataset_id}.{table_id}")
        logger.info("Stock tickers table already exists")
        return True
    except NotFound:
        # Create table schema
        schema = [
            bigquery.SchemaField("ticker", "STRING"),
            bigquery.SchemaField("exchange", "STRING"),
            bigquery.SchemaField("company_name", "STRING"),
            bigquery.SchemaField("last_updated", "TIMESTAMP")
        ]
        
        # Ensure dataset exists
        dataset_ref = client.dataset(dataset_id)
        try:
            client.get_dataset(dataset_ref)
        except NotFound:
            # Create dataset
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = os.getenv('GCP_REGION', 'US')
            client.create_dataset(dataset)
            logger.info(f"Created BigQuery dataset {dataset_id}")
        
        # Create table
        table = bigquery.Table(f"{project_id}.{dataset_id}.{table_id}", schema=schema)
        client.create_table(table)
        logger.info(f"Created stock tickers table {table_id}")
        return False
        
def _load_tickers_from_bigquery(client: bigquery.Client, 
                                    project_id: str, dataset_id: str) -> Set[str]:
    """
    Load tickers from BigQuery table.
    
    Returns:
        Set of stock tickers
    """
    logger.info("Loading stock tickers from BigQuery")
    
    query = f"""
    SELECT ticker FROM `{project_id}.{dataset_id}.stock_tickers`
    """
    
    query_job = client.query(query)
    results = query_job.result()
    
    tickers = set()
    for row in results:
        ticker = row.ticker
        # Double-check that ticker follows our pattern (1-5 letters)
        if re.match(r'^[A-Z]{1,5}$', ticker):
            tickers.add(ticker)
    
    if not tickers:
        raise Exception("No valid tickers found in BigQuery results")
    
    logger.info(f"Loaded {len(tickers)} stock tickers from BigQuery")
    return tickers

def get_tickers(client, project_id, dataset_id, table_id):
    table_full_id = f"{project_id}.{dataset_id}.{table_id}"
    try:
        if not _ensure_table_exists(client, project_id, dataset_id, table_id):
                # If newly created, populate with data
            all_stocks = _scrape_exchange_tickers()
            if all_stocks:
                _batch_insert_tickers(client, table_full_id, all_stocks)

    except Exception as e:
        logger.error(f"Error loading stock tickers from BigQuery: {str(e)}")
        logger.info("Falling back to alternative ticker sources")
        raise Exception(f"Error loading stock tickers from BigQuery: {str(e)}")

    tickers = _load_tickers_from_bigquery(client, project_id, dataset_id)
    return tickers

if __name__ == "__main__":
    bq_client = bigquery.Client(project=PROJECT_ID)
    
    # 1. Load tickers from the source table
    tickers = get_tickers(bq_client, PROJECT_ID, DATASET_ID, SOURCE_TABLE_ID)
    # Convert the set to a list so it can be sliced in the batching loop
    tickers = list(tickers) 

    
    if not tickers:
        logger.info("No valid tickers loaded from BigQuery. Exiting.")
    else:
        all_data = []
        # 2. Fetch data in batches
        num_tickers = len(tickers)
        num_batches = (num_tickers + BATCH_SIZE - 1) // BATCH_SIZE
        logger.info(f"Processing {num_tickers} tickers in {num_batches} batches of size {BATCH_SIZE}")

        for i in tqdm(range(0, num_tickers, BATCH_SIZE), desc="Fetching data batches"):
            batch_tickers = tickers[i:min(i + BATCH_SIZE, num_tickers)] # Ensure last batch doesn't exceed list bounds
            logger.info(f"Fetching batch {i//BATCH_SIZE + 1}/{num_batches} ({len(batch_tickers)} tickers: {', '.join(batch_tickers[:5])}{'...' if len(batch_tickers) > 5 else ''})")
            
            batch_data = fetch_stock_data(batch_tickers, HISTORY_PERIOD)
            
            if not batch_data.empty:
                all_data.append(batch_data)
            else:
                 logger.warning(f"Batch {i//BATCH_SIZE + 1} returned no data.")
            
            # Add a small delay between batches to potentially avoid rate limits
            time.sleep(10) 

        if not all_data:
             logger.warning("No data was fetched successfully for any ticker.")
        else:
            # 3. Combine results from all batches
            final_df = pd.concat(all_data, ignore_index=True)
            logger.info(f"Successfully fetched data for {final_df['Ticker'].nunique()} unique tickers, totaling {len(final_df)} rows.")
            
            # 4. Save the combined data to the target BigQuery table
            save_to_bigquery(bq_client, final_df, PROJECT_ID, DATASET_ID, TARGET_TABLE_ID)

    logger.info("Script finished.") 