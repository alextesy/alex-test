import os
import logging
import pandas as pd
from datetime import datetime
from typing import Optional

from google.cloud import bigquery

logger = logging.getLogger(__name__)

class BigQueryExtractor:
    """
    Extracts data from BigQuery for stock analysis.
    """
    
    def __init__(self):
        """Initialize the BigQuery extractor."""
        self.project_id = os.getenv('GOOGLE_CLOUD_PROJECT_ID')
        self.dataset_id = os.getenv('BIGQUERY_DATASET', 'reddit_data')
        self.raw_table_id = 'raw_messages'
        self._client = None
    
    @property
    def client(self) -> bigquery.Client:
        """
        Lazy-loaded BigQuery client.
        
        Returns:
            BigQuery client
        """
        if self._client is None:
            self._client = bigquery.Client(project=self.project_id)
        return self._client
    
    def get_reddit_data(self, last_run_time: Optional[datetime] = None, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Fetch Reddit data from BigQuery.
        
        Args:
            last_run_time: Timestamp of the last successful ETL run
            limit: Optional limit on number of rows to return
            
        Returns:
            DataFrame with Reddit posts/comments
        """
        logger.info("Fetching Reddit data from BigQuery for stock analysis")
        
        # If we have a last run timestamp, only get data since then
        if last_run_time:
            formatted_timestamp = last_run_time.strftime('%Y-%m-%d %H:%M:%S')
            time_filter = f"AND created_at > TIMESTAMP('{formatted_timestamp}')"
            logger.info(f"Fetching Reddit data created after: {formatted_timestamp}")
        else:
            # Process the entire table for the initial run
            time_filter = ""
            logger.info("Initial run: Fetching all Reddit data from BigQuery (no time filter)")
        
        # Get data based on time filter with deduplication logic
        query = f"""
        WITH RankedMessages AS (
            SELECT
                message_id,
                content,
                author,
                created_at,
                subreddit,
                title,
                url,
                score,
                message_type,
                ROW_NUMBER() OVER (PARTITION BY message_id ORDER BY created_at DESC, timestamp DESC) as row_num
            FROM
                `{self.project_id}.{self.dataset_id}.{self.raw_table_id}`
            WHERE
                content IS NOT NULL
                AND LENGTH(content) > 0
                AND content != '[deleted]'
                {time_filter}
        )
        SELECT
            message_id,
            content,
            author,
            created_at,
            subreddit,
            title,
            url,
            score,
            message_type
        FROM
            RankedMessages
        WHERE
            row_num = 1
        ORDER BY
            created_at DESC
        """
        
        if limit:
            query += f"\nLIMIT {limit}"
        
        query_job = self.client.query(query)
        rows = [dict(row) for row in query_job]
        
        if not rows:
            logger.warning("No new Reddit data found in BigQuery")
            return pd.DataFrame()
        
        logger.info(f"Retrieved {len(rows)} deduplicated Reddit posts/comments from BigQuery")
        
        # Convert to DataFrame
        df = pd.DataFrame(rows)
        return df 