import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

from temporalio import activity

from src.extractors.bigquery_extractor import BigQueryExtractor

logger = logging.getLogger(__name__)

@activity.defn
async def extract_reddit_data_activity(last_run_time: Optional[datetime] = None) -> List[Dict[str, Any]]:
    """
    Activity to extract Reddit data from BigQuery.
    
    Args:
        last_run_time: Timestamp of the last successful ETL run
        
    Returns:
        List of dictionaries containing deduplicated Reddit data
    """
    logger.info("Starting extraction activity: Reddit data from BigQuery (with deduplication)")
    
    extractor = BigQueryExtractor()
    df = extractor.get_reddit_data(last_run_time)
    
    if df.empty:
        logger.info("No new Reddit data found in BigQuery")
        return []
    
    # Convert DataFrame to list of dictionaries
    reddit_data = df.to_dict('records')
    logger.info(f"Extracted {len(reddit_data)} deduplicated Reddit posts/comments from BigQuery")
    
    return reddit_data 