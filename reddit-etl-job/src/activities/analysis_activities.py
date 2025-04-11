import logging
import pandas as pd
from typing import List, Dict, Any

from temporalio import activity

from src.utils.stock_analyzer import StockAnalyzer

logger = logging.getLogger(__name__)

@activity.defn
async def analyze_stock_mentions_activity(reddit_data: List[Dict[str, Any]]):
    """
    Activity to analyze Reddit data for stock mentions.
    
    Args:
        reddit_data: List of dictionaries containing Reddit data
        
    Returns:
        None
    """
    logger.info(f"Starting analysis activity: Identifying stock mentions in {len(reddit_data)} Reddit posts")
    
    if not reddit_data:
        logger.info("No Reddit data to analyze")
        return []
    
    # Convert list of dictionaries to DataFrame
    df = pd.DataFrame(reddit_data)
    
    # Create analyzer and process data
    analyzer = StockAnalyzer()
    analyzer.process_reddit_data(df)
    
    logger.info(f"Stock mentions analysis completed")
    
