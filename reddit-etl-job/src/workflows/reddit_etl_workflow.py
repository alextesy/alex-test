from datetime import datetime, timedelta
from typing import Optional, List
import logging

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

from src.models.stock_data import StockMention, DailySummary, HourlySummary, WeeklySummary
from src.activities.extraction_activities import extract_reddit_data_activity
from src.activities.analysis_activities import analyze_stock_mentions_activity
from src.activities.aggregation_activities import (
    aggregate_daily_summaries_activity,
    aggregate_hourly_summaries_activity,
    aggregate_weekly_summaries_activity
)
from src.activities.persistence_activities import (
    save_stock_mentions_activity,
    save_daily_summaries_activity,
    save_hourly_summaries_activity,
    save_weekly_summaries_activity
)
from src.activities.state_activities import get_last_run_activity, update_run_timestamp_activity

# Configure logging
logger = logging.getLogger(__name__)

@workflow.defn
class RedditEtlWorkflow:
    """Workflow to orchestrate the Reddit ETL process for stock data analysis."""

    @workflow.run
    async def run(self) -> dict:
        """
        Execute the Reddit ETL workflow.
        
        Returns:
            dict: Summary of the ETL run
        """
        current_run_time = datetime.utcnow()
        processed_data = False
        
        try:
            # Get last run timestamp
            last_run_time = await workflow.execute_activity(
                get_last_run_activity,
                start_to_close_timeout=timedelta(minutes=2)
            )
            
            # Extract Reddit data from BigQuery since last run
            reddit_data = await workflow.execute_activity(
                extract_reddit_data_activity,
                last_run_time,
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=RetryPolicy(
                    maximum_attempts=3,
                    initial_interval=timedelta(seconds=10)
                )
            )
            
            if not reddit_data:
                # Still update the timestamp even if there's no new data
                await workflow.execute_activity(
                    update_run_timestamp_activity,
                    current_run_time,
                    start_to_close_timeout=timedelta(minutes=2)
                )
                
                return {"status": "success", "message": "No new Reddit data found", "processed": 0}
            
            # Process data to identify stock mentions
            stock_mentions = await workflow.execute_activity(
                analyze_stock_mentions_activity,
                reddit_data,
                start_to_close_timeout=timedelta(minutes=10),
                retry_policy=RetryPolicy(
                    maximum_attempts=3,
                    initial_interval=timedelta(seconds=30)
                )
            )
            
            if not stock_mentions:
                # Still update the timestamp even if no stock mentions were found
                await workflow.execute_activity(
                    update_run_timestamp_activity,
                    current_run_time,
                    start_to_close_timeout=timedelta(minutes=2)
                )
                
                return {"status": "success", "message": "No stock mentions identified", "processed": 0}
            
            # Save stock mentions to PostgreSQL
            await workflow.execute_activity(
                save_stock_mentions_activity,
                stock_mentions,
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=RetryPolicy(
                    maximum_attempts=3,
                    initial_interval=timedelta(seconds=10)
                )
            )
            
            # Mark as processed
            processed_data = True
            
            # Execute aggregation activities in parallel
            daily_summaries_promise = workflow.execute_activity(
                aggregate_daily_summaries_activity,
                stock_mentions,
                start_to_close_timeout=timedelta(minutes=5)
            )
            
            hourly_summaries_promise = workflow.execute_activity(
                aggregate_hourly_summaries_activity,
                stock_mentions,
                start_to_close_timeout=timedelta(minutes=5)
            )
            
            weekly_summaries_promise = workflow.execute_activity(
                aggregate_weekly_summaries_activity,
                stock_mentions,
                start_to_close_timeout=timedelta(minutes=5)
            )
            
            # Wait for all aggregations to complete
            daily_summaries, hourly_summaries, weekly_summaries = await workflow.wait_all(
                daily_summaries_promise,
                hourly_summaries_promise,
                weekly_summaries_promise
            )
            
            # Save aggregations to database in parallel
            save_daily_promise = workflow.execute_activity(
                save_daily_summaries_activity,
                daily_summaries,
                start_to_close_timeout=timedelta(minutes=5)
            )
            
            save_hourly_promise = workflow.execute_activity(
                save_hourly_summaries_activity,
                hourly_summaries,
                start_to_close_timeout=timedelta(minutes=5)
            )
            
            save_weekly_promise = workflow.execute_activity(
                save_weekly_summaries_activity,
                weekly_summaries,
                start_to_close_timeout=timedelta(minutes=5)
            )
            
            # Wait for all saves to complete
            await workflow.wait_all(
                save_daily_promise,
                save_hourly_promise,
                save_weekly_promise
            )
            
            # Update state with current run time
            await workflow.execute_activity(
                update_run_timestamp_activity,
                current_run_time,
                start_to_close_timeout=timedelta(minutes=2)
            )
            
            return {
                "status": "success",
                "message": f"Successfully processed {len(stock_mentions)} stock mentions",
                "processed": len(stock_mentions),
                "daily_summaries": len(daily_summaries),
                "hourly_summaries": len(hourly_summaries),
                "weekly_summaries": len(weekly_summaries)
            }
        
        except Exception as e:
            # If we've processed some data but failed later, still update timestamp
            # This prevents reprocessing the same data in case of a partial failure
            if processed_data:
                try:
                    await workflow.execute_activity(
                        update_run_timestamp_activity,
                        current_run_time,
                        start_to_close_timeout=timedelta(minutes=2)
                    )
                except Exception as update_error:
                    logger.error(f"Failed to update timestamp after partial processing: {str(update_error)}")
            
            raise ApplicationError(f"ETL workflow failed: {str(e)}") 