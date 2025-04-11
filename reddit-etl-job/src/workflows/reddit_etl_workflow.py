from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple
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
        
        try:
            # Task 1: Get last run timestamp
            last_run_time = await self._get_last_run_time()
            
            # Task 2: Extract Reddit data
            reddit_data = await self._extract_reddit_data(last_run_time)
            
            if not reddit_data:
                logger.error("No new Reddit data found")
                raise ApplicationError("ETL workflow failed: No new Reddit data found")
            
            # Task 3: Analyze stock mentions
            stock_mentions = await self._analyze_stock_mentions(reddit_data)
            
            if not stock_mentions:
                # Still update the timestamp even if no stock mentions were found
                await self._update_run_timestamp(current_run_time)
                return {"status": "success", "message": "No stock mentions identified", "processed": 0}
            
            # Task 4: Save stock mentions
            await self._save_stock_mentions(stock_mentions)
            
            # Task 5: Aggregate summaries
            daily_summaries, hourly_summaries, weekly_summaries = await self._aggregate_summaries(stock_mentions)
            
            # Task 6: Save aggregated data
            await self._save_aggregated_data(daily_summaries, hourly_summaries, weekly_summaries)
            
            # Task 7: Update run timestamp
            await self._update_run_timestamp(current_run_time)
            
            return {
                "status": "success",
                "message": f"Successfully processed {len(stock_mentions)} stock mentions",
                "processed": len(stock_mentions),
                "daily_summaries": len(daily_summaries),
                "hourly_summaries": len(hourly_summaries),
                "weekly_summaries": len(weekly_summaries)
            }
        
        except Exception as e:
            logger.error(f"ETL workflow failed: {str(e)}")
            raise ApplicationError(f"ETL workflow failed: {str(e)}")
    
    @workflow.task
    async def _get_last_run_time(self) -> datetime:
        """Task to get the last run timestamp."""
        return await workflow.execute_activity(
            get_last_run_activity,
            start_to_close_timeout=timedelta(minutes=2)
        )
    
    @workflow.task
    async def _extract_reddit_data(self, last_run_time: datetime) -> List[Dict[str, Any]]:
        """Task to extract Reddit data since last run."""
        return await workflow.execute_activity(
            extract_reddit_data_activity,
            last_run_time,
            start_to_close_timeout=timedelta(minutes=30),
            retry_policy=RetryPolicy(
                maximum_attempts=3,
                initial_interval=timedelta(seconds=10)
            )
        )
    
    @workflow.task
    async def _analyze_stock_mentions(self, reddit_data: List[Dict[str, Any]]) -> List[StockMention]:
        """Task to analyze Reddit data for stock mentions."""
        return await workflow.execute_activity(
            analyze_stock_mentions_activity,
            reddit_data,
            start_to_close_timeout=timedelta(minutes=30),
            retry_policy=RetryPolicy(
                maximum_attempts=3,
                initial_interval=timedelta(seconds=30)
            )
        )
    
    @workflow.task
    async def _save_stock_mentions(self, stock_mentions: List[StockMention]) -> int:
        """Task to save stock mentions to the database."""
        return await workflow.execute_activity(
            save_stock_mentions_activity,
            stock_mentions,
            start_to_close_timeout=timedelta(minutes=60),
            retry_policy=RetryPolicy(
                maximum_attempts=3,
                initial_interval=timedelta(seconds=10)
            )
        )
    
    @workflow.task
    async def _aggregate_summaries(self, stock_mentions: List[StockMention]) -> Tuple[List[DailySummary], List[HourlySummary], List[WeeklySummary]]:
        """Task to aggregate summaries at different time intervals."""
        # Execute aggregation activities in parallel
        daily_summaries_promise = workflow.execute_activity(
            aggregate_daily_summaries_activity,
            stock_mentions,
            start_to_close_timeout=timedelta(minutes=30)
        )
        
        hourly_summaries_promise = workflow.execute_activity(
            aggregate_hourly_summaries_activity,
            stock_mentions,
            start_to_close_timeout=timedelta(minutes=30)
        )
        
        weekly_summaries_promise = workflow.execute_activity(
            aggregate_weekly_summaries_activity,
            stock_mentions,
            start_to_close_timeout=timedelta(minutes=30)
        )
        
        # Wait for all aggregations to complete
        return await workflow.wait_all(
            daily_summaries_promise,
            hourly_summaries_promise,
            weekly_summaries_promise
        )
    
    @workflow.task
    async def _save_aggregated_data(
        self, 
        daily_summaries: List[DailySummary], 
        hourly_summaries: List[HourlySummary], 
        weekly_summaries: List[WeeklySummary]
    ) -> None:
        """Task to save aggregated data to the database."""
        # Save aggregations to database in parallel
        save_daily_promise = workflow.execute_activity(
            save_daily_summaries_activity,
            daily_summaries,
            start_to_close_timeout=timedelta(minutes=60)
        )
        
        save_hourly_promise = workflow.execute_activity(
            save_hourly_summaries_activity,
            hourly_summaries,
            start_to_close_timeout=timedelta(minutes=60)
        )
        
        save_weekly_promise = workflow.execute_activity(
            save_weekly_summaries_activity,
            weekly_summaries,
            start_to_close_timeout=timedelta(minutes=60)
        )
        
        # Wait for all saves to complete
        await workflow.wait_all(
            save_daily_promise,
            save_hourly_promise,
            save_weekly_promise
        )
    
    @workflow.task
    async def _update_run_timestamp(self, current_run_time: datetime) -> None:
        """Task to update the run timestamp."""
        await workflow.execute_activity(
            update_run_timestamp_activity,
            current_run_time,
            start_to_close_timeout=timedelta(minutes=2)
        ) 