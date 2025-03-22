import os
import time
import random
import asyncio
import logging
from datetime import datetime
from typing import Tuple, List, Optional

import asyncpraw
from utils.retry import retry_with_backoff
from models.message import RedditPost, RedditComment  # Assumes these are your domain models


# --- RateLimiter -------------------------------------------------------------
class RateLimiter:
    """
    A simple rate limiter to ensure we respect Reddit's API limits.
    Adds random jitter to avoid synchronized requests.
    """
    def __init__(self, base_delay: float = 3.0):
        self.base_delay = base_delay
        self.last_request_time = 0.0

    async def wait(self):
        now = time.time()
        elapsed = now - self.last_request_time
        # Apply jitter: a random factor between 0.5 and 1.5
        jitter = random.uniform(0.5, 1.5)
        delay = self.base_delay * jitter
        wait_time = delay - elapsed
        if wait_time > 0:
            await asyncio.sleep(wait_time)
        self.last_request_time = time.time()


# --- RedditAPI ---------------------------------------------------------------
class RedditAPI:
    """
    Wraps the asyncpraw Reddit client, handling initialization,
    rate limiting (by sharing a RateLimiter instance), and cleanup.
    """
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        try:
            self.reddit = asyncpraw.Reddit(
                client_id=os.getenv('REDDIT_CLIENT_ID'),
                client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
                user_agent=os.getenv('REDDIT_USER_AGENT', 'stocks_test 1.0')
            )
            self.logger.info("Reddit API client initialized successfully")
        except Exception as e:
            self.logger.error("Failed to initialize Reddit API client", exc_info=True)
            raise
        self.rate_limiter = RateLimiter(base_delay=3.0)

    async def get_subreddit(self, subreddit_name: str):
        await self.rate_limiter.wait()
        return await self.reddit.subreddit(subreddit_name)

    async def close(self):
        self.logger.info("Closing Reddit API client")
        try:
            await self.reddit.close()
            self.logger.info("Reddit API client closed successfully")
        except Exception as e:
            self.logger.error("Error closing Reddit API client", exc_info=True)


# --- CommentProcessor --------------------------------------------------------
class CommentProcessor:
    """
    Processes Reddit comment trees recursively.
    Converts asyncpraw comment objects into our RedditComment domain model.
    """
    def __init__(self, rate_limiter: RateLimiter):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.rate_limiter = rate_limiter

    async def process_comment(
        self,
        comment,
        depth: int = 0,
        processed: Optional[List[RedditComment]] = None,
        limit: Optional[int] = None
    ) -> List[RedditComment]:
        if processed is None:
            processed = []

        # Stop if a limit is reached
        if limit is not None and len(processed) >= limit:
            return processed

        try:
            self.logger.debug(f"Processing comment {comment.id} at depth {depth}")
            comment_obj = RedditComment(
                id=comment.id,
                content=comment.body,
                author=str(comment.author) if comment.author else '[deleted]',
                timestamp=comment.created_utc,
                created_at=datetime.fromtimestamp(comment.created_utc),
                url=f"https://reddit.com{comment.permalink}",
                score=comment.score,
                parent_id=comment.parent_id,
                depth=depth,
                subreddit=comment.subreddit.display_name
            )
            processed.append(comment_obj)
        except Exception as e:
            self.logger.error(f"Error processing comment {comment.id}: {e}", exc_info=True)
            return processed

        # Process replies recursively
        if hasattr(comment, 'replies') and comment.replies:
            for reply in comment.replies:
                if not isinstance(reply, asyncpraw.models.MoreComments):
                    await self.process_comment(reply, depth=depth + 1, processed=processed, limit=limit)
        return processed

    async def process_comments(self, comments, limit: Optional[int] = None) -> List[RedditComment]:
        processed_comments = []
        try:
            for comment in comments:
                if isinstance(comment, asyncpraw.models.MoreComments):
                    continue
                await self.process_comment(comment, depth=0, processed=processed_comments, limit=limit)
        except Exception as e:
            self.logger.error("Error processing comments list: " + str(e), exc_info=True)
        self.logger.info(f"Total processed comments: {len(processed_comments)}")
        return processed_comments


# --- RedditScraper -----------------------------------------------------------
class RedditScraper:
    """
    Main orchestrator for Reddit scraping. Provides methods to fetch posts
    from a subreddit, retrieve a post with all its comments (recursively),
    and fetch the daily discussion thread (with support for updating only new comments).
    """
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.api = RedditAPI()
        self.rate_limiter = self.api.rate_limiter
        self.comment_processor = CommentProcessor(self.rate_limiter)
        # List of stock-related subreddits to monitor.
        self.stock_subreddits = ['wallstreetbets', 'stocks', 'investing', 'stockmarket', 'options']

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.api.close()

    async def fetch_posts_from_subreddit(
        self,
        subreddit_name: str,
        limit: int = 100,
        sort: str = 'hot',
        time_filter: Optional[str] = None
    ) -> List[RedditPost]:
        posts = []
        try:
            self.logger.info(f"Fetching posts from r/{subreddit_name} (sort: {sort}, limit: {limit})")
            subreddit = await self.api.get_subreddit(subreddit_name)
            # Choose sorting method based on parameter.
            if sort == 'hot':
                submissions = subreddit.hot(limit=limit)
            elif sort == 'new':
                submissions = subreddit.new(limit=limit)
            elif sort == 'top':
                submissions = subreddit.top(limit=limit, time_filter=time_filter)
            elif sort == 'rising':
                submissions = subreddit.rising(limit=limit)
            else:
                submissions = subreddit.hot(limit=limit)

            async for submission in submissions:
                try:
                    post_obj = RedditPost(
                        id=submission.id,
                        content=submission.selftext,
                        author=str(submission.author) if submission.author else '[deleted]',
                        timestamp=submission.created_utc,
                        created_at=datetime.fromtimestamp(submission.created_utc),
                        url=submission.url,
                        score=submission.score,
                        title=submission.title,
                        selftext=submission.selftext,
                        num_comments=submission.num_comments,
                        subreddit=subreddit_name
                    )
                    posts.append(post_obj)
                    self.logger.debug(f"Processed post {submission.id} from r/{subreddit_name}")
                except Exception as e:
                    self.logger.error(f"Error processing submission {submission.id}: {e}", exc_info=True)
            self.logger.info(f"Successfully fetched {len(posts)} posts from r/{subreddit_name}")
        except Exception as e:
            self.logger.error(f"Error fetching posts from subreddit {subreddit_name}: {e}", exc_info=True)
        return posts

    @retry_with_backoff(retries=3, base_delay=5, exceptions=(Exception,))
    async def fetch_post_with_comments(
        self,
        post_id: str,
        comment_limit: Optional[int] = None
    ) -> Tuple[RedditPost, List[RedditComment]]:
        """
        Fetches a specific post and recursively processes its comments.
        Will retry up to 3 times with exponential backoff on connection errors.
        """
        try:
            self.logger.info(f"Fetching post {post_id} with comments (limit: {comment_limit})")
            await self.rate_limiter.wait()
            
            try:
                submission = await self.api.reddit.submission(id=post_id)
            except Exception as e:
                error_msg = str(e)
                error_type = type(e).__name__
                self.logger.error(f"Network error when fetching submission {post_id}: {error_type} - {error_msg}", exc_info=True)
                raise  # Let retry_with_backoff handle this
            
            # Create the post object.
            post_obj = RedditPost(
                id=submission.id,
                content=submission.selftext,
                author=str(submission.author) if submission.author else '[deleted]',
                timestamp=submission.created_utc,
                created_at=datetime.fromtimestamp(submission.created_utc),
                url=submission.url,
                score=submission.score,
                title=submission.title,
                selftext=submission.selftext,
                num_comments=submission.num_comments,
                subreddit=submission.subreddit.display_name
            )
            await self.rate_limiter.wait()
            
            # Replace "more comments" objects with a moderate limit.
            self.logger.info(f"Replacing 'more comments' objects for post {post_id}")
            try:
                await submission.comments.replace_more(limit=50)
            except Exception as e:
                error_msg = str(e)
                error_type = type(e).__name__
                self.logger.error(f"Error replacing 'more comments' for post {post_id}: {error_type} - {error_msg}", exc_info=True)
                raise  # Let retry_with_backoff handle this
                
            self.logger.info(f"Processing comments for post {post_id}")
            comments = await self.comment_processor.process_comments(submission.comments, limit=comment_limit)
            self.logger.info(f"Successfully processed {len(comments)} comments for post {post_id}")
            return post_obj, comments
        except Exception as e:
            error_msg = str(e)
            error_type = type(e).__name__
            # Specifically log connection reset errors
            if "Connection reset by peer" in error_msg:
                self.logger.error(f"Connection reset error for post {post_id}: {error_type} - {error_msg}", exc_info=True)
            else:
                self.logger.error(f"Error fetching post with comments for {post_id}: {error_type} - {error_msg}", exc_info=True)
            raise

    @retry_with_backoff(retries=3, base_delay=5, exceptions=(Exception,))
    async def fetch_daily_discussion(
        self,
        limit: Optional[int] = None,
        last_discussion_id: Optional[str] = None,
        last_check_time: Optional[float] = None
    ) -> Tuple[Optional[RedditPost], List[RedditComment]]:
        """
        Searches for the latest Daily/Weekend Discussion Thread on wallstreetbets.
        If a previous discussion ID and last check time are provided, only new comments are returned.
        """
        self.logger.info("Fetching daily discussion thread")
        try:
            subreddit = await self.api.get_subreddit('wallstreetbets')
            # Determine thread type based on weekday.
            thread_type = "Weekend" if datetime.now().weekday() >= 5 else "Daily"
            search_title = f"{thread_type} Discussion Thread"
            submissions = []
            # Approach 1: Search by title.
            self.logger.info(f"Searching for {search_title} in r/wallstreetbets")
            async for submission in subreddit.search(query=search_title, limit=10, sort='new'):
                await self.rate_limiter.wait()
                submissions.append(submission)
                if submission.title.startswith(search_title):
                    break
            # Fallback: if no submissions via search, try hot posts.
            if not submissions:
                self.logger.info("No thread found via search, checking hot posts")
                async for submission in subreddit.hot(limit=20):
                    await self.rate_limiter.wait()
                    if search_title.lower() in submission.title.lower():
                        submissions.append(submission)
            for submission in submissions:
                if search_title.lower() in submission.title.lower():
                    # If this is an existing discussion thread, filter new comments.
                    if last_discussion_id and submission.id == last_discussion_id and last_check_time is not None:
                        post, comments = await self.fetch_post_with_comments(submission.id, comment_limit=limit)
                        self.logger.info(f"Number of comments before time filtering: {len(comments)} (filtering comments after {datetime.fromtimestamp(last_check_time)})")
                        new_comments = [c for c in comments if c.timestamp > last_check_time]
                        self.logger.info(f"Found existing thread {submission.id} with {len(new_comments)} new comments")
                        return post, new_comments
                    else:
                        return await self.fetch_post_with_comments(submission.id, comment_limit=limit)
            self.logger.warning(f"No {thread_type} discussion thread found")
            return None, []
        except Exception as e:
            self.logger.error(f"Error fetching daily discussion thread: {e}", exc_info=True)
            raise

    async def fetch_posts_from_all_subreddits(
        self,
        limit: int = 100,
        sort: str = 'hot',
        time_filter: Optional[str] = None
    ) -> List[RedditPost]:
        """
        Fetch posts from all configured stock-related subreddits concurrently.
        """
        self.logger.info(f"Starting to fetch posts from {len(self.stock_subreddits)} subreddits")
        posts = []
        tasks = [
            self.fetch_posts_from_subreddit(sub, limit=limit, sort=sort, time_filter=time_filter)
            for sub in self.stock_subreddits
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                self.logger.error(f"Error fetching posts from {self.stock_subreddits[i]}: {str(result)}", exc_info=True)
            else:
                posts.extend(result)
        self.logger.info(f"Successfully fetched {len(posts)} total posts from all subreddits")
        return posts