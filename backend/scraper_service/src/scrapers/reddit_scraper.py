import logging
import os
import time
import praw
from dotenv import load_dotenv
from typing import List, Optional, Tuple
from models.message import RedditPost, RedditComment, Message
from models.database_models import MessageType
from .base_scraper import SocialMediaScraper
from datetime import datetime, timedelta
from ..utils.retry import retry_with_backoff

load_dotenv()

logger = logging.getLogger(__name__)

class RedditScraper(SocialMediaScraper):
    def __init__(self):
        logger.info("Initializing Reddit scraper")
        try:
            self.reddit = praw.Reddit(
                client_id=os.getenv('REDDIT_CLIENT_ID'),
                client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
                user_agent=os.getenv('REDDIT_USER_AGENT', 'stocks_test 1.0')
            )
            # Add rate limiting parameters
            self.request_delay = 2  # seconds between requests
            self.last_request_time = 0
            logger.info("Successfully initialized Reddit API client")
        except Exception as e:
            logger.error(f"Failed to initialize Reddit API client: {str(e)}")
            raise
            
        # Default stock-related subreddits
        self.stock_subreddits = ['wallstreetbets', 'stocks', 'investing', 'stockmarket', 'options']
        logger.info(f"Monitoring subreddits: {', '.join(self.stock_subreddits)}")

    def _wait_for_rate_limit(self):
        """Ensure we don't exceed Reddit's rate limits"""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        if time_since_last_request < self.request_delay:
            sleep_time = self.request_delay - time_since_last_request
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
        self.last_request_time = time.time()

    def _get_subreddit_posts(self, subreddit_name: str, limit: int = 100, sort: str = 'hot', time_filter: str = None) -> List[Message]:
        """Private method to fetch posts from a specified subreddit."""
        logger.info(f"Fetching {limit} {sort} posts from r/{subreddit_name}")
        posts = []
        
        try:
            subreddit = self.reddit.subreddit(subreddit_name)
            logger.debug(f"Successfully connected to r/{subreddit_name}")
            
            # Get the appropriate sorting method
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
            
            logger.debug(f"Using {sort} sort for r/{subreddit_name}")
                
            for post in submissions:
                try:
                    post_obj = RedditPost(
                        id=post.id,
                        content=post.selftext,
                        author=str(post.author) if post.author else '[deleted]',
                        timestamp=post.created_utc,
                        created_at=datetime.fromtimestamp(post.created_utc),
                        url=post.url,
                        score=post.score,
                        title=post.title,
                        selftext=post.selftext,
                        num_comments=post.num_comments,
                        subreddit=subreddit_name
                    )
                    posts.append(post_obj)
                    logger.debug(f"Processed post {post.id} from r/{subreddit_name}")
                except Exception as e:
                    logger.error(f"Error processing post {post.id} from r/{subreddit_name}: {str(e)}")
                    continue
                
        except Exception as e:
            logger.error(f"Error fetching posts from r/{subreddit_name}: {str(e)}")
            
        logger.info(f"Retrieved {len(posts)} posts from r/{subreddit_name}")
        return posts

    def get_posts(self, query: str, limit: int = 100, time_filter: str = None) -> List[Message]:
        """
        Search for posts across stock-related subreddits matching the query.
        
        Args:
            query (str): Search query string
            limit (int): Maximum number of posts to return (default: 100)
            time_filter (str): One of 'all', 'day', 'hour', 'month', 'week', 'year' (default: None)
            
        Returns:
            List[Message]: List of Reddit posts matching the query
        """
        posts = []
        posts_per_subreddit = limit // len(self.stock_subreddits)
        for subreddit_name in self.stock_subreddits:
            try:
                posts_from_subreddit = self._get_subreddit_posts(subreddit_name, posts_per_subreddit, time_filter)
                posts.extend(posts_from_subreddit)
            except Exception as e:
                print(f"Error searching Reddit for '{query}': {str(e)}")
        return posts

    
    def get_subreddit_posts(self, subreddit_name: str, limit: int = 100, sort: str = 'hot', time_filter: str = None) -> List[Message]:
        """
        Public method to fetch posts from a specified subreddit.
        Delegates to private _get_subreddit_posts method.
        
        Args:
            subreddit_name (str): Name of the subreddit to fetch posts from
            limit (int): Maximum number of posts to return (default: 100)
            sort (str): Sort method ('hot', 'new', 'top', 'rising') (default: 'hot')
            time_filter (str): One of 'all', 'day', 'hour', 'month', 'week', 'year'.
                              Only applies to 'top' and 'controversial' sorts. (default: None)
            
        Returns:
            List[Message]: List of Reddit posts from the specified subreddit
        """
        return self._get_subreddit_posts(subreddit_name, limit, sort, time_filter)

    def _process_comments(self, comments_list, limit: int = None, max_depth: int = 10) -> List[Message]:
        """Process comments from a Reddit submission with depth limit."""
        logger.info(f"Processing comments (limit={limit}, max_depth={max_depth})")
        processed_comments = []
        
        def process_comment(comment, depth=0):
            if limit and len(processed_comments) >= limit:
                return
            if depth >= max_depth:
                logger.debug(f"Reached max depth {max_depth}, stopping recursion")
                return
                
            try:
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
                processed_comments.append(comment_obj)
                logger.debug(f"Processed comment {comment.id} at depth {depth}")
                
                # Process replies recursively up to max_depth
                if hasattr(comment, 'replies'):
                    for reply in comment.replies:
                        process_comment(reply, depth + 1)
            except Exception as e:
                logger.error(f"Error processing comment {comment.id}: {str(e)}")
                
        # Process all top-level comments
        try:
            if hasattr(comments_list, 'list'):
                # Handle CommentForest object
                for comment in comments_list:
                    if limit and len(processed_comments) >= limit:
                        break
                    process_comment(comment)
            else:
                # Handle direct list of comments
                for comment in comments_list:
                    if limit and len(processed_comments) >= limit:
                        break
                    process_comment(comment)
        except Exception as e:
            logger.error(f"Error processing comments: {str(e)}")
            
        logger.info(f"Processed {len(processed_comments)} total comments")
        return processed_comments

    def get_post_with_comments(self, post_id: str, comment_limit: int = 100) -> tuple[Message, List[Message]]:
        """
        Fetch a specific post and its comments.
        
        Args:
            post_id (str): Reddit post ID
            comment_limit (int): Maximum number of comments to fetch
            
        Returns:
            tuple[Message, List[Message]]: Tuple of (post, comments)
        """
        try:
            submission = self.reddit.submission(id=post_id)
            
            post = RedditPost(
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
            
            comments = self._process_comments(submission.comments, comment_limit)
            return post, comments
            
        except Exception as e:
            print(f"Error fetching post and comments for ID {post_id}: {str(e)}")
            return None, []

    @retry_with_backoff(retries=3, base_delay=5, exceptions=(Exception,))
    def _replace_more_comments(self, comments, limit: Optional[int] = None) -> None:
        """
        Carefully replace MoreComments objects with actual comments.
        
        This method handles the expansion of "load more comments" items in a Reddit thread.
        It uses a combination of:
        1. Small batch sizes to avoid hitting rate limits
        2. Threshold to skip expanding small comment chains
        3. Retry with backoff when rate limits are hit
        
        Args:
            comments: PRAW comment forest
            limit: Maximum number of "more comments" instances to replace
        """
        try:
            # Get total number of "more comments" instances
            more_comments = sum(1 for comment in comments.list() if isinstance(comment, praw.models.MoreComments))
            logger.info(f"Found {more_comments} 'more comments' instances to expand")
            
            # Process in small batches
            batch_size = 2  # Only process 2 "more comments" at a time
            remaining = limit if limit is not None else more_comments
            
            while remaining > 0:
                current_batch = min(batch_size, remaining)
                logger.debug(f"Processing batch of {current_batch} 'more comments' instances")
                
                # Replace a small batch of "more comments"
                # Note: replace_more only accepts limit and threshold parameters
                comments.replace_more(
                    limit=current_batch,
                    threshold=5  # Only replace if there are at least 5 children
                )
                
                remaining -= current_batch
                if remaining > 0:
                    # Sleep between batches to respect rate limits
                    time.sleep(2)
                    
            logger.info("Finished expanding all comment threads")
            
        except Exception as e:
            logger.error(f"Failed to replace more comments: {str(e)}")
            raise  # Let the retry decorator handle it

    @retry_with_backoff(retries=3, base_delay=5, exceptions=(Exception,))
    async def get_daily_discussion_comments(self, limit: int = None, last_discussion_id: str = None, last_check_time: float = None) -> tuple[Message, List[Message]]:
        """Find and fetch the most recent Daily/Weekend Discussion Thread from wallstreetbets.
        
        Args:
            limit (int, optional): Maximum number of comments to fetch. Defaults to None.
            last_discussion_id (str, optional): ID of the last discussion thread checked. Defaults to None.
            last_check_time (float, optional): Timestamp of last check for new comments. Defaults to None.
            
        Returns:
            tuple[Message, List[Message]]: Tuple of (post, comments). Returns (None, []) if no new thread 
            and no new comments since last check.
        """
        logger.info("Searching for most recent discussion thread")
        
        # Get today's date and check if it's weekend
        now = datetime.now()
        is_weekend = now.weekday() >= 5  # 5 = Saturday, 6 = Sunday
        
        # Set search parameters based on whether it's weekend
        thread_type = "Weekend" if is_weekend else "Daily"
        search_title = f"{thread_type} Discussion Thread"
        logger.debug(f"Searching for thread with title containing: {search_title}")
        
        # Search in wallstreetbets subreddit
        subreddit = await self.reddit.subreddit('wallstreetbets')
        submissions = subreddit.search(query=search_title, limit=10, sort='new')
        
        # Get the most recent matching thread
        async for submission in submissions:
            if submission.title.startswith(search_title):
                # If we have a last_discussion_id and it matches current submission
                if last_discussion_id and submission.id == last_discussion_id:
                    logger.info("Found same discussion thread as last time")
                    # If we have a last_check_time, fetch only new comments
                    if last_check_time:
                        logger.info(f"Fetching new comments since {datetime.fromtimestamp(last_check_time)}")
                        new_comments = await self.get_new_comments(submission.id, last_check_time)
                        # Get the post data again to ensure we have latest stats
                        post, _ = await self.get_post_with_comments(submission.id, 0)
                        return post, new_comments
                    return None, []
                    
                logger.info(f"Found new discussion thread: {submission.title}")
                return await self.get_post_with_comments(submission.id, limit)
                
        thread_type = "weekend" if is_weekend else "daily"
        logger.warning(f"No {thread_type} discussion thread found")
        return None, []

    @retry_with_backoff(retries=3, base_delay=5, exceptions=(Exception,))
    def get_new_comments(self, submission_id: str, last_check_time: float = None) -> List[Message]:
        """Fetch only new comments since last check time."""
        # Set last_check_time to 1 hour ago if not provided
        if last_check_time is None:
            last_check_time = time.time() - 3600  # Current time minus 1 hour
            
        submission = self.reddit.submission(id=submission_id)
        self._replace_more_comments(submission.comments, limit=None)
        
        # Filter comments created after last_check_time
        new_comments = []
        for comment in submission.comments.list():
            if comment.created_utc > last_check_time:
                new_comments.append(comment)
        comments = self._process_comments(new_comments)

        logger.info(f"Found {len(comments)} new comments")
        return comments