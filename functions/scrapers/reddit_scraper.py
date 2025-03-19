import logging
import os
import time
import asyncpraw
from dotenv import load_dotenv
from typing import List, Optional, Tuple
from models.message import RedditPost, RedditComment, Message
from models.database_models import MessageType
from .base_scraper import SocialMediaScraper
from datetime import datetime, timedelta
from utils.retry import retry_with_backoff
import asyncio
import random

load_dotenv()

logger = logging.getLogger(__name__)

class RedditScraper(SocialMediaScraper):
    def __init__(self):
        logger.info("Initializing Reddit scraper")
        try:
            # Initialize AsyncPRAW with environment variables
            self.reddit = asyncpraw.Reddit(
                client_id=os.getenv('REDDIT_CLIENT_ID'),
                client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
                user_agent=os.getenv('REDDIT_USER_AGENT', 'stocks_test 1.0')
            )
            # Add rate limiting parameters - increase delay to be more conservative
            self.request_delay = 3  # seconds between requests (increased from 2)
            self.last_request_time = 0
            logger.info("Successfully initialized Reddit API client")
        except Exception as e:
            logger.error(f"Failed to initialize Reddit API client: {str(e)}")
            raise
            
        # Default stock-related subreddits
        self.stock_subreddits = ['wallstreetbets', 'stocks', 'investing', 'stockmarket', 'options']
        logger.info(f"Monitoring subreddits: {', '.join(self.stock_subreddits)}")

    async def __aenter__(self):
        """Async context manager entry"""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()

    async def close(self):
        """Close the Reddit client and any open sessions"""
        logger.info("Closing Reddit client and sessions")
        try:
            if hasattr(self.reddit, '_core'):
                logger.debug("Reddit client has _core attribute")
                if hasattr(self.reddit._core, '_requestor'):
                    logger.debug("Reddit client _core has _requestor attribute")
                    if hasattr(self.reddit._core._requestor, '_http'):
                        logger.debug("Closing Reddit client HTTP session")
                        await self.reddit._core._requestor._http.close()
                        logger.debug("Successfully closed HTTP session")
            
            logger.debug("Closing main Reddit client")
            await self.reddit.close()
            logger.info("Successfully closed Reddit client")
        except Exception as e:
            logger.error(f"Error closing Reddit client: {str(e)}")
            logger.debug("Close error details:", exc_info=True)

    async def _wait_for_rate_limit(self):
        """Ensure we don't exceed Reddit's rate limits"""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        
        # Add jitter to avoid synchronized requests
        jitter = random.uniform(0.5, 1.5)
        delay = self.request_delay * jitter
        
        if time_since_last_request < delay:
            sleep_time = delay - time_since_last_request
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            await asyncio.sleep(sleep_time)
            
        self.last_request_time = time.time()

    async def _get_subreddit_posts(self, subreddit_name: str, limit: int = 100, sort: str = 'hot', time_filter: str = None) -> List[Message]:
        """Private method to fetch posts from a specified subreddit."""
        logger.info(f"Fetching {limit} {sort} posts from r/{subreddit_name}")
        posts = []
        
        try:
            subreddit = await self.reddit.subreddit(subreddit_name)
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
                
            async for post in submissions:
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
                posts_from_subreddit = asyncio.run(self._get_subreddit_posts(subreddit_name, posts_per_subreddit, time_filter))
                posts.extend(posts_from_subreddit)
            except Exception as e:
                print(f"Error searching Reddit for '{query}': {str(e)}")
        return posts

    
    async def get_subreddit_posts(self, subreddit_name: str, limit: int = 100, sort: str = 'hot', time_filter: str = None) -> List[Message]:
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
        return await self._get_subreddit_posts(subreddit_name, limit, sort, time_filter)

    async def _process_comments(self, comments_list, limit: int = None) -> List[Message]:
        """Process comments from a Reddit submission."""
        logger.info(f"Processing comments (limit={limit})")
        processed_comments = []
        comment_count = 0
        
        try:
            # Try to get the length of the comments list for better logging
            try:
                if hasattr(comments_list, '__len__'):
                    comment_count = len(comments_list)
                    logger.info(f"Starting to process {comment_count} comments")
                else:
                    logger.info("Starting to process comments (count unknown)")
            except Exception:
                logger.debug("Could not determine comment count")
            
            # Check if comments_list is None or empty
            if not comments_list:
                logger.warning("Comments list is empty or None")
                return []
                
            # Check if comments_list is the right type
            logger.info(f"Comments list type: {type(comments_list)}")
        
            async def process_comment(comment, depth=0):
                if limit and len(processed_comments) >= limit:
                    return
                    
                try:
                    # Log comment attributes for debugging
                    logger.debug(f"Processing comment {comment.id} at depth {depth}")
                    logger.debug(f"Comment attributes: id={comment.id}, author={comment.author}, score={comment.score}")
                    
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
                    
                    # Process replies recursively
                    if hasattr(comment, 'replies') and comment.replies:
                        reply_count = 0
                        try:
                            if hasattr(comment.replies, '__len__'):
                                reply_count = len(comment.replies)
                        except Exception:
                            pass
                            
                        logger.debug(f"Processing {reply_count} replies for comment {comment.id}")
                        # No need to await replies anymore
                        for reply in comment.replies:
                            if not isinstance(reply, asyncpraw.models.MoreComments):
                                await process_comment(reply, depth + 1)
                except Exception as e:
                    logger.error(f"Error processing comment {comment.id}: {str(e)}")
                    logger.debug("Comment processing error details:", exc_info=True)
                    
            # Process all comments
            try:
                # Comments list is now a property, no need to await
                more_comments_count = 0
                regular_comments_count = 0
                
                # Try different approaches to iterate through comments
                try:
                    # First try direct iteration
                    for comment in comments_list:
                        if isinstance(comment, asyncpraw.models.MoreComments):
                            more_comments_count += 1
                        else:
                            regular_comments_count += 1
                            await process_comment(comment)
                except TypeError:
                    # If direct iteration fails, try accessing as a list
                    logger.warning("Direct iteration failed, trying list access")
                    if hasattr(comments_list, '__getitem__'):
                        for i in range(len(comments_list)):
                            comment = comments_list[i]
                            if isinstance(comment, asyncpraw.models.MoreComments):
                                more_comments_count += 1
                            else:
                                regular_comments_count += 1
                                await process_comment(comment)
                    else:
                        logger.error("Comments list is not iterable and does not support indexing")
                        
                logger.info(f"Found {regular_comments_count} regular comments and {more_comments_count} 'more comments' objects")
                
                # If we didn't process any comments but the list isn't empty, something went wrong
                if regular_comments_count == 0 and comment_count > 0:
                    logger.warning("No regular comments were processed despite having comments in the list")
                    
                    # Try a different approach - use the list() method if available
                    try:
                        if hasattr(comments_list, 'list'):
                            logger.info("Trying to use comments_list.list() method")
                            flat_comments = comments_list.list()
                            logger.info(f"Got {len(flat_comments)} comments from list() method")
                            
                            for comment in flat_comments:
                                if not isinstance(comment, asyncpraw.models.MoreComments):
                                    await process_comment(comment)
                    except Exception as list_error:
                        logger.error(f"Error using list() method: {str(list_error)}")
                
            except Exception as e:
                logger.error(f"Error processing comments list: {str(e)}")
                logger.debug("Comments list processing error details:", exc_info=True)
                
            logger.info(f"Processed {len(processed_comments)} total comments")
            return processed_comments
        except Exception as e:
            logger.error(f"Unexpected error in _process_comments: {str(e)}")
            logger.debug("Process comments error details:", exc_info=True)
            return processed_comments

    @retry_with_backoff(retries=3, base_delay=5, exceptions=(Exception,))
    async def get_post_with_comments(self, post_id: str, comment_limit: int = 100) -> tuple[Message, List[Message]]:
        """
        Fetch a specific post and its comments.
        
        Args:
            post_id (str): Reddit post ID
            comment_limit (int): Maximum number of comments to fetch
            
        Returns:
            tuple[Message, List[Message]]: Tuple of (post, comments)
        """
        logger.info(f"Starting to fetch post and comments for ID {post_id}")
        max_retries = 3
        retry_count = 0
        backoff_time = 5  # Start with 5 seconds
        
        while retry_count < max_retries:
            try:
                # Add rate limiting wait
                logger.debug(f"Applying rate limiting before fetching submission {post_id}")
                await self._wait_for_rate_limit()
                
                # Fetch the submission
                logger.info(f"Fetching submission with ID {post_id}")
                submission = await self.reddit.submission(id=post_id)
                logger.info(f"Successfully fetched submission: '{submission.title}' with {submission.num_comments} comments")
                
                # Create post object
                logger.debug(f"Creating RedditPost object for submission {post_id}")
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
                logger.debug(f"Successfully created RedditPost object for {post_id}")
                
                # Get comments with additional error handling
                try:
                    # Add rate limiting wait before fetching comments
                    logger.debug(f"Applying rate limiting before fetching comments for {post_id}")
                    await self._wait_for_rate_limit()
                    
                    # Increase the replace_more limit to get more comments
                    # but still be conservative to avoid connection issues
                    replace_limit = min(20, comment_limit if comment_limit else 20)
                    logger.info(f"Using replace_more limit of {replace_limit}")
                    
                    await submission.comments.replace_more(limit=replace_limit)
                    
                    # Log the comment tree structure for debugging
                    try:
                        top_level_count = len(submission.comments)
                        logger.info(f"Found {top_level_count} top-level comments in the thread")
                        
                        # Check if comments are accessible
                        if hasattr(submission.comments, '__iter__'):
                            logger.info("Comment tree is iterable")
                        else:
                            logger.warning("Comment tree is not iterable, may have issues processing comments")
                    except Exception as e:
                        logger.warning(f"Error inspecting comment tree: {str(e)}")
                    
                    logger.info(f"Successfully fetched comment tree for {post_id}, now processing comments")
                    
                    # Process comments
                    comments = await self._process_comments(submission.comments, comment_limit)
                    
                    # If we didn't get any comments but the submission has comments, try a different approach
                    if len(comments) == 0 and submission.num_comments > 0:
                        logger.warning(f"No comments processed despite submission having {submission.num_comments} comments. Trying alternative approach.")
                        
                        # Try to get comments using the list() method directly
                        try:
                            logger.info("Trying to get flat comment list directly")
                            await self._wait_for_rate_limit()
                            
                            # Try to get a flat list of comments
                            flat_comments = submission.comments.list()
                            logger.info(f"Got {len(flat_comments)} comments from flat list")
                            
                            # Process the flat list
                            comments = await self._process_comments(flat_comments, comment_limit)
                            logger.info(f"Processed {len(comments)} comments from flat list")
                        except Exception as flat_error:
                            logger.error(f"Error getting flat comment list: {str(flat_error)}")
                            logger.debug("Flat list error details:", exc_info=True)
                            
                        # If we still don't have comments, try a third approach
                        if len(comments) == 0 and submission.num_comments > 0:
                            logger.warning("Still no comments. Trying third approach: re-fetching submission")
                            try:
                                # Re-fetch the submission with a fresh client
                                logger.info("Creating a new Reddit client for fresh fetch")
                                temp_reddit = asyncpraw.Reddit(
                                    client_id=os.getenv('REDDIT_CLIENT_ID'),
                                    client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
                                    user_agent=os.getenv('REDDIT_USER_AGENT', 'stocks_test 1.0')
                                )
                                
                                # Wait for rate limiting
                                await self._wait_for_rate_limit()
                                
                                # Fetch the submission again
                                logger.info(f"Re-fetching submission {post_id}")
                                fresh_submission = await temp_reddit.submission(id=post_id)
                                
                                # Wait for rate limiting
                                await self._wait_for_rate_limit()
                                
                                # Get comments with a more aggressive approach
                                logger.info("Fetching comments with more aggressive settings")
                                await fresh_submission.comments.replace_more(limit=None)
                                
                                # Process comments
                                fresh_comments = await self._process_comments(fresh_submission.comments, comment_limit)
                                logger.info(f"Processed {len(fresh_comments)} comments from fresh fetch")
                                
                                # Close the temporary client
                                await temp_reddit.close()
                                
                                # Use these comments if we got any
                                if len(fresh_comments) > 0:
                                    comments = fresh_comments
                            except Exception as fresh_error:
                                logger.error(f"Error with fresh fetch approach: {str(fresh_error)}")
                                logger.debug("Fresh fetch error details:", exc_info=True)
                    
                    logger.info(f"Successfully processed {len(comments)} comments for submission {post_id}")
                    
                    return post, comments
                except Exception as comment_error:
                    logger.warning(f"Error fetching comments for post {post_id}, returning post with empty comments: {str(comment_error)}")
                    logger.debug("Comment error details:", exc_info=True)
                    return post, []
                    
            except asyncio.TimeoutError:
                retry_count += 1
                logger.warning(f"Timeout error fetching post {post_id}, retry {retry_count}/{max_retries}")
                await asyncio.sleep(backoff_time)
                backoff_time *= 2  # Exponential backoff
                
            except ConnectionError as conn_error:
                retry_count += 1
                logger.warning(f"Connection error fetching post {post_id}, retry {retry_count}/{max_retries}: {str(conn_error)}")
                await asyncio.sleep(backoff_time)
                backoff_time *= 2  # Exponential backoff
                
            except Exception as e:
                # For other exceptions, log and retry with backoff
                retry_count += 1
                logger.error(f"Error fetching post {post_id}, retry {retry_count}/{max_retries}: {str(e)}")
                logger.debug("Error details:", exc_info=True)
                
                # If this is a connection reset error, add extra delay
                if "Connection reset by peer" in str(e):
                    extra_delay = 30  # Add 30 seconds extra delay for connection reset errors
                    logger.warning(f"Connection reset detected, adding {extra_delay}s extra delay")
                    await asyncio.sleep(extra_delay)
                
                await asyncio.sleep(backoff_time)
                backoff_time *= 2  # Exponential backoff
        
        # If we've exhausted all retries
        logger.error(f"Failed to fetch post {post_id} after {max_retries} retries")
        return None, []

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
        
        max_retries = 3
        retry_count = 0
        backoff_time = 5  # Start with 5 seconds
        
        while retry_count < max_retries:
            try:
                # Add rate limiting wait
                await self._wait_for_rate_limit()
                
                # Search in wallstreetbets subreddit
                subreddit = await self.reddit.subreddit('wallstreetbets')
                
                # Try two different approaches to find the daily discussion thread
                
                # Approach 1: Search for the thread by title
                logger.info("Approach 1: Searching for thread by title")
                try:
                    submissions = []
                    async for submission in subreddit.search(query=search_title, limit=10, sort='new'):
                        # Add rate limiting between each submission fetch
                        await self._wait_for_rate_limit()
                        submissions.append(submission)
                        
                        # Break early if we find a matching thread to reduce API calls
                        if submission.title.startswith(search_title):
                            break
                    
                    logger.info(f"Found {len(submissions)} submissions from search")
                except Exception as search_error:
                    logger.error(f"Error searching for discussion thread: {str(search_error)}")
                    submissions = []
                
                # Approach 2: Get hot posts and filter by title
                if not submissions:
                    logger.info("Approach 2: Getting hot posts and filtering by title")
                    try:
                        hot_submissions = []
                        async for submission in subreddit.hot(limit=20):
                            # Add rate limiting between each submission fetch
                            await self._wait_for_rate_limit()
                            
                            # Check if this is a daily discussion thread
                            if search_title.lower() in submission.title.lower():
                                hot_submissions.append(submission)
                                logger.info(f"Found potential thread from hot: {submission.title}")
                        
                        logger.info(f"Found {len(hot_submissions)} potential threads from hot")
                        submissions.extend(hot_submissions)
                    except Exception as hot_error:
                        logger.error(f"Error getting hot posts: {str(hot_error)}")
                
                # Process the submissions we found
                for submission in submissions:
                    if search_title.lower() in submission.title.lower():
                        # If we have a last_discussion_id and it matches current submission
                        if last_discussion_id and submission.id == last_discussion_id:
                            logger.info("Found same discussion thread as last time")
                            # If we have a last_check_time, fetch only new comments
                            if last_check_time:
                                logger.info(f"Fetching new comments since {datetime.fromtimestamp(last_check_time)}")
                                try:
                                    # Get new comments with improved error handling
                                    logger.info(f"Calling get_new_comments for thread ID {submission.id} with last_check_time={last_check_time}")
                                    new_comments = await self.get_new_comments(submission.id, last_check_time)
                                    logger.info(f"Successfully got {len(new_comments)} new comments from get_new_comments")
                                    
                                    # Get the post data again to ensure we have latest stats
                                    logger.info(f"Fetching updated post data for thread ID {submission.id}")
                                    post, _ = await self.get_post_with_comments(submission.id, 0)
                                    logger.info(f"Successfully fetched updated post data for thread ID {submission.id}")
                                    
                                    # If we didn't get any new comments but there should be some, try a different approach
                                    if len(new_comments) == 0 and submission.num_comments > 0:
                                        logger.warning(f"No new comments found despite thread having {submission.num_comments} comments. Trying alternative approach.")
                                        
                                        # Try getting all comments and filtering by timestamp
                                        try:
                                            logger.info(f"Trying alternative approach: get all comments and filter by timestamp")
                                            _, all_comments = await self.get_post_with_comments(submission.id, None)
                                            logger.info(f"Got {len(all_comments)} total comments")
                                            
                                            # Filter comments by timestamp
                                            filtered_comments = [c for c in all_comments if c.timestamp > last_check_time]
                                            logger.info(f"Filtered to {len(filtered_comments)} new comments based on timestamp")
                                            
                                            if len(filtered_comments) > 0:
                                                new_comments = filtered_comments
                                        except Exception as alt_error:
                                            logger.error(f"Error with alternative approach: {str(alt_error)}")
                                    
                                    return post, new_comments
                                except Exception as comment_error:
                                    logger.error(f"Error fetching new comments: {str(comment_error)}")
                                    logger.debug("New comments error details:", exc_info=True)
                                    
                                    # Try a fallback approach if the first one fails
                                    try:
                                        logger.info("First approach failed, trying fallback approach")
                                        # Get all comments and filter by timestamp
                                        post, all_comments = await self.get_post_with_comments(submission.id, None)
                                        
                                        # Filter comments by timestamp
                                        new_comments = [c for c in all_comments if c.timestamp > last_check_time]
                                        logger.info(f"Fallback approach found {len(new_comments)} new comments")
                                        
                                        return post, new_comments
                                    except Exception as fallback_error:
                                        logger.error(f"Fallback approach also failed: {str(fallback_error)}")
                                        return None, []
                            
                            # If we don't have a last_check_time, just return the post with no comments
                            logger.info("No last_check_time provided, returning post with no comments")
                            try:
                                post, _ = await self.get_post_with_comments(submission.id, 0)
                                return post, []
                            except Exception as post_error:
                                logger.error(f"Error fetching post: {str(post_error)}")
                                return None, []
                        
                        # This is a new discussion thread
                        logger.info(f"Found new discussion thread: {submission.title}")
                        try:
                            # Use our improved get_post_with_comments method
                            logger.info(f"Calling get_post_with_comments for thread ID {submission.id} with limit={limit}")
                            result = await self.get_post_with_comments(submission.id, limit)
                            logger.info(f"Successfully returned from get_post_with_comments for thread ID {submission.id}")
                            return result
                        except Exception as post_error:
                            logger.error(f"Error fetching post with comments: {str(post_error)}")
                            logger.debug("Post error details:", exc_info=True)
                            raise
                
                # If we get here, no matching thread was found
                thread_type = "weekend" if is_weekend else "daily"
                logger.warning(f"No {thread_type} discussion thread found")
                return None, []
                
            except asyncio.TimeoutError:
                retry_count += 1
                logger.warning(f"Timeout error searching for discussion thread, retry {retry_count}/{max_retries}")
                await asyncio.sleep(backoff_time)
                backoff_time *= 2  # Exponential backoff
                
            except ConnectionError as conn_error:
                retry_count += 1
                logger.warning(f"Connection error searching for discussion thread, retry {retry_count}/{max_retries}: {str(conn_error)}")
                await asyncio.sleep(backoff_time)
                backoff_time *= 2  # Exponential backoff
                
            except Exception as e:
                # For other exceptions, log and retry with backoff
                retry_count += 1
                logger.error(f"Error searching for discussion thread, retry {retry_count}/{max_retries}: {str(e)}")
                
                # If this is a connection reset error, add extra delay
                if "Connection reset by peer" in str(e):
                    extra_delay = 30  # Add 30 seconds extra delay for connection reset errors
                    logger.warning(f"Connection reset detected, adding {extra_delay}s extra delay")
                    await asyncio.sleep(extra_delay)
                
                await asyncio.sleep(backoff_time)
                backoff_time *= 2  # Exponential backoff
        
        # If we've exhausted all retries
        logger.error(f"Failed to find discussion thread after {max_retries} retries")
        return None, []

    @retry_with_backoff(retries=3, base_delay=5, exceptions=(Exception,))
    async def get_new_comments(self, submission_id: str, last_check_time: float = None) -> List[Message]:
        """Fetch only new comments since last check time."""
        # Set last_check_time to 1 hour ago if not provided
        if last_check_time is None:
            last_check_time = time.time() - 3600  # Current time minus 1 hour
            
        logger.info(f"Fetching new comments for submission {submission_id} since {datetime.fromtimestamp(last_check_time)}")
        
        max_retries = 3
        retry_count = 0
        backoff_time = 5  # Start with 5 seconds
        
        while retry_count < max_retries:
            try:
                # Add rate limiting wait
                await self._wait_for_rate_limit()
                
                # Fetch the submission
                logger.info(f"Fetching submission {submission_id} for new comments")
                submission = await self.reddit.submission(id=submission_id)
                logger.info(f"Successfully fetched submission: '{submission.title}' with {submission.num_comments} comments")
                
                # Use a more aggressive approach for replacing more comments
                try:
                    # Add rate limiting wait before fetching comments
                    await self._wait_for_rate_limit()
                    
                    # Use a higher limit for replace_more to get more comments
                    replace_limit = 30  # More aggressive for new comments
                    logger.info(f"Using replace_more limit of {replace_limit} for new comments")
                    
                    logger.info(f"Replacing more comments for submission {submission_id}")
                    await submission.comments.replace_more(limit=replace_limit)
                    logger.info(f"Successfully replaced more comments for submission {submission_id}")
                    
                    # Filter comments created after last_check_time
                    new_comments = []
                    
                    # Get all comments as a flat list
                    try:
                        logger.info(f"Getting flat comment list for submission {submission_id}")
                        all_comments = submission.comments.list()
                        logger.info(f"Successfully retrieved flat comment list with {len(all_comments)} comments")
                        
                        # Count comments by age
                        new_count = 0
                        old_count = 0
                        more_count = 0
                        
                        for comment in all_comments:
                            if isinstance(comment, asyncpraw.models.MoreComments):
                                more_count += 1
                                continue
                                
                            if comment.created_utc > last_check_time:
                                new_comments.append(comment)
                                new_count += 1
                            else:
                                old_count += 1
                        
                        logger.info(f"Comment age breakdown: {new_count} new, {old_count} old, {more_count} 'more comments' objects")
                        logger.info(f"Filtered {len(new_comments)} new comments out of {len(all_comments)} total comments")
                        
                        # If we didn't find any new comments but there should be some, try a different approach
                        if len(new_comments) == 0 and submission.num_comments > 0:
                            logger.warning(f"No new comments found despite thread having {submission.num_comments} comments. Trying alternative approach.")
                            
                            # Try a more aggressive approach
                            try:
                                # Create a fresh client for a clean fetch
                                logger.info("Creating a fresh client for alternative approach")
                                temp_reddit = asyncpraw.Reddit(
                                    client_id=os.getenv('REDDIT_CLIENT_ID'),
                                    client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
                                    user_agent=os.getenv('REDDIT_USER_AGENT', 'stocks_test 1.0')
                                )
                                
                                # Wait for rate limiting
                                await self._wait_for_rate_limit()
                                
                                # Fetch the submission again
                                logger.info(f"Re-fetching submission {submission_id}")
                                fresh_submission = await temp_reddit.submission(id=submission_id)
                                
                                # Wait for rate limiting
                                await self._wait_for_rate_limit()
                                
                                # Get comments with a more aggressive approach
                                logger.info("Fetching comments with more aggressive settings")
                                await fresh_submission.comments.replace_more(limit=None)
                                
                                # Get all comments
                                fresh_all_comments = fresh_submission.comments.list()
                                logger.info(f"Got {len(fresh_all_comments)} comments from fresh fetch")
                                
                                # Filter by timestamp
                                fresh_new_comments = []
                                for comment in fresh_all_comments:
                                    if not isinstance(comment, asyncpraw.models.MoreComments) and comment.created_utc > last_check_time:
                                        fresh_new_comments.append(comment)
                                
                                logger.info(f"Filtered to {len(fresh_new_comments)} new comments from fresh fetch")
                                
                                # Close the temporary client
                                await temp_reddit.close()
                                
                                # Use these comments if we got any
                                if len(fresh_new_comments) > 0:
                                    new_comments = fresh_new_comments
                            except Exception as fresh_error:
                                logger.error(f"Error with fresh fetch approach: {str(fresh_error)}")
                                logger.debug("Fresh fetch error details:", exc_info=True)
                        
                    except Exception as list_error:
                        logger.error(f"Error getting comment list for submission {submission_id}: {str(list_error)}")
                        logger.debug("Comment list error details:", exc_info=True)
                        raise
                    
                    # Process the new comments
                    logger.info(f"Processing {len(new_comments)} new comments")
                    comments = await self._process_comments(new_comments, None)
                    logger.info(f"Successfully processed {len(comments)} new comments")
                    
                    return comments
                    
                except Exception as comment_error:
                    logger.error(f"Error processing comments for submission {submission_id}: {str(comment_error)}")
                    raise
                    
            except asyncio.TimeoutError:
                retry_count += 1
                logger.warning(f"Timeout error fetching new comments for submission {submission_id}, retry {retry_count}/{max_retries}")
                await asyncio.sleep(backoff_time)
                backoff_time *= 2  # Exponential backoff
                
            except ConnectionError as conn_error:
                retry_count += 1
                logger.warning(f"Connection error fetching new comments for submission {submission_id}, retry {retry_count}/{max_retries}: {str(conn_error)}")
                await asyncio.sleep(backoff_time)
                backoff_time *= 2  # Exponential backoff
                
            except Exception as e:
                # For other exceptions, log and retry with backoff
                retry_count += 1
                logger.error(f"Error fetching new comments for submission {submission_id}, retry {retry_count}/{max_retries}: {str(e)}")
                
                # If this is a connection reset error, add extra delay
                if "Connection reset by peer" in str(e):
                    extra_delay = 30  # Add 30 seconds extra delay for connection reset errors
                    logger.warning(f"Connection reset detected, adding {extra_delay}s extra delay")
                    await asyncio.sleep(extra_delay)
                
                await asyncio.sleep(backoff_time)
                backoff_time *= 2  # Exponential backoff
        
        # If we've exhausted all retries
        logger.error(f"Failed to fetch new comments for submission {submission_id} after {max_retries} retries")
        return []