import logging
import os
import time
import praw
from dotenv import load_dotenv
from typing import List, Optional, Tuple
from models.message import RedditPost, RedditComment, Message
from models.database_models import MessageType
from .base_scraper import SocialMediaScraper
from datetime import datetime

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
            logger.info("Successfully initialized Reddit API client")
        except Exception as e:
            logger.error(f"Failed to initialize Reddit API client: {str(e)}")
            raise
            
        # Default stock-related subreddits
        self.stock_subreddits = ['wallstreetbets', 'stocks', 'investing', 'stockmarket', 'options']
        logger.info(f"Monitoring subreddits: {', '.join(self.stock_subreddits)}")

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

    def _process_comments(self, comments_list, limit: int = None) -> List[Message]:
        """Process comments from a Reddit submission."""
        logger.info(f"Processing comments (limit={limit})")
        processed_comments = []
        
        def process_comment(comment, depth=0):
            if limit and len(processed_comments) >= limit:
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
                
                # Process replies recursively
                for reply in comment.replies:
                    process_comment(reply, depth + 1)
            except Exception as e:
                logger.error(f"Error processing comment {comment.id}: {str(e)}")
                
        # Process all comments
        try:
            for comment in comments_list:
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

    def get_daily_discussion_comments(self, limit: int = None) -> tuple[Message, List[Message]]:
        """Find and fetch the most recent Daily Discussion Thread from wallstreetbets."""
        logger.info("Searching for today's Daily Discussion Thread")
        try:
            # Get today's date in the format "December 27, 2024"
            today = datetime.now().strftime("%B %d, %Y")
            search_title = f"Daily Discussion Thread for {today}"
            logger.debug(f"Searching for thread with title: {search_title}")
            
            # Search in wallstreetbets subreddit
            subreddit = self.reddit.subreddit('wallstreetbets')
            submissions = subreddit.search(query=search_title, limit=200, sort='new', time_filter='day')
            
            # Search through today's posts
            for submission in submissions:
                if submission.title.startswith("Daily Discussion Thread for") or submission.title.startswith("Weekend Discussion Thread for"):
                    logger.info(f"Found discussion thread: {submission.title}")
                    
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
                        subreddit='wallstreetbets'
                    )
                    
                    # Get all comments
                    logger.info(f"Replacing MoreComments objects, limit={limit}")
                    submission.comments.replace_more(limit=limit)
                    comments = self._process_comments(submission.comments, limit=limit)
                    
                    logger.info(f"Successfully retrieved discussion thread with {len(comments)} comments")
                    return post, comments
            
            logger.warning("No daily discussion thread found for today")
            return None, []
            
        except Exception as e:
            logger.error(f"Error fetching daily discussion thread: {str(e)}")
            return None, []
        
    def get_new_comments(self, submission_id: str, last_check_time: float = None) -> List[Message]:
        """Fetch only new comments since last check time."""
        try:
            # Set last_check_time to 1 hour ago if not provided
            if last_check_time is None:
                last_check_time = time.time() - 3600  # Current time minus 1 hour
                
            submission = self.reddit.submission(id=submission_id)
            submission.comments.replace_more(limit=None)
            
            # Filter comments created after last_check_time
            new_comments = []
            for comment in submission.comments.list():
                if comment.created_utc > last_check_time:
                    new_comments.append(comment)
            comments = self._process_comments(submission)

            logger.info(f"Found {len(comments)} new comments")
            return comments
            
        except Exception as e:
            logger.error(f"Error fetching new comments: {str(e)}")
            return []