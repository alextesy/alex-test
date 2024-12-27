import praw
import os
from dotenv import load_dotenv
from typing import List
from base_scraper import SocialMediaScraper
from models.message import RedditPost, RedditComment, Message

load_dotenv()

class RedditScraper(SocialMediaScraper):
    def __init__(self):
        self.reddit = praw.Reddit(
            client_id=os.getenv('REDDIT_CLIENT_ID'),
            client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
            user_agent=os.getenv('REDDIT_USER_AGENT', 'stocks_test 1.0')
        )
        # Default stock-related subreddits
        self.stock_subreddits = ['wallstreetbets', 'stocks', 'investing', 'stockmarket', 'options']

    def _get_subreddit_posts(self, subreddit_name: str, limit: int = 100, sort: str = 'hot', time_filter: str = None) -> List[Message]:
        """
        Private method to fetch posts from a specified subreddit.
        
        Args:
            subreddit_name (str): Name of the subreddit to fetch posts from
            limit (int): Maximum number of posts to return (default: 100)
            sort (str): Sort method ('hot', 'new', 'top', 'rising') (default: 'hot')
            time_filter (str): One of 'all', 'day', 'hour', 'month', 'week', 'year'.
                              Only applies to 'top' and 'controversial' sorts. (default: None)
            
        Returns:
            List[Message]: List of Reddit posts from the specified subreddit
        """
        posts = []
        
        try:
            subreddit = self.reddit.subreddit(subreddit_name)
            
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
                
            for post in submissions:
                post_obj = RedditPost(
                    id=post.id,
                    content=post.selftext,
                    author=str(post.author) if post.author else '[deleted]',
                    timestamp=post.created_utc,
                    url=post.url,
                    score=post.score,
                    platform='reddit',
                    source=f"reddit/r/{subreddit_name}",
                    title=post.title,
                    selftext=post.selftext,
                    num_comments=post.num_comments,
                    subreddit=subreddit_name
                )
                posts.append(post_obj)
                
        except Exception as e:
            print(f"Error fetching posts from r/{subreddit_name}: {str(e)}")
            
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

    def _process_comments(self, submission, limit: int = 100) -> List[Message]:
        """
        Process comments from a Reddit submission.
        
        Args:
            submission: PRAW submission object
            limit (int): Maximum number of comments to process
            
        Returns:
            List[Message]: List of processed comments
        """
        comments = []
        submission.comments.replace_more(limit=0)  # Remove MoreComments objects
        
        def process_comment(comment, depth=0):
            if len(comments) >= limit:
                return
                
            comment_obj = RedditComment(
                id=comment.id,
                content=comment.body,
                author=str(comment.author) if comment.author else '[deleted]',
                timestamp=comment.created_utc,
                url=f"https://reddit.com{comment.permalink}",
                score=comment.score,
                platform='reddit',
                source=f"reddit/r/{comment.subreddit.display_name}",
                parent_id=comment.parent_id,
                depth=depth
            )
            comments.append(comment_obj)
            
            # Process replies recursively
            for reply in comment.replies:
                process_comment(reply, depth + 1)
                
        # Process top-level comments
        for comment in submission.comments[:limit]:
            process_comment(comment)
            
        return comments

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
                url=submission.url,
                score=submission.score,
                platform='reddit',
                source=f"reddit/r/{submission.subreddit.display_name}",
                title=submission.title,
                selftext=submission.selftext,
                num_comments=submission.num_comments,
                subreddit=submission.subreddit.display_name
            )
            
            comments = self._process_comments(submission, comment_limit)
            return post, comments
            
        except Exception as e:
            print(f"Error fetching post and comments for ID {post_id}: {str(e)}")
            return None, []