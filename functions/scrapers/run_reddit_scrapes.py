import os
import asyncio
from scrapers.reddit_scraper_v2 import RedditScraper
from dotenv import load_dotenv

load_dotenv()

async def check_scraper():
    # Create the scraper without using it as a context manager
    rs = RedditScraper()
    test_post, comments = await rs.fetch_post_with_comments('1jhdzhc')
    return comments

async def main():
    comments = await check_scraper()
    print(f"Number of comments: {len(comments)}")
    return comments

if __name__ == "__main__":
    # Run the async function with asyncio.run()
    result = asyncio.run(main())