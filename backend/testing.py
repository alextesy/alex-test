from backend.scraper_service.src.scrapers.reddit_scraper import RedditScraper

reddit_scraper = RedditScraper()
comments = reddit_scraper.get_daily_discussion_comments(limit=5)
print(comments)