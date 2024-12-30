import time
from datetime import datetime
from typing import List
import requests
from bs4 import BeautifulSoup
from models.message import Message, CNBCArticle
from .base_scraper import SocialMediaScraper

class CNBCScraper(SocialMediaScraper):
    """Scraper for CNBC news articles"""
    
    BASE_URL = "https://www.cnbc.com"
    SEARCH_URL = f"{BASE_URL}/search"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })

    def get_posts(self, query: str, limit: int = 100) -> List[Message]:
        """
        Get CNBC articles matching a query
        
        Args:
            query (str): Search query
            limit (int): Maximum number of articles to return
            
        Returns:
            List[Message]: List of matching articles
        """
        articles = []
        params = {
            'query': query,
            'type': 'article'
        }
        
        try:
            response = self.session.get(self.SEARCH_URL, params=params)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find all article cards/containers
            article_elements = soup.find_all('div', class_='SearchResult-searchResult')[:limit]
            
            for article in article_elements:
                try:
                    # Extract article data
                    title = article.find('a', class_='ResultLink').text.strip()
                    url = self.BASE_URL + article.find('a', class_='ResultLink')['href']
                    timestamp = self._extract_timestamp(article)
                    
                    # Get full article content
                    article_data = self._get_article_details(url)
                    
                    articles.append(CNBCArticle(
                        id=url.split('/')[-1],
                        content=article_data['content'],
                        title=title,
                        author=article_data['author'],
                        author_title=article_data['author_title'],
                        timestamp=timestamp,
                        url=url,
                        score=0,  # CNBC doesn't have a scoring system
                        platform='cnbc',
                        source='cnbc',
                        summary=article_data['summary'],
                        category=article_data['category']
                    ))
                except Exception as e:
                    print(f"Error processing article: {str(e)}")
                    continue
                    
            return articles
            
        except Exception as e:
            print(f"Error fetching CNBC articles: {str(e)}")
            return []
    
    def _extract_timestamp(self, article_element) -> float:
        """Extract timestamp from article element"""
        try:
            date_text = article_element.find('span', class_='SearchResult-publishedDate').text.strip()
            date_obj = datetime.strptime(date_text, '%b %d %Y')
            return date_obj.timestamp()
        except:
            return datetime.utcnow().timestamp()
    
    def _get_article_details(self, url: str) -> dict:
        """Get full article details from article page"""
        try:
            response = self.session.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract article content
            content_elements = soup.find_all(['p', 'h2'], class_='ArticleBody-paragraph')
            content = ' '.join([elem.text.strip() for elem in content_elements])
            
            # Extract author info
            author_element = soup.find('a', class_='Author-authorName')
            author = author_element.text.strip() if author_element else 'CNBC Staff'
            
            author_title_element = soup.find('span', class_='Author-authorTitle')
            author_title = author_title_element.text.strip() if author_title_element else ''
            
            # Extract category
            category_element = soup.find('a', class_='ArticleHeader-eyebrow')
            category = category_element.text.strip() if category_element else ''
            
            # Extract summary
            summary_element = soup.find('div', class_='ArticleHeader-summary')
            summary = summary_element.text.strip() if summary_element else ''
            
            return {
                'content': content,
                'author': author,
                'author_title': author_title,
                'category': category,
                'summary': summary
            }
            
        except Exception as e:
            print(f"Error getting article details: {str(e)}")
            return {
                'content': '',
                'author': 'CNBC Staff',
                'author_title': '',
                'category': '',
                'summary': ''
            } 