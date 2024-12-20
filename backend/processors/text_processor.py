from typing import Set, Tuple
import re
from nltk.sentiment import SentimentIntensityAnalyzer
from transformers import pipeline
import nltk
from functools import lru_cache

class TextProcessor:
    """Processes text to extract information and analyze sentiment"""
    
    def __init__(self):
        # Download required NLTK data
        try:
            nltk.data.find('vader_lexicon')
        except LookupError:
            nltk.download('vader_lexicon')
        
        self.sia = SentimentIntensityAnalyzer()
        
        # Initialize the transformer model for more accurate sentiment analysis
        try:
            self.sentiment_pipeline = pipeline(
                "sentiment-analysis",
                model="ProsusAI/finbert",  # Financial domain-specific BERT
                max_length=512,
                truncation=True
            )
        except Exception as e:
            print(f"Warning: Could not load FinBERT model: {e}")
            self.sentiment_pipeline = None

    @staticmethod
    def extract_stock_mentions(text: str, title: str = "") -> Set[str]:
        """
        Extract stock mentions from text and title.
        
        Args:
            text (str): Main content text
            title (str): Optional title text
            
        Returns:
            Set[str]: Set of unique stock symbols mentioned
        """
        # Combine text and title for processing
        full_text = f"{title} {text}"
        
        # Common words that might be mistaken for tickers
        common_words = {
            'A', 'I', 'AT', 'BE', 'DO', 'IT', 'ARE', 'FOR', 'CEO', 'API',
            'AI', 'AM', 'PM', 'USA', 'GDP', 'IPO', 'CEO', 'CFO', 'CTO',
            'RSS', 'ETF', 'DIY', 'FAQ', 'ASAP', 'EDIT', 'ELI5'
        }
        
        # Find cashtag mentions ($AAPL)
        cashtag_pattern = r'\$([A-Z]{2,5})\b'
        cashtag_matches = set(re.findall(cashtag_pattern, full_text))
        
        # Find standalone uppercase ticker mentions (AAPL)
        ticker_pattern = r'\b([A-Z]{2,5})\b'
        ticker_matches = {
            match for match in re.findall(ticker_pattern, full_text)
            if match not in common_words
        }
        
        return cashtag_matches.union(ticker_matches)

    @lru_cache(maxsize=1000)
    def get_sentiment_nltk(self, text: str) -> float:
        """
        Get sentiment score using NLTK's VADER.
        Returns a score between -1 (negative) and 1 (positive).
        """
        scores = self.sia.polarity_scores(text)
        return scores['compound']

    @lru_cache(maxsize=1000)
    def get_sentiment_finbert(self, text: str) -> float:
        """
        Get sentiment score using FinBERT.
        Returns a score between -1 (negative) and 1 (positive).
        """
        if not self.sentiment_pipeline:
            return 0.0
            
        try:
            # Truncate text if it's too long
            text = text[:512] if len(text) > 512 else text
            result = self.sentiment_pipeline(text)[0]
            
            # Convert FinBERT labels to scores
            label_scores = {
                'positive': 1.0,
                'negative': -1.0,
                'neutral': 0.0
            }
            return label_scores[result['label']] * result['score']
            
        except Exception as e:
            print(f"Error in FinBERT sentiment analysis: {e}")
            return 0.0

    def analyze_text(self, text: str, title: str = "") -> Tuple[Set[str], float]:
        """
        Analyze text to extract stock mentions and sentiment.
        
        Args:
            text (str): Main content text
            title (str): Optional title text
            
        Returns:
            Tuple[Set[str], float]: (stock mentions, sentiment score)
        """
        # Extract stock mentions
        stocks = self.extract_stock_mentions(text, title)
        
        # Combine text for sentiment analysis
        full_text = f"{title} {text}".strip()
        
        # Try FinBERT first, fall back to NLTK if it fails
        sentiment = self.get_sentiment_finbert(full_text)
        if sentiment == 0.0:  # If FinBERT failed or returned neutral
            sentiment = self.get_sentiment_nltk(full_text)
            
        return stocks, sentiment 