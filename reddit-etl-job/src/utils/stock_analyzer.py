import re
import math
import logging
import pandas as pd
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
import spacy
from typing import List, Dict, Any, Set, Optional

from src.models.stock_data import StockMention

logger = logging.getLogger(__name__)

class StockAnalyzer:
    """
    Analyzes Reddit text for stock mentions and sentiment.
    """
    
    def __init__(self):
        """Initialize the stock analyzer."""
        # Download NLTK resources if not already present
        try:
            nltk.data.find('vader_lexicon')
        except LookupError:
            nltk.download('vader_lexicon')
            
        # Load spaCy model
        try:
            self.nlp = spacy.load("en_core_web_sm")
        except OSError:
            logger.info("Downloading spaCy model")
            spacy.cli.download("en_core_web_sm")
            self.nlp = spacy.load("en_core_web_sm")
            
        # Initialize sentiment analyzer
        self.sentiment_analyzer = SentimentIntensityAnalyzer()
        
        # Load stock tickers
        self.stock_tickers = self.load_stock_tickers()
    
    def load_stock_tickers(self) -> Set[str]:
        """
        Load a list of stock tickers from various exchanges.
        
        Returns:
            Set of stock tickers
        """
        logger.info("Loading stock tickers")
        tickers = set()
        
        # Common stock tickers we specifically want to track
        common_tickers = {
            'AAPL', 'MSFT', 'GOOGL', 'GOOG', 'AMZN', 'META', 'TSLA', 'NVDA', 'AMD', 'INTC',
            'GME', 'AMC', 'PLTR', 'TLRY', 'BB', 'NOK', 'SPY', 'QQQ', 'ARKK', 'BABA',
            'NIO', 'COIN', 'HOOD', 'SOFI', 'LCID', 'RIVN', 'SNAP', 'ABNB', 'RBLX', 'UBER'
        }
        tickers.update(common_tickers)
        
        try:
            # Add major index tickers
            indices = ['SPY', 'QQQ', 'DIA', 'IWM', 'VTI']
            tickers.update(indices)
            
            # Get top 100 stocks from S&P 500 by market cap
            # This could potentially be slow, but it's a one-time operation
            sp500 = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
            sp500_tickers = sp500['Symbol'].str.upper().tolist()
            tickers.update(sp500_tickers[:100])  # Top 100 by position in the list
            
            logger.info(f"Loaded {len(tickers)} stock tickers")
        except Exception as e:
            logger.error(f"Error loading additional stock tickers: {str(e)}")
            logger.info(f"Continuing with {len(tickers)} basic tickers")
        
        return tickers
    
    def extract_stock_mentions(self, text: str) -> List[str]:
        """
        Extract stock tickers mentioned in text.
        
        Args:
            text: Text to analyze
            
        Returns:
            List of mentioned stock tickers
        """
        if not text or not isinstance(text, str):
            return []
        
        # Extract potential tickers using a regex pattern for ticker-like mentions
        # Look for uppercase sequences that could be tickers
        ticker_pattern = r'\b([A-Z]{1,5})\b'
        potential_tickers = re.findall(ticker_pattern, text)
        
        # Filter for actual tickers
        mentioned_tickers = [ticker for ticker in potential_tickers if ticker in self.stock_tickers]
        
        # Also look for specific formats like $AAPL
        dollar_ticker_pattern = r'\$([A-Z]{1,5})\b'
        dollar_tickers = re.findall(dollar_ticker_pattern, text)
        for ticker in dollar_tickers:
            if ticker in self.stock_tickers and ticker not in mentioned_tickers:
                mentioned_tickers.append(ticker)
        
        return mentioned_tickers
    
    def analyze_sentiment(self, text: str, ticker: str, score: int = 0) -> Dict[str, Any]:
        """
        Analyze sentiment for a specific stock mentioned in text.
        
        Args:
            text: The full text containing the stock mention
            ticker: The stock ticker to analyze
            score: The Reddit post/comment score (upvotes)
            
        Returns:
            Dictionary with sentiment analysis results
        """
        # Extract sentences containing the ticker
        ticker_context = self.extract_ticker_context(text, ticker)
        
        if not ticker_context:
            # If no specific context, analyze the full text
            sentiment = self.sentiment_analyzer.polarity_scores(text)
            return {
                'compound': sentiment['compound'],
                'positive': sentiment['pos'],
                'negative': sentiment['neg'],
                'neutral': sentiment['neu'],
                'context': text[:200] if len(text) > 200 else text,  # Limit context length
                'signals': [],
                'confidence': self.calc_confidence_score(sentiment['compound'], score)
            }
        
        # Analyze sentiment of the ticker context
        sentiment = self.sentiment_analyzer.polarity_scores(ticker_context)
        
        # Extract key phrases about the stock
        doc = self.nlp(ticker_context)
        key_phrases = []
        
        # Advanced signal detection with confidence levels
        for sentence in doc.sents:
            sentence_text = sentence.text.strip()
            if ticker in sentence_text or f"${ticker}" in sentence_text:
                # Trading signals - Buy indicators
                if re.search(r'\b(buy|bought|buying|long|calls|bullish|moon|rocket|🚀|💎|🙌|going up|to the moon|undervalued|cheap|discount)\b', sentence_text, re.IGNORECASE):
                    key_phrases.append("BUY")
                
                # Trading signals - Sell indicators
                elif re.search(r'\b(sell|selling|sold|short|puts|bearish|crash|dump|tank|dropping|overvalued|expensive|bubble|correction|margin call)\b', sentence_text, re.IGNORECASE):
                    key_phrases.append("SELL")
                
                # Trading signals - Hold indicators
                elif re.search(r'\b(hold|holding|hodl|diamond hands|patient|patience|long term|longterm)\b', sentence_text, re.IGNORECASE):
                    key_phrases.append("HOLD")
                
                # Price target mentions
                price_targets = self.extract_price_targets(sentence_text, ticker)
                if price_targets:
                    for target_type, value in price_targets.items():
                        key_phrases.append(f"{target_type}:{value}")
                
                # Earnings/financial performance signals
                if re.search(r'\b(earnings|revenue|growth|profit|loss|guidance|forecast|EPS|P/E|dividend)\b', sentence_text, re.IGNORECASE):
                    key_phrases.append("EARNINGS")
                
                # News/catalyst signals
                if re.search(r'\b(news|announcement|released|launched|partnership|acquisition|merger|FDA|approval|patent|lawsuit)\b', sentence_text, re.IGNORECASE):
                    key_phrases.append("NEWS")
                
                # Technical analysis signals
                if re.search(r'\b(resistance|support|trend|breakout|pattern|cup|handle|head|shoulders|triangle|wedge|channel|RSI|MACD|oversold|overbought)\b', sentence_text, re.IGNORECASE):
                    key_phrases.append("TECHNICAL")
                
                # Options-related signals
                if re.search(r'\b(option|call|put|strike|expiry|contracts|leaps|covered|naked|straddle|strangle|iron condor|spread)\b', sentence_text, re.IGNORECASE):
                    key_phrases.append("OPTIONS")
        
        # Calculate confidence score based on sentiment and Reddit score (upvotes)
        confidence = self.calc_confidence_score(sentiment['compound'], score)
        
        return {
            'compound': sentiment['compound'],
            'positive': sentiment['pos'],
            'negative': sentiment['neg'],
            'neutral': sentiment['neu'],
            'signals': key_phrases,
            'context': ticker_context[:200] if len(ticker_context) > 200 else ticker_context,
            'confidence': confidence
        }
    
    def calc_confidence_score(self, sentiment_score: float, reddit_score: int) -> float:
        """
        Calculate a confidence score based on sentiment strength and Reddit upvotes.
        
        Args:
            sentiment_score: VADER compound sentiment score (-1 to 1)
            reddit_score: Reddit post/comment score (upvotes)
            
        Returns:
            float: Confidence score (0 to 1)
        """
        # Normalize sentiment strength (absolute value of compound score)
        sentiment_strength = abs(sentiment_score)
        
        # Apply logarithmic scaling to Reddit score to handle viral posts
        # log(1) = 0, so we add 1 to avoid log(0)
        score_factor = min(1.0, math.log(abs(reddit_score) + 1) / 10) if reddit_score != 0 else 0
        
        # Weight the sentiment strength more than the Reddit score
        confidence = (0.7 * sentiment_strength) + (0.3 * score_factor)
        
        return round(confidence, 2)
    
    def extract_price_targets(self, text: str, ticker: str) -> Dict[str, float]:
        """
        Extract price targets mentioned for a stock.
        
        Args:
            text: Text to analyze
            ticker: Stock ticker
            
        Returns:
            Dictionary with target types and values
        """
        targets = {}
        
        # Look for price targets in various formats
        # e.g. "$AAPL price target $150", "TSLA to $420"
        
        # General price target pattern
        pt_pattern = fr'(?:{ticker}|${ticker}).*?(?:price target|target price|PT)\s*\$?(\d+(?:\.\d+)?)'
        pt_matches = re.findall(pt_pattern, text, re.IGNORECASE)
        if pt_matches:
            targets['PT'] = float(pt_matches[0])
        
        # "to $X" pattern (often implies a target)
        to_pattern = fr'(?:{ticker}|${ticker}).*?to\s+\$(\d+(?:\.\d+)?)'
        to_matches = re.findall(to_pattern, text, re.IGNORECASE)
        if to_matches and 'PT' not in targets:
            targets['PT'] = float(to_matches[0])
        
        return targets
    
    def extract_ticker_context(self, text: str, ticker: str, window_size: int = 150) -> str:
        """
        Extract text surrounding the ticker mention for better context.
        
        Args:
            text: Full text
            ticker: Stock ticker
            window_size: Number of characters to include before and after ticker
            
        Returns:
            Context around ticker mentions
        """
        contexts = []
        
        # Look for ticker mentions (both with and without $ prefix)
        ticker_patterns = [rf'\b{ticker}\b', rf'\${ticker}\b']
        
        for pattern in ticker_patterns:
            for match in re.finditer(pattern, text):
                start = max(0, match.start() - window_size)
                end = min(len(text), match.end() + window_size)
                context = text[start:end]
                contexts.append(context)
        
        if contexts:
            return " ".join(contexts)
        return ""
    
    def process_reddit_data(self, df: pd.DataFrame) -> List[StockMention]:
        """
        Process Reddit data to identify stock mentions and analyze sentiment.
        
        Args:
            df: DataFrame with Reddit data
            
        Returns:
            List of StockMention objects
        """
        if df.empty:
            return []
        
        # Initialize list to store stock mentions
        stock_mentions = []
        
        # Process each Reddit post/comment
        total_rows = len(df)
        logger.info(f"Processing {total_rows} Reddit posts for stock mentions")
        
        for index, row in df.iterrows():
            if index % 1000 == 0:
                logger.info(f"Processing Reddit post {index+1}/{total_rows}")
            
            # Combine title and content for posts
            text = row['content']
            if pd.notna(row['title']) and row['title']:
                text = f"{row['title']} {text}"
            
            # Skip if text is empty or not a string
            if not isinstance(text, str) or not text.strip():
                continue
            
            # Extract stock tickers mentioned in the text
            mentioned_tickers = self.extract_stock_mentions(text)
            
            # Process each mentioned ticker
            for ticker in mentioned_tickers:
                # Analyze sentiment for this ticker, passing the score
                score = row['score'] if isinstance(row['score'], (int, float)) else 0
                sentiment = self.analyze_sentiment(text, ticker, score)
                
                # Create stock mention object
                mention = StockMention(
                    message_id=row['message_id'],
                    ticker=ticker,
                    author=row['author'],
                    created_at=row['created_at'],
                    subreddit=row['subreddit'],
                    url=row['url'],
                    score=score,
                    message_type=row['message_type'],
                    sentiment_compound=sentiment['compound'],
                    sentiment_positive=sentiment['positive'],
                    sentiment_negative=sentiment['negative'],
                    sentiment_neutral=sentiment['neutral'],
                    signals=sentiment.get('signals', []),
                    context=sentiment['context'],
                    confidence=sentiment.get('confidence', 0.0)
                )
                
                stock_mentions.append(mention)
        
        logger.info(f"Identified {len(stock_mentions)} stock mentions in Reddit data")
        
        return stock_mentions 