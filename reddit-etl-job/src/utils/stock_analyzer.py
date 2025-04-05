import re
import math
import logging
import pandas as pd
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
import spacy
from typing import List, Dict, Any, Set, Optional, Tuple
from datetime import datetime
import os
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

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
        Load stock tickers from BigQuery or fallback sources.
        
        Returns:
            Set of stock tickers
        """
        logger.info("Loading stock tickers")
        
        # Initialize with common tickers as fallback
        tickers = self._get_common_tickers()
        
        try:
            # Connect to BigQuery
            project_id = os.getenv('GOOGLE_CLOUD_PROJECT_ID')
            dataset_id = os.getenv('BIGQUERY_DATASET')
            client = bigquery.Client(project=project_id)
            table_id = f"{project_id}.{dataset_id}.stock_tickers"
            
            # Create table if it doesn't exist
            if not self._ensure_table_exists(client, project_id, dataset_id, table_id):
                # If newly created, populate with data
                all_stocks = self._scrape_exchange_tickers()
                if all_stocks:
                    self._batch_insert_tickers(client, table_id, all_stocks)
            
            # Load tickers from BigQuery
            tickers = self._load_tickers_from_bigquery(client, project_id, dataset_id)
            
        except Exception as e:
            logger.error(f"Error loading stock tickers from BigQuery: {str(e)}")
            logger.info("Falling back to alternative ticker sources")
            tickers = self._load_fallback_tickers()
        
        return tickers
    
    def _get_common_tickers(self) -> Set[str]:
        """Get a set of common stock tickers as a baseline."""
        common_tickers = {
            'AAPL', 'MSFT', 'GOOGL', 'GOOG', 'AMZN', 'META', 'TSLA', 'NVDA', 'AMD', 'INTC',
            'GME', 'AMC', 'PLTR', 'TLRY', 'BB', 'NOK', 'SPY', 'QQQ', 'ARKK', 'BABA',
            'NIO', 'COIN', 'HOOD', 'SOFI', 'LCID', 'RIVN', 'SNAP', 'ABNB', 'RBLX', 'UBER'
        }
        return set(common_tickers)
    
    def _ensure_table_exists(self, client: bigquery.Client, project_id: str, 
                             dataset_id: str, table_id: str) -> bool:
        """
        Check if stock_tickers table exists, create if it doesn't.
        
        Returns:
            bool: True if table already existed, False if it was newly created
        """
        try:
            client.get_table(table_id)
            logger.info("Stock tickers table already exists")
            return True
        except NotFound:
            # Create table schema
            schema = [
                bigquery.SchemaField("ticker", "STRING"),
                bigquery.SchemaField("exchange", "STRING"),
                bigquery.SchemaField("company_name", "STRING"),
                bigquery.SchemaField("last_updated", "TIMESTAMP")
            ]
            
            # Ensure dataset exists
            dataset_ref = client.dataset(dataset_id)
            try:
                client.get_dataset(dataset_ref)
            except NotFound:
                # Create dataset
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = os.getenv('GCP_REGION', 'US')
                client.create_dataset(dataset)
                logger.info(f"Created BigQuery dataset {dataset_id}")
            
            # Create table
            table = bigquery.Table(table_id, schema=schema)
            client.create_table(table)
            logger.info(f"Created stock tickers table {table_id}")
            return False
    
    def _scrape_exchange_tickers(self) -> List[Dict[str, Any]]:
        """
        Scrape stock tickers from major exchanges.
        
        Returns:
            List of stock data dictionaries
        """
        logger.info("Downloading stock tickers from exchanges")
        all_stocks = []
        
        # Define exchanges to scrape
        exchanges = [
            ('NASDAQ', 'https://www.nasdaq.com/market-activity/stocks/screener?exchange=NASDAQ&render=download'),
            ('NYSE', 'https://www.nasdaq.com/market-activity/stocks/screener?exchange=NYSE&render=download'),
            ('AMEX', 'https://www.nasdaq.com/market-activity/stocks/screener?exchange=AMEX&render=download')
        ]
        
        # Process each exchange
        for exchange, url in exchanges:
            try:
                exchange_stocks = pd.read_csv(url)
                # Filter and clean tickers
                for _, row in exchange_stocks.iterrows():
                    ticker = row['Symbol'].strip().upper()
                    # Only add tickers that match our pattern (1-5 letters)
                    if re.match(r'^[A-Z]{1,5}$', ticker):
                        all_stocks.append({
                            'ticker': ticker,
                            'exchange': exchange,
                            'company_name': row['Name'] if 'Name' in row else '',
                            'last_updated': datetime.now().isoformat()
                        })
                logger.info(f"Downloaded {len(exchange_stocks)} {exchange} stocks")
            except Exception as e:
                logger.error(f"Error downloading {exchange} stocks: {str(e)}")
        
        logger.info(f"Total of {len(all_stocks)} unique stock tickers scraped")
        return all_stocks
    
    def _batch_insert_tickers(self, client: bigquery.Client, table_id: str, 
                              stocks: List[Dict[str, Any]]) -> None:
        """
        Insert stock tickers into BigQuery in efficient batches.
        """
        if not stocks:
            logger.warning("No stocks to insert")
            return
        
        # Insert in batches for better performance
        batch_size = 1000
        for i in range(0, len(stocks), batch_size):
            batch = stocks[i:i+batch_size]
            errors = client.insert_rows_json(table_id, batch)
            if errors:
                logger.error(f"Error inserting stock tickers: {errors}")
            else:
                logger.info(f"Inserted batch of {len(batch)} stock tickers")
        
        logger.info(f"Completed inserting {len(stocks)} stock tickers")
    
    def _load_tickers_from_bigquery(self, client: bigquery.Client, 
                                    project_id: str, dataset_id: str) -> Set[str]:
        """
        Load tickers from BigQuery table.
        
        Returns:
            Set of stock tickers
        """
        logger.info("Loading stock tickers from BigQuery")
        tickers = self._get_common_tickers()  # Start with common tickers
        
        query = f"""
        SELECT ticker FROM `{project_id}.{dataset_id}.stock_tickers`
        """
        
        query_job = client.query(query)
        results = query_job.result()
        
        for row in results:
            ticker = row.ticker
            # Double-check that ticker follows our pattern (1-5 letters)
            if re.match(r'^[A-Z]{1,5}$', ticker):
                tickers.add(ticker)
        
        logger.info(f"Loaded {len(tickers)} stock tickers from BigQuery")
        return tickers
    
    def _load_fallback_tickers(self) -> Set[str]:
        """
        Load tickers from fallback sources when BigQuery isn't available.
        
        Returns:
            Set of stock tickers
        """
        logger.info("Loading tickers from fallback sources")
        tickers = self._get_common_tickers()
        
        try:
            # Add major index tickers
            indices = ['SPY', 'QQQ', 'DIA', 'IWM', 'VTI']
            tickers.update(indices)
            
            # Get stocks from S&P 500
            sp500 = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
            sp500_tickers = sp500['Symbol'].str.upper().tolist()
            for ticker in sp500_tickers:
                # Ensure ticker format compliance
                if re.match(r'^[A-Z]{1,5}$', ticker):
                    tickers.add(ticker)
            
            logger.info(f"Loaded {len(tickers)} stock tickers from fallback sources")
        except Exception as e:
            logger.error(f"Error loading fallback tickers: {str(e)}")
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
        
        # For very long texts, only look at the first 2000 characters
        # Most meaningful mentions occur early in a post
        if len(text) > 2000:
            text = text[:2000]
        
        # Combine both patterns to reduce regex passes (more efficient)
        combined_pattern = r'(?:\$?([A-Z]{1,5}))\b'
        all_matches = set(re.findall(combined_pattern, text))
        
        # Use set intersection for faster lookup
        mentioned_tickers = list(all_matches.intersection(self.stock_tickers))
        
        # Limit to 10 unique tickers per post to avoid spam
        if len(mentioned_tickers) > 10:
            return mentioned_tickers[:10]
        
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
        # Extract sentences containing the ticker - this is expensive so limit context size
        ticker_context = self.extract_ticker_context(text, ticker, window_size=100)
        
        # If no specific context found, analyze a portion of the full text
        # Limiting text size for performance
        if not ticker_context:
            # Limit text length to reduce processing time
            limited_text = text[:500] if len(text) > 500 else text
            sentiment = self.sentiment_analyzer.polarity_scores(limited_text)
            return {
                'compound': sentiment['compound'],
                'positive': sentiment['pos'],
                'negative': sentiment['neg'],
                'neutral': sentiment['neu'],
                'context': limited_text[:200] if len(limited_text) > 200 else limited_text,
                'signals': [],
                'confidence': self.calc_confidence_score(sentiment['compound'], score)
            }
        
        # Analyze sentiment of the ticker context
        sentiment = self.sentiment_analyzer.polarity_scores(ticker_context)
        
        # Only run expensive NLP for high-engagement posts (high scores)
        # or posts with strong sentiment
        signals = []
        if abs(sentiment['compound']) > 0.3 or score > 10:
            # Extract key phrases about the stock using regex instead of full NLP when possible
            signals = self.extract_signals_regex(ticker_context, ticker)
            
            # Only use spaCy for high-value content with strong signals
            if (abs(sentiment['compound']) > 0.5 or score > 50) and len(signals) == 0:
                signals = self.extract_signals_spacy(ticker_context, ticker)
        
        # Calculate confidence score based on sentiment and Reddit score (upvotes)
        confidence = self.calc_confidence_score(sentiment['compound'], score)
        
        return {
            'compound': sentiment['compound'],
            'positive': sentiment['pos'],
            'negative': sentiment['neg'],
            'neutral': sentiment['neu'],
            'signals': signals,
            'context': ticker_context[:200] if len(ticker_context) > 200 else ticker_context,
            'confidence': confidence
        }
    
    def extract_signals_regex(self, text: str, ticker: str) -> List[str]:
        """Extract trading signals using regex patterns (faster than NLP)"""
        signals = []
        
        # Trading signals - Buy indicators
        if re.search(r'\b(buy|bought|buying|long|calls|bullish|moon|rocket|🚀|💎|🙌|going up|to the moon|undervalued|cheap|discount)\b', text, re.IGNORECASE):
            signals.append("BUY")
        
        # Trading signals - Sell indicators
        if re.search(r'\b(sell|selling|sold|short|puts|bearish|crash|dump|tank|dropping|overvalued|expensive|bubble|correction|margin call)\b', text, re.IGNORECASE):
            signals.append("SELL")
        
        # Trading signals - Hold indicators
        if re.search(r'\b(hold|holding|hodl|diamond hands|patient|patience|long term|longterm)\b', text, re.IGNORECASE):
            signals.append("HOLD")
        
        # Price target mentions - simplified to just check for price values
        price_targets = self.extract_price_targets(text, ticker)
        if price_targets:
            for target_type, value in price_targets.items():
                signals.append(f"{target_type}:{value}")
        
        # Earnings/financial performance signals
        if re.search(r'\b(earnings|revenue|growth|profit|loss|guidance|forecast|EPS|P/E|dividend)\b', text, re.IGNORECASE):
            signals.append("EARNINGS")
        
        # News/catalyst signals
        if re.search(r'\b(news|announcement|released|launched|partnership|acquisition|merger|FDA|approval|patent|lawsuit)\b', text, re.IGNORECASE):
            signals.append("NEWS")
        
        return signals
    
    def extract_signals_spacy(self, text: str, ticker: str) -> List[str]:
        """Extract trading signals using NLP (slower but more accurate)"""
        signals = []
        
        # Use spaCy to process the text
        doc = self.nlp(text)
        
        # Advanced signal detection with confidence levels
        for sentence in doc.sents:
            sentence_text = sentence.text.strip()
            if ticker in sentence_text or f"${ticker}" in sentence_text:
                # Trading signals - Buy indicators
                if re.search(r'\b(buy|bought|buying|long|calls|bullish|moon|rocket|🚀|💎|🙌|going up|to the moon|undervalued|cheap|discount)\b', sentence_text, re.IGNORECASE):
                    signals.append("BUY")
                
                # Trading signals - Sell indicators
                elif re.search(r'\b(sell|selling|sold|short|puts|bearish|crash|dump|tank|dropping|overvalued|expensive|bubble|correction|margin call)\b', sentence_text, re.IGNORECASE):
                    signals.append("SELL")
                
                # Trading signals - Hold indicators
                elif re.search(r'\b(hold|holding|hodl|diamond hands|patient|patience|long term|longterm)\b', sentence_text, re.IGNORECASE):
                    signals.append("HOLD")
                
                # Technical analysis signals
                if re.search(r'\b(resistance|support|trend|breakout|pattern|cup|handle|head|shoulders|triangle|wedge|channel|RSI|MACD|oversold|overbought)\b', sentence_text, re.IGNORECASE):
                    signals.append("TECHNICAL")
                
                # Options-related signals
                if re.search(r'\b(option|call|put|strike|expiry|contracts|leaps|covered|naked|straddle|strangle|iron condor|spread)\b', sentence_text, re.IGNORECASE):
                    signals.append("OPTIONS")
        
        return signals
    
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
        # Early exit for very short texts - just return the whole text
        if len(text) <= window_size * 3:
            return text
        
        # For efficiency, limit to the first 3 mentions
        max_contexts = 3
        contexts = []
        
        # Use a single regex pattern with alternation for better performance
        pattern = fr'\b({ticker}|\${ticker})\b'
        
        for match in re.finditer(pattern, text):
            start = max(0, match.start() - window_size)
            end = min(len(text), match.end() + window_size)
            context = text[start:end]
            contexts.append(context)
            
            # Limit to max_contexts to avoid processing too many mentions
            if len(contexts) >= max_contexts:
                break
        
        if contexts:
            return " ".join(contexts)
        
        # If no explicit mentions found but ticker might be in text,
        # return a truncated version of the beginning of the text
        if ticker in text or f"${ticker}" in text:
            return text[:window_size * 2]
        
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
        
        # Process each Reddit post/comment in batches to avoid memory issues
        total_rows = len(df)
        logger.info(f"Processing {total_rows} Reddit posts for stock mentions")
        
        # Define batch size
        BATCH_SIZE = 10000
        
        # Process in batches
        for batch_start in range(0, total_rows, BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE, total_rows)
            logger.info(f"Processing batch from {batch_start} to {batch_end-1} of {total_rows}")
            
            # Get the current batch
            batch_df = df.iloc[batch_start:batch_end]
            
            # Process each post in the batch
            for index, row in batch_df.iterrows():
                if index % 1000 == 0:
                    logger.info(f"Processing Reddit post {index}/{total_rows}")
                
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
            
            # Force garbage collection after each batch
            import gc
            gc.collect()
            logger.info(f"Completed batch. Total stock mentions found so far: {len(stock_mentions)}")
        
        logger.info(f"Identified {len(stock_mentions)} stock mentions in Reddit data")
        
        return stock_mentions 