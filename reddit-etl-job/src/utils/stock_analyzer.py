import re
import math
import logging
import pandas as pd
import numpy as np
import spacy
from typing import List, Dict, Any, Set
from datetime import datetime
import os
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from transformers import pipeline

from src.models.stock_data import StockMention

COMMON_NON_TICKER_WORDS = {
    "CEO", "USA", "USD", "CFO", "SEC", "IRS", "IPO", "ETF", "GDP", "FDA", "AI",
    "YOLO", "FOMO", "WSB", "PDT", "ROI", "IMO", "TLDR", "DD", "TOS", "CAD"
}

# Hardcoded common English stopwords
ENGLISH_STOPWORDS = {
    "a", "an", "and", "are", "as", "at", "be", "by", "for", "from", "has", "have", "he",
    "in", "is", "it", "its", "of", "on", "that", "the", "to", "was", "were", "will", "with",
    "i", "you", "your", "they", "this", "we", "but", "or", "not", "if", "so", "just", "my",
    "they", "their", "what", "which", "who", "whom", "why", "how", "can", "should", "would"
}


# Load spaCy English model (do this once, not per call)


logger = logging.getLogger(__name__)

class StockAnalyzer:
    """
    Analyzes Reddit text for stock mentions and sentiment.
    """
    
    def __init__(self):
        """Initialize the stock analyzer."""

        # Load spaCy model
        try:
            self.nlp = spacy.load("en_core_web_sm")
        except OSError:
            logger.info("Downloading spaCy model")
            spacy.cli.download("en_core_web_sm")
            self.nlp = spacy.load("en_core_web_sm")
            
        # Initialize sentiment analyzer
        self.sentiment_pipeline = pipeline(
                                    "sentiment-analysis",
                                    model="distilbert-base-uncased-finetuned-sst-2-english",
                                    top_k=None,  # disable top_k for consistent format
                                    return_all_scores=False
                                )
        # Load stock tickers
        self.stock_tickers = self.load_stock_tickers()

        # Initialize regex patterns
        self._init_regex_patterns()

    def _init_regex_patterns(self):
        """Precompile regex patterns used across the analyzer."""
        # Pattern to extract potential stock tickers (2-5 letters, optional preceding $)
        self.ticker_pattern = re.compile(r'(?<!\w)(?:\$)?([A-Za-z]{2,5})(?![a-zA-Z])')
        
        # Regex patterns for trading signals and other indicators
        self.regex_buy = re.compile(
            r'\b(buy|bought|buying|long|calls|bullish|moon|rocket|🚀|💎|🙌|going up|to the moon|undervalued|cheap|discount)\b',
            re.IGNORECASE
        )
        self.regex_sell = re.compile(
            r'\b(sell|selling|sold|short|puts|bearish|crash|dump|tank|dropping|overvalued|expensive|bubble|correction|margin call)\b',
            re.IGNORECASE
        )
        self.regex_hold = re.compile(
            r'\b(hold|holding|hodl|diamond hands|patient|patience|long term|longterm)\b',
            re.IGNORECASE
        )
        self.regex_earnings = re.compile(
            r'\b(earnings|revenue|growth|profit|loss|guidance|forecast|EPS|P/E|dividend)\b',
            re.IGNORECASE
        )
        self.regex_news = re.compile(
            r'\b(news|announcement|released|launched|partnership|acquisition|merger|FDA|approval|patent|lawsuit)\b',
            re.IGNORECASE
        )
        self.regex_technical = re.compile(
            r'\b(resistance|support|trend|breakout|pattern|cup|handle|head|shoulders|triangle|wedge|channel|RSI|MACD|oversold|overbought)\b',
            re.IGNORECASE
        )
        self.regex_options = re.compile(
            r'\b(option|call|put|strike|expiry|contracts|leaps|covered|naked|straddle|strangle|iron condor|spread)\b',
            re.IGNORECASE
        )
        
        self.regex_price = re.compile(r'\$?(\d+(?:\.\d+)?)(?:\$)?')
        self.regex_percent = re.compile(r'([+-]?\d+(?:\.\d+)?)\s?%')

    def load_stock_tickers(self) -> Set[str]:
        """
        Load stock tickers from BigQuery or fallback sources.
        
        Returns:
            Set of stock tickers
        """
        logger.info("Loading stock tickers")
                
        try:
            # Connect to BigQuery
            project_id = os.getenv('GOOGLE_CLOUD_PROJECT_ID')
            dataset_id = os.getenv('BIGUERY_STOCKS_DATASET')
            table_id = os.getenv('TICKER_SOURCE_TABLE')
            client = bigquery.Client(project=project_id)
            table_id = f"{project_id}.{dataset_id}.{table_id}"
            
            
            # Load tickers from BigQuery
            tickers = self._load_tickers_from_bigquery(client,table_id)
            
        except Exception as e:
            logger.error(f"Error loading stock tickers from BigQuery: {str(e)}")
            logger.info("Falling back to alternative ticker sources")
            tickers = self._load_fallback_tickers()
        
        return tickers
    
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
    
    def _load_tickers_from_bigquery(self, client: bigquery.Client, 
                                    table_id: str) -> Set[str]:
        """
        Load tickers from BigQuery table.
        
        Returns:
            Set of stock tickers
        """
        tickers = set()
        logger.info("Loading stock tickers from BigQuery")
        
        query = f"""
        SELECT Ticker FROM `{table_id}` 
        group by Ticker
        order by avg(volume) desc
        limit 1000
        """
        query_job = client.query(query)
        results = query_job.result()
        
        for row in results:
            ticker = row.Ticker
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
        if not isinstance(text, str) or not text:
            return []

        text = text[:2000]

        matches = self.ticker_pattern.findall(text)

        tickers = {
            match.upper()
            for match in matches
            if match.lower() not in ENGLISH_STOPWORDS
            and match.upper() not in COMMON_NON_TICKER_WORDS
            and match.upper() in self.stock_tickers
        }

        return list(tickers)[:10]
    
    def analyze_sentiment_batch(self, texts: List[str], scores: List[int]) -> List[Dict[str, Any]]:
        """Batch sentiment analysis using transformers pipeline."""
        results = self.sentiment_pipeline(texts, batch_size=16)

        output = []
        for i, res in enumerate(results):
            # Normalize result into a score dictionary
            if isinstance(res, list):
                score_dict = {item["label"].upper(): item["score"] for item in res}
            else:
                score_dict = {res["label"].upper(): res["score"]}

            # Pull individual scores with fallback
            pos = score_dict.get("POSITIVE", 0.0)
            neg = score_dict.get("NEGATIVE", 0.0)
            neu = score_dict.get("NEUTRAL", 0.0)

            # Calculate compound as net sentiment: positive - negative
            compound = round(pos - neg, 3)

            # Confidence from compound + Reddit score
            confidence = self.calc_confidence_score(compound, scores[i])

            output.append({
                "compound": compound,
                "positive": pos,
                "negative": neg,
                "neutral": neu,
                "confidence": confidence
            })

        return output

    
    def extract_signals_regex(self, text: str, ticker: str) -> List[str]:
        """Extract trading signals using precompiled regex patterns (faster than NLP)"""
        signals = []

        if self.regex_buy.search(text):
            signals.append("BUY")

        if self.regex_sell.search(text):
            signals.append("SELL")

        if self.regex_hold.search(text):
            signals.append("HOLD")

        price_targets = self.extract_price_and_percent_signals(text)
        if price_targets:
            for target_type, value in price_targets.items():
                signals.append(f"{target_type}:{value}")

        if self.regex_earnings.search(text):
            signals.append("EARNINGS")

        if self.regex_news.search(text):
            signals.append("NEWS")

        if self.regex_technical.search(text):
            signals.append("TECHNICAL")

        if self.regex_options.search(text):
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
    
    def extract_price_and_percent_signals(self, text: str) -> Dict[str, float]:
        """
        Extract the first price and percentage change from the text.
        Skips overlapping patterns (e.g., 5% is not counted as a price).
        
        Returns:
            Dictionary with optional keys 'PT' and 'CHANGE'
        """
        signals = {}
        percent_positions = set()

        # Extract percentage change
        for match in self.regex_percent.finditer(text):
            try:
                val = float(match.group(1))
                signals['CHANGE'] = val
                percent_positions.update(range(match.start(), match.end()))
                break  # Only take first
            except ValueError:
                continue

        # Extract price, skip if it overlaps with a % match
        for match in self.regex_price.finditer(text):
            if any(pos in percent_positions for pos in range(match.start(), match.end())):
                continue
            try:
                val = float(match.group(1))
                if 1 <= val <= 10000:
                    signals['PT'] = val
                    break  # Only take first
            except ValueError:
                continue

        return signals


    
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

    def _process_batch(self, batch_df: pd.DataFrame) -> List[StockMention]:
        """
        Process a single batch of Reddit data.
        This must be a method for class integration.
        
        Args:
            batch_df: DataFrame containing Reddit data
            
        Returns:
            List of StockMention objects
        """
        logger.info(f"Starting to process batch of {len(batch_df)} Reddit posts")
        
        batch_mentions = []
        
        # Process each post in the batch
        for row in batch_df.itertuples(index=False):    
            # Combine title and content for posts
            text = row.content
            if pd.notna(row.title) and row.title:
                text = f"{row['title']} {text}"
            
            # Skip if text is empty or not a string
            if not isinstance(text, str) or not text.strip():
                continue
                
            texts = []
            scores = []
            ticker_contexts = []
            tickers_to_analyze = []
            score = getattr(row, "score", 0)
            message_id = getattr(row, "message_id", 0)

            # Extract stock tickers mentioned in the text
            mentioned_tickers = self.extract_stock_mentions(text)
            for ticker in mentioned_tickers:
                context = self.extract_ticker_context(text, ticker, window_size=100) or text[:500]
                texts.append(context[:512])
                scores.append(score if isinstance(score, (int, float, np.int64)) else 0)
                ticker_contexts.append(context)
                tickers_to_analyze.append((row, ticker))  # store row and ticker together
                
            if texts:
                sentiments = self.analyze_sentiment_batch(texts, scores)

                # Attach sentiment results to StockMention
                for i, sentiment in enumerate(sentiments):
                    row, ticker = tickers_to_analyze[i]
                    mention = StockMention(
                        message_id=getattr(row, 'message_id'),
                        ticker=ticker,
                        author=getattr(row, 'author'),
                        created_at=getattr(row, 'created_at'),
                        subreddit=getattr(row, 'subreddit'),
                        url=getattr(row, 'url'),
                        score=scores[i],
                        message_type=getattr(row, 'message_type'),
                        sentiment_compound=sentiment['compound'],
                        sentiment_positive=sentiment['positive'],
                        sentiment_negative=sentiment['negative'],
                        sentiment_neutral=sentiment['neutral'],
                        signals=self.extract_signals_regex(ticker_contexts[i], ticker),
                        context=ticker_contexts[i][:200],
                        confidence=sentiment['confidence']
                    )

                    batch_mentions.append(mention)
                    
        self.save_batch_mentions(batch_mentions)
        logger.info(f"Processed batch and found {len(batch_mentions)} stock mentions")
        return batch_mentions
    
    def save_batch_mentions(self, batch_mentions: List[StockMention]):
        """
        Save a batch of stock mentions to BigQuery.
        """
        from src.activities.persistence_activities import save_stock_mentions_activity

        
        save_result = save_stock_mentions_activity(batch_mentions)
        logger.info(f"Saved {save_result} stock mentions to BigQuery")
        
        return save_result
        
    
    def process_reddit_data(self, df: pd.DataFrame) -> List[StockMention]:
        """
        Process Reddit data to identify stock mentions and analyze sentiment.
        Using parallel processing to handle batches concurrently.
        
        Args:
            df: DataFrame with Reddit data
            
        Returns:
            List of StockMention objects
        """
        from multiprocessing.dummy import Pool as ThreadPool
        import time
        
        if df.empty:
            return []
        
        # Initialize list to store stock mentions
        total_rows = len(df)
        logger.info(f"Processing {total_rows} Reddit posts for stock mentions")
        
        # Define batch size - smaller for better parallelization
        BATCH_SIZE = 500  # Reduced from 10000 to 500
        logger.info(f"Using batch size of {BATCH_SIZE} posts per worker")
        
        # Create batches
        batches = []
        for batch_start in range(0, total_rows, BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE, total_rows)
            logger.info(f"Creating batch from {batch_start} to {batch_end-1} of {total_rows}")
            batches.append(df.iloc[batch_start:batch_end])
        
        # Determine number of processes to use
        num_processes = min(os.cpu_count(), len(batches))
        logger.info(f"Using {num_processes} processes to handle {len(batches)} batches")
        
                
        # Process batches in parallel
        stock_mentions = []
        start_time = time.time()
        
        with ThreadPool(num_processes) as pool:
            # Use the global _process_batch function instead of a local one
            results = pool.map(self._process_batch, batches)
            
            # Flatten results list
            for i, batch_result in enumerate(results):
                stock_mentions.extend(batch_result)
                logger.info(f"Completed batch {i+1}/{len(batches)} with {len(batch_result)} mentions")
        
        elapsed_time = time.time() - start_time
        logger.info(f"Identified {len(stock_mentions)} stock mentions in Reddit data in {elapsed_time:.2f} seconds")
        logger.info(f"Processing speed: {total_rows/elapsed_time:.2f} posts/second")
        
        return stock_mentions 