from typing import List
from multiprocessing import Pool, cpu_count
from models.message import Message
from processors.text_processor import TextProcessor
from models.stock import Stock

class MessageProcessor:
    def __init__(self, num_processes: int = None):
        self.text_processor = TextProcessor()
        self.num_processes = num_processes or max(1, cpu_count() - 1)
    
    def process_message(self, message: Message) -> Message:
        """Process a single message to extract stocks and sentiment"""
        title = getattr(message, 'title', '')
        stock_symbols, sentiment = self.text_processor.analyze_text(
            text=message.content,
            title=title
        )
        message.mentioned_stocks = {Stock(symbol=symbol) for symbol in stock_symbols}
        message.sentiment = sentiment
        return message
    
    def process_messages(self, messages: List[Message]) -> List[Message]:
        """
        Process a batch of messages using multiprocessing
        
        Args:
            messages: List of messages to process
            
        Returns:
            List of processed messages with updated sentiment and stock mentions
        """
        if not messages:
            return []
            
        # For small batches, process sequentially
        if len(messages) < 100:  # Arbitrary threshold, adjust as needed
            return [self.process_message(message) for message in messages]
        
        # For larger batches, use multiprocessing
        with Pool(processes=self.num_processes) as pool:
            processed_messages = pool.map(self.process_message, messages)
            
        return processed_messages 