from dataclasses import dataclass
from typing import Optional
from sqlalchemy.orm import Session
from .database_models import DBStock

@dataclass
class Stock:
    """Model representing a stock/ticker symbol with exchange information"""
    symbol: str
    name: Optional[str] = None
    sector: Optional[str] = None
    exchange: Optional[str] = None
    
    def to_db_model(self, db: Session) -> DBStock:
        """Convert to database model"""
        return DBStock(
            symbol=self.symbol,
            name=self.name,
            sector=self.sector,
            exchange=self.exchange
        ) 