from dataclasses import dataclass
from typing import Optional
from sqlalchemy.orm import Session
from .database_models import Stock as DBStock

@dataclass
class Stock:
    """Model representing a stock/ticker symbol"""
    symbol: str
    name: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    
    def to_db_model(self, db: Session) -> DBStock:
        """Convert to database model"""
        return DBStock(
            symbol=self.symbol,
            name=self.name,
            sector=self.sector,
            industry=self.industry
        ) 