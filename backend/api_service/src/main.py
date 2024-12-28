from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional
from datetime import datetime, timedelta
from sqlalchemy import func
from db.database import SessionLocal
from models.database_models import ProcessedMessage, Stock

app = FastAPI(title="Stock Social Sentiment API")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/sentiment/stocks/{symbol}")
async def get_stock_sentiment(
    symbol: str,
    days: Optional[int] = Query(7, description="Number of days to look back")
):
    """Get sentiment analysis for a specific stock symbol"""
    db = SessionLocal()
    try:
        cutoff_time = datetime.now() - timedelta(days=days)
        
        # Get messages mentioning this stock
        messages = (
            db.query(ProcessedMessage)
            .join(ProcessedMessage.stocks)
            .filter(Stock.symbol == symbol.upper())
            .filter(ProcessedMessage.timestamp >= cutoff_time.timestamp())
            .all()
        )
        
        if not messages:
            raise HTTPException(status_code=404, detail=f"No data found for stock {symbol}")
        
        # Calculate average sentiment
        avg_sentiment = sum(m.sentiment for m in messages) / len(messages)
        
        return {
            "symbol": symbol.upper(),
            "average_sentiment": avg_sentiment,
            "message_count": len(messages),
            "time_period_days": days
        }
    
    finally:
        db.close()

@app.get("/api/trending/stocks")
async def get_trending_stocks(
    days: Optional[int] = Query(1, description="Number of days to look back"),
    limit: Optional[int] = Query(10, description="Number of stocks to return")
):
    """Get trending stocks based on mention frequency"""
    db = SessionLocal()
    try:
        cutoff_time = datetime.now() - timedelta(days=days)
        
        # Get most mentioned stocks
        trending = (
            db.query(
                Stock.symbol,
                Stock.name,
                func.count(ProcessedMessage.id).label("mention_count"),
                func.avg(ProcessedMessage.sentiment).label("average_sentiment")
            )
            .join(Stock.messages)
            .filter(ProcessedMessage.timestamp >= cutoff_time.timestamp())
            .group_by(Stock.symbol, Stock.name)
            .order_by(func.count(ProcessedMessage.id).desc())
            .limit(limit)
            .all()
        )
        
        return [{
            "symbol": stock.symbol,
            "name": stock.name,
            "mention_count": stock.mention_count,
            "average_sentiment": float(stock.average_sentiment)
        } for stock in trending]
    
    finally:
        db.close()

@app.get("/api/messages/recent")
async def get_recent_messages(
    limit: Optional[int] = Query(50, description="Number of messages to return")
):
    """Get recent processed messages"""
    db = SessionLocal()
    try:
        messages = (
            db.query(ProcessedMessage)
            .order_by(ProcessedMessage.timestamp.desc())
            .limit(limit)
            .all()
        )
        
        return [{
            "id": msg.id,
            "content": msg.content,
            "author": msg.author,
            "timestamp": msg.timestamp,
            "url": msg.url,
            "platform": msg.platform,
            "sentiment": msg.sentiment,
            "stocks": [stock.symbol for stock in msg.stocks]
        } for msg in messages]
    
    finally:
        db.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 