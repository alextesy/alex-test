from flask import Blueprint, jsonify, request
from services.sentiment_service import SentimentService
from services.stock_service import StockService

api_bp = Blueprint('api', __name__)
sentiment_service = SentimentService()
stock_service = StockService()

@api_bp.route('/stocks/trending')
def get_trending_stocks():
    try:
        trending_stocks = stock_service.get_trending_stocks()
        return jsonify(trending_stocks)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@api_bp.route('/stocks/<symbol>/sentiment')
def get_stock_sentiment(symbol):
    try:
        sentiment = sentiment_service.get_sentiment(symbol)
        return jsonify(sentiment)
    except Exception as e:
        return jsonify({"error": str(e)}), 500 