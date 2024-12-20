from flask import Flask, jsonify
from flask_cors import CORS
from api.routes import api_bp
from scrapers.twitter_scraper import TwitterScraper
from scrapers.reddit_scraper import RedditScraper
from scrapers.news_scraper import NewsScraper

app = Flask(__name__)
CORS(app)

# Register blueprints
app.register_blueprint(api_bp, url_prefix='/api')

@app.route('/health')
def health_check():
    return jsonify({"status": "healthy"})

if __name__ == '__main__':
    app.run(debug=True) 