# This file makes the functions directory a proper Python package
# Firebase Functions will automatically discover functions in main.py

# Firebase Functions v2 needs proper exports
from .main import run_scraper_scheduler 