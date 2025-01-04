#!/bin/bash
cd /app
python -m scraper_service.src.main >> /app/logs/scraper.log 2>&1 