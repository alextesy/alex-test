#!/bin/bash
cd ~/app/alex-test/backend
export PYTHONPATH=.
python3 -m scraper_service.src.main >> /app/logs/alexstocks/scraper.log 2>&1 