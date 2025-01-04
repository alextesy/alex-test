#!/bin/bash
cd /app
python -m scraper_service.src.main >> /var/log/scraper.log 2>&1 