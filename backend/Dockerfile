FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    cron \
    && rm -rf /var/lib/apt/lists/*

# Install poetry
RUN pip install poetry==1.7.1

# Copy poetry files
COPY pyproject.toml poetry.lock* ./

# Copy the rest of the application
COPY . .

# Make the run script executable
RUN chmod +x run_scraper.sh

# Install dependencies
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --only main,scraper

# Set environment variables
ENV PYTHONPATH=/app
ENV ENV=production

# Create log file
RUN touch /var/log/scraper.log

# Set up cron job
RUN echo "0 * * * * /alex-test/backend/run_scraper.sh" > /etc/cron.d/scraper-cron && \
    chmod 0644 /etc/cron.d/scraper-cron && \
    crontab /etc/cron.d/scraper-cron

# Create entrypoint script
RUN echo '#!/bin/bash\ncron && tail -f /var/log/scraper.log' > /entrypoint.sh && \
    chmod +x /entrypoint.sh

# Run cron in the foreground
CMD ["/entrypoint.sh"] 