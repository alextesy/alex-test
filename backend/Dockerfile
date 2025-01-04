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

# Create logs directory
RUN mkdir -p /app/logs && \
    chmod -R 777 /app/logs

# Set up cron job
RUN echo "0 * * * * /app/run_scraper.sh" > /etc/cron.d/scraper-cron && \
    chmod 0644 /etc/cron.d/scraper-cron && \
    crontab /etc/cron.d/scraper-cron

# Create entrypoint script with better log handling
RUN echo '#!/bin/bash\n\
echo "Starting entrypoint script..."\n\
echo "Creating log directory..."\n\
mkdir -p /app/logs\n\
echo "Creating log file..."\n\
touch /app/logs/scraper.log\n\
echo "Setting permissions..."\n\
chmod 777 /app/logs/scraper.log\n\
echo "Running initial scraper..."\n\
/app/run_scraper.sh\n\
echo "Starting cron..."\n\
cron\n\
echo "Starting log tail..."\n\
' > /entrypoint.sh && \
    chmod +x /entrypoint.sh

# Run cron in the foreground
CMD ["/entrypoint.sh"] 