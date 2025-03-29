FROM python:3.10-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install spaCy model
RUN python -m spacy download en_core_web_sm

# Copy source code
COPY . .

# Set Python path to include the current directory
ENV PYTHONPATH=/app

# Run the application
# Change from main.py to simple_run.py
CMD ["python", "simple_run.py"] 