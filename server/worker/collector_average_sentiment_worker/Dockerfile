FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY worker/collector_average_sentiment_worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy worker Python files
COPY worker/collector_average_sentiment_worker/*.py .

ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]
