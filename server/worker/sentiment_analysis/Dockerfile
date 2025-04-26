FROM python:3.9-slim

WORKDIR /app

# Install only essential build tools
RUN apt-get update && apt-get install -y \
  build-essential \
  && rm -rf /var/lib/apt/lists/*

# Install dependencies
COPY worker/sentiment_analysis/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-download the sentiment analysis model to avoid downloading at runtime
RUN python3 -c "from transformers import pipeline; pipeline('sentiment-analysis')"

# Copy worker Python files
COPY worker/sentiment_analysis/*.py .
COPY common/ ./common/
COPY rabbitmq/ ./rabbitmq/

ENV PYTHONUNBUFFERED=1

CMD ["python3", "main.py"]