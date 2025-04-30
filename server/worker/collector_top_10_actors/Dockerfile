FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY worker/collector_top_10_actors/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy worker Python files
COPY worker/collector_top_10_actors/*.py .

ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]
