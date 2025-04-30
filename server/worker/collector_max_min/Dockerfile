FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY worker/collector_max_min/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy worker Python files
COPY worker/collector_max_min/*.py .

ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]
