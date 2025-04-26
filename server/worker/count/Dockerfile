FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY worker/count/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy worker Python files
COPY worker/count/*.py .

ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]
