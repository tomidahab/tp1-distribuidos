FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY worker/top_5_budget/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy worker Python files
COPY worker/top_5_budget/*.py .

ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]
