FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY worker/countries_budget/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy worker Python files
COPY worker/countries_budget/*.py .

ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]