FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY boundary/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy boundary Python files
COPY boundary/*.py .

ENV PYTHONUNBUFFERED=1
EXPOSE 5000

CMD ["python", "main.py"]
