FROM python:3.10-slim

WORKDIR /app

# Install dependencies first (for better caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all Python files
COPY *.py ./

# Set default environment variable configuration
ENV PYTHONUNBUFFERED=1

EXPOSE 5000

# Entry point is main.py, not boundary.py
CMD ["python", "main.py"]
