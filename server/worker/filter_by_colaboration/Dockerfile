FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY ./worker/filter_by_colaboration/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy worker Python files
COPY ./worker/filter_by_colaboration/*.py .
COPY ./rabbitmq /app/rabbitmq
COPY ./common /app/common

ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]