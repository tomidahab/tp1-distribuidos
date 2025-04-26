FROM python:3.11-slim

WORKDIR /app

COPY ./worker/average_sentiment/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./worker/average_sentiment .
COPY ./rabbitmq /app/rabbitmq
COPY ./common /app/common

ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]
