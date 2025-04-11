FROM python:3.9-slim
WORKDIR /app
COPY consumer_routing/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY consumer_routing/consumer_routing.py .
COPY proto/message_pb2.py .
CMD ["python", "consumer_routing.py"]
