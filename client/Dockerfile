FROM python:3.9-slim
WORKDIR /app
COPY client/client.py .
COPY proto/message_pb2.py .
CMD ["python", "client.py"]
