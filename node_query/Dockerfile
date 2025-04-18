FROM python:3.9-slim
WORKDIR /app
COPY node_query/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY node_query/node_query.py .
COPY proto/message_pb2.py .
CMD ["python", "node_query.py"]
