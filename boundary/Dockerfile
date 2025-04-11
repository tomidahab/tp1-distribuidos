FROM python:3.9-slim
WORKDIR /app
COPY boundary/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY boundary/boundary.py .
COPY proto/message_pb2.py .
EXPOSE 5000
CMD ["python", "boundary.py"]
