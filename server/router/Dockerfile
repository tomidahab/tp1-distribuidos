FROM python:3.11-slim

WORKDIR /app

COPY router/requirements.txt . 
RUN pip install --no-cache-dir -r requirements.txt

COPY router/*.py .
COPY router/load_balancer/*.py .

ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]
