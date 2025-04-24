FROM python:3.11-slim

WORKDIR /app

COPY boundary/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY boundary/*.py .

ENV PYTHONUNBUFFERED=1
EXPOSE 5000

CMD ["python", "main.py"]
