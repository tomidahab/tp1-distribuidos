FROM huggingface/transformers-pytorch-cpu

WORKDIR /app

# Install dependencies
COPY worker/sentiment_analysis/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy worker Python files
COPY worker/sentiment_analysis/*.py .
COPY common/ ./common/

ENV PYTHONUNBUFFERED=1

RUN pip list | grep transformers

CMD ["python3", "main.py"]