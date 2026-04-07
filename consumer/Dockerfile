FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 複製所有模組
COPY config.py models.py validators.py writers.py anomaly.py dlq.py alerts.py consumer.py ./

CMD ["python", "-u", "consumer.py"]
