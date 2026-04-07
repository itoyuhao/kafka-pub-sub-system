FROM python:3.12-slim

WORKDIR /app

# 先複製 requirements 並安裝，善用 Docker 的 layer cache 機制
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 複製所有模組
COPY config.py generators.py fraud_simulator.py producer.py ./

CMD ["python", "-u", "producer.py"]
