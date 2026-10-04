FROM python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless build-essential \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY fingraph_job.py .
COPY flink-sql-connector-kafka-3.2.0-1.19.jar .
CMD ["python", "fingraph_job.py"]