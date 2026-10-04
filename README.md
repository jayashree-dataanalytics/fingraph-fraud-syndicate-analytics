# FinGraph: Real-Time Fraud Syndicate Analytics
Kafka -> Flink -> Neo4j (GDS) -> React dashboard for AML / smurfing detection.

## Week 1 quick start
    docker compose -f docker/docker-compose.yml up -d
    docker exec -it fingraph-kafka /opt/kafka/bin/kafka-topics.sh --create --topic transactions.raw --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
    pip install -r simulator/requirements.txt
    python simulator/simulator.py --rate 200 --duration 60

## Week 2: stream processing
    # one-time: download the Kafka connector jar into flink_jobs/
    #   https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar
    pip install -r flink_jobs/requirements.txt      # Python 3.8-3.11 + Java 11/17 (use WSL2 on Windows)
    python flink_jobs/fingraph_job.py               # terminal 1 (start first)
    python simulator/simulator.py --duration 120    # terminal 2
    python benchmarks/latency.py                    # < 1 s proof
    python benchmarks/query_timing.py               # < 100 ms proof
