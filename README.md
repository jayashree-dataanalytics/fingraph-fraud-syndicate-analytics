# FinGraph: Real-Time Fraud Syndicate Analytics
Kafka -> Flink -> Neo4j (GDS) -> React dashboard for AML / smurfing detection.

## Week 1 quick start
    docker compose -f docker/docker-compose.yml up -d
    docker exec -it fingraph-kafka /opt/kafka/bin/kafka-topics.sh --create --topic transactions.raw --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
    pip install -r simulator/requirements.txt
    python simulator/simulator.py --rate 200 --duration 60
