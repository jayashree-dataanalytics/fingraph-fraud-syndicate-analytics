# FinGraph: Real-Time Fraud Syndicate Analytics
Kafka → Flink → Neo4j (GDS) → animated network dashboard for AML / smurfing detection.

Instead of flagging single transactions ("over $10,000"), FinGraph stores money flow as a graph
(accounts = nodes, transfers = edges) and detects syndicates: starburst fan-in, layering chains
and circular flows.

## Project structure
    docker/        docker-compose.yml (Kafka, Neo4j + GDS, Flink job)
    simulator/     transaction simulator with ground-truth labels
    flink_jobs/    PyFlink stream processor + Dockerfile
    neo4j/cypher/  schema.cypher, queries.cypher, gds.cypher
    benchmarks/    latency.py (< 1 s proof), query_timing.py (< 100 ms proof)
    analytics/     run_gds.py (WCC, Louvain, PageRank)
    dashboard/     index.html (animated graph UI) + server.py (Neo4j API)
    docs/          screenshots and benchmark results

## Week 1: ingestion setup and graph schema   [Done]
- Docker Compose environment: Kafka (KRaft) and Neo4j 5 with the GDS plugin.
- Graph schema: Account nodes, TRANSFERRED_TO edges (tx_id, amount, ts), with uniqueness
  constraints and indexes on tx_id and timestamps.
- Python simulator: benign background traffic plus labeled fraud patterns
  (starburst, layering with fee skim, circular flow), streamed to the Kafka topic
  transactions.raw. A separate labels.jsonl holds the ground truth.

    docker compose -f docker/docker-compose.yml up -d kafka neo4j
    docker exec fingraph-kafka /opt/kafka/bin/kafka-topics.sh --create --topic transactions.raw --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
    Get-Content neo4j\cypher\schema.cypher | docker exec -i fingraph-neo4j cypher-shell -u neo4j -p fingraph123
    pip install -r simulator/requirements.txt
    python simulator/simulator.py --rate 200 --duration 60

## Week 2: stream processing and Cypher queries   [Done]
- PyFlink job (runs in Docker): reads Kafka, validates records, de-duplicates by tx_id,
  and upserts accounts and transfers into Neo4j in micro-batches (idempotent MERGE).
- Cypher detection queries: starburst (structuring fan-in), circular flows,
  multi-hop money trail, and risk score v1.
- Mid-review benchmarks: end-to-end latency (target < 1 s) and multi-hop query time (target < 100 ms).

    # one-time: download the Kafka connector jar into flink_jobs/
    #   https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar
    docker compose -f docker/docker-compose.yml up -d --build flink-job
    python simulator/simulator.py --duration 120
    python benchmarks/latency.py 5                # < 1 s proof
    python benchmarks/query_timing.py             # < 100 ms proof

| Metric | Target | Result |
|---|---|---|
| Simulator → Neo4j latency (p99) | < 1000 ms | fill in |
| Multi-hop query time (p95) | < 100 ms | fill in |

## Week 3: graph data science and dashboard   [In progress]
- Neo4j GDS: Weakly Connected Components (isolates syndicates), Louvain communities,
  weighted PageRank (centrality of shell accounts).
- Animated dashboard: moving money-flow particles, pulsing high-risk accounts, node size by
  PageRank, flagged-group panel, one-click Freeze, and a "Replay attack" timeline.
  Without a database it shows built-in demo data.

    python analytics/run_gds.py                   # add --every 30 to refresh continuously
    python dashboard/server.py                    # open http://localhost:8000

## Week 4: alerts, automation and polish   [Planned]
- Rules engine: Slack / email alert when an account or group risk score passes a threshold.
- Dashboard polish: expand / collapse nodes, trace the money trail visually, final layout.
- Final evaluation: precision / recall of detections against labels.jsonl.

## Notes
- On Windows, the Flink job runs inside Docker (native PyFlink was unstable).
- Neo4j login for local development: neo4j / fingraph123.
- All data is simulated; no real customer data is used.

## Alert rules engine

`alerts/rules_engine.py` turns risk scores in the graph into alerts. It reads accounts and communities from Neo4j, checks them against a list of rules, and sends an alert for each match.

### Rule types

- **account**: alerts on any account whose risk score is at or above `threshold`.
- **group**: alerts on any community whose score is at or above `threshold` and which has at least `min_members` accounts.

Each rule has a `name`, a `type`, a `threshold` and a `cooldown_minutes`. Group rules also take `min_members`.

### Cooldown

After an alert fires, the same account or group is not alerted again until its `cooldown_minutes` has passed. Each account and group has its own cooldown. Cooldown state is stored in `.alert_state.json`, which is git-ignored.

### Dry run

`run_once(..., dry_run=True)` evaluates the rules without sending anything, so thresholds can be checked safely.

### Tests

The unit tests use fake Neo4j sessions, so no database is needed:

    python alerts/test_rules.py -v

They cover the threshold boundary, cooldown blocking, re-alerting after cooldown, group alerts with member counts, and per-account cooldowns.
