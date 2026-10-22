# Clean setup (Weeks 1-2), PowerShell, run from the project root

1. Docker Desktop must be running.
2. Download the connector jar (one time):
   Invoke-WebRequest -Uri "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar" -OutFile flink_jobs\flink-sql-connector-kafka-3.2.0-1.19.jar
3. pip install confluent-kafka==2.5.3 neo4j==5.26.0
4. Reset everything:  docker compose -f docker/docker-compose.yml down -v
5. docker compose -f docker/docker-compose.yml up -d kafka neo4j      (wait ~40 s)
6. docker exec fingraph-kafka /opt/kafka/bin/kafka-topics.sh --create --topic transactions.raw --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
7. Get-Content neo4j\cypher\schema.cypher | docker exec -i fingraph-neo4j cypher-shell -u neo4j -p fingraph123
8. docker compose -f docker/docker-compose.yml up -d --build flink-job   (first build takes minutes)
9. Wait ~30 s, then: python simulator/simulator.py --rate 200 --duration 60
10. Neo4j Browser (http://localhost:7474): MATCH ()-[t:TRANSFERRED_TO]->() RETURN count(t)

# Dashboard (Week 3)
    python analytics/run_gds.py          # writes wcc / community / pr onto accounts
    python dashboard/server.py           # then open http://localhost:8000
Without a database connection, dashboard/index.html shows built-in demo data.

# Replace the old files in your existing GitHub repo (keeps history)
    cd <your repo folder>
    git pull
    git rm -r -q .                       # removes old tracked files only
    Copy-Item -Recurse -Force "<extracted fingraph folder>\*" .
    git add -A
    git commit -m "Restructure project: clean Week 1-2 rebuild and dashboard"
    git push origin main
Then commit small and often while you actually work (Week 3: GDS, dashboard; Week 4: alerts, polish).
