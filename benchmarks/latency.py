"""Mid-review proof #1: simulator -> Neo4j edge latency (target < 1 s).
Run while the simulator + Flink job are running (or right after)."""
from neo4j import GraphDatabase

Q = """
MATCH ()-[t:TRANSFERRED_TO]->()
WHERE t.write_ts > timestamp() - $window_ms
WITH t.write_ts - t.sim_ts AS lat
RETURN count(*) AS events,
       percentileCont(lat, 0.5)  AS p50_ms,
       percentileCont(lat, 0.95) AS p95_ms,
       percentileCont(lat, 0.99) AS p99_ms,
       max(lat) AS max_ms
"""

with GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "fingraph123")) as d:
    r = d.execute_query(Q, window_ms=120_000).records[0]
    print({k: (round(v, 1) if isinstance(v, float) else v) for k, v in r.items()})
    print("PASS (p99 < 1000 ms)" if r["p99_ms"] is not None and r["p99_ms"] < 1000
          else "CHECK: p99 above 1 s (or no data)")
