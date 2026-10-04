"""Mid-review proof #2: multi-hop Cypher latency (target < 100 ms)."""
import statistics, time
from neo4j import GraphDatabase

QUERIES = {
    "starburst": """
        MATCH (m:Account)-[t:TRANSFERRED_TO]->(s:Account)
        WHERE t.ts > datetime() - duration('PT1H') AND t.amount >= 9000 AND t.amount < 10000
        WITH s, count(DISTINCT m) AS senders WHERE senders >= 20
        RETURN s.id, senders""",
    "cycle_3hop": """
        MATCH (a:Account)-[t1:TRANSFERRED_TO]->(b:Account)-[t2:TRANSFERRED_TO]->(c:Account)-[t3:TRANSFERRED_TO]->(a)
        WHERE t1.ts > datetime() - duration('PT1H') AND t1.amount >= 5000
          AND abs(t1.amount - t2.amount) / t1.amount < 0.05
          AND abs(t2.amount - t3.amount) / t2.amount < 0.05
          AND a.id < b.id AND a.id < c.id
        RETURN a.id, b.id, c.id LIMIT 25""",
    "multihop_trace_4": """
        MATCH p = (a:Account {id: $id})-[:TRANSFERRED_TO*1..4]->(z:Account)
        WHERE all(r IN relationships(p) WHERE r.ts > datetime() - duration('PT1H'))
        RETURN z.id, length(p) LIMIT 50""",
}

with GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "fingraph123")) as d:
    seed = d.execute_query(
        "MATCH (a:Account) WITH a, COUNT { (a)-[:TRANSFERRED_TO]->() } AS deg "
        "ORDER BY deg DESC LIMIT 1 RETURN a.id AS id").records
    params = {"id": seed[0]["id"] if seed else "ACC000001"}
    for name, q in QUERIES.items():
        for _ in range(3):                      # warm-up
            d.execute_query(q, **params)
        times = []
        for _ in range(20):
            t0 = time.perf_counter()
            d.execute_query(q, **params)
            times.append((time.perf_counter() - t0) * 1000)
        p50, p95 = statistics.median(times), sorted(times)[18]
        print(f"{name:18s} p50={p50:6.1f} ms  p95={p95:6.1f} ms  {'PASS' if p95 < 100 else 'SLOW'}")
