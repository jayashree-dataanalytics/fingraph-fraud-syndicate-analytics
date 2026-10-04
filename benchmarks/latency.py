"""Mid-review proof #1: simulator -> Neo4j edge latency (target < 1 s).

Usage:
    python benchmarks/latency.py            # last 6 hours (default)
    python benchmarks/latency.py 5          # last 5 minutes only (best right after a simulator run)
"""
import sys
from neo4j import GraphDatabase

minutes = float(sys.argv[1]) if len(sys.argv) > 1 else 360
window_ms = int(minutes * 60 * 1000)

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
    r = d.execute_query(Q, window_ms=window_ms).records[0]
    print(f"Window: last {minutes:g} minutes")
    print({k: (round(v, 1) if isinstance(v, float) else v) for k, v in r.items()})
    if r["events"] == 0:
        print("NO DATA in this window. Run the simulator, or use a longer window, e.g. python benchmarks/latency.py 360")
    elif r["p99_ms"] < 1000:
        print("PASS (p99 < 1000 ms)")
    else:
        print("CHECK: p99 above 1 s. Try a lower simulator --rate (e.g. 100) and re-run.")