"""Runs the GDS pipeline (project -> WCC -> Louvain -> PageRank) and prints a summary.

Usage:
    python analytics/run_gds.py              # run once
    python analytics/run_gds.py --every 30   # re-run every 30 seconds
"""
import sys, time
from neo4j import GraphDatabase

PROJECT = """
MATCH (s:Account)-[t:TRANSFERRED_TO]->(d:Account)
WHERE t.ts > datetime() - duration('PT1H')
WITH s, d, sum(t.amount) AS total
WITH gds.graph.project('fin', s, d, {relationshipProperties: {total: total}}) AS g
RETURN g.nodeCount AS nodes, g.relationshipCount AS rels
"""

SUSPICIOUS = """
MATCH (a:Account) WHERE a.wcc IS NOT NULL
WITH a.wcc AS comp, collect(a) AS members
WHERE size(members) >= 3 AND size(members) <= 200
RETURN comp, size(members) AS size,
       [m IN members | m.id][0..3] AS sample_accounts,
       reduce(r = 0.0, m IN members | CASE WHEN coalesce(m.risk_score, 0) > r THEN m.risk_score ELSE r END) AS max_risk
ORDER BY max_risk DESC, size DESC LIMIT 10
"""


def run_once(d):
    t0 = time.perf_counter()
    d.execute_query("CALL gds.graph.drop('fin', false)")
    p = d.execute_query(PROJECT).records[0]
    wcc = d.execute_query("CALL gds.wcc.write('fin', {writeProperty: 'wcc'}) YIELD componentCount RETURN componentCount").records[0]
    lou = d.execute_query("CALL gds.louvain.write('fin', {relationshipWeightProperty: 'total', writeProperty: 'community'}) "
                          "YIELD communityCount, modularity RETURN communityCount, modularity").records[0]
    d.execute_query("CALL gds.pageRank.write('fin', {relationshipWeightProperty: 'total', writeProperty: 'pr'}) YIELD nodePropertiesWritten RETURN nodePropertiesWritten")
    print(f"Graph: {p['nodes']} accounts, {p['rels']} account pairs | WCC components: {wcc['componentCount']} | "
          f"Louvain communities: {lou['communityCount']} (modularity {lou['modularity']:.3f}) | "
          f"took {(time.perf_counter() - t0) * 1000:.0f} ms")
    for r in d.execute_query(SUSPICIOUS).records:
        print(f"  suspicious group: comp={r['comp']} size={r['size']} max_risk={r['max_risk']:.2f} sample={r['sample_accounts']}")


if __name__ == "__main__":
    every = float(sys.argv[sys.argv.index("--every") + 1]) if "--every" in sys.argv else None
    with GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "fingraph123")) as d:
        while True:
            run_once(d)
            if every is None:
                break
            time.sleep(every)
