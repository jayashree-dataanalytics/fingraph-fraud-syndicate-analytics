// ===== FinGraph Week 3: Graph Data Science (GDS) =====
// Run each block separately in the Neo4j Browser.

// 1. Project an in-memory graph: one weighted edge per account pair, last 1 hour
CALL gds.graph.drop('fin', false) YIELD graphName RETURN graphName;

MATCH (s:Account)-[t:TRANSFERRED_TO]->(d:Account)
WHERE t.ts > datetime() - duration('PT1H')
WITH s, d, sum(t.amount) AS total
WITH gds.graph.project('fin', s, d, {relationshipProperties: {total: total}}) AS g
RETURN g.graphName AS graph, g.nodeCount AS nodes, g.relationshipCount AS rels;

// 2. Weakly Connected Components: isolates groups that never touch the rest of the network
CALL gds.wcc.write('fin', {writeProperty: 'wcc'}) YIELD componentCount, nodePropertiesWritten
RETURN componentCount, nodePropertiesWritten;

// 3. Louvain community detection (weighted by money moved)
CALL gds.louvain.write('fin', {relationshipWeightProperty: 'total', writeProperty: 'community'})
YIELD communityCount, modularity RETURN communityCount, modularity;

// 4. PageRank centrality (weighted)
CALL gds.pageRank.write('fin', {relationshipWeightProperty: 'total', writeProperty: 'pr'})
YIELD nodePropertiesWritten, ranIterations RETURN nodePropertiesWritten, ranIterations;

// 5. Suspicious components: small, isolated groups moving real money.
//    (Benign traffic forms one giant component; syndicates are separate small ones.)
MATCH (a:Account) WHERE a.wcc IS NOT NULL
WITH a.wcc AS comp, collect(a) AS members
WHERE size(members) >= 3 AND size(members) <= 200
UNWIND members AS m
WITH comp, size(members) AS size, m ORDER BY m.pr DESC
WITH comp, size, collect(m.id)[0..3] AS top_accounts, max(coalesce(m.risk_score, 0)) AS max_risk
RETURN comp, size, top_accounts, max_risk
ORDER BY max_risk DESC, size DESC LIMIT 20;

// 6. Biggest community by size (expect one giant benign component)
MATCH (a:Account) WHERE a.wcc IS NOT NULL
RETURN a.wcc AS comp, count(*) AS size ORDER BY size DESC LIMIT 5;
