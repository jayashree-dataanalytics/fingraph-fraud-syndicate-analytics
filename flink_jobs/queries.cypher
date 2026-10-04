// ===== FinGraph detection queries (Week 2) =====
// Time windows are 1 hour so they work with the live simulator; widen for real data.

// 1. STARBURST / structuring: many distinct senders, each just under $10k, into one account
MATCH (m:Account)-[t:TRANSFERRED_TO]->(s:Account)
WHERE t.ts > datetime() - duration('PT1H') AND t.amount >= 9000 AND t.amount < 10000
WITH s, count(DISTINCT m) AS senders, sum(t.amount) AS total
WHERE senders >= 20
RETURN s.id AS shell_account, senders, round(total) AS total_received
ORDER BY senders DESC;

// 2. CIRCULAR FLOW (A -> B -> C -> A) with near-equal amounts.
//    Amount-similarity avoids flagging the many random 3-cycles in benign traffic.
MATCH (a:Account)-[t1:TRANSFERRED_TO]->(b:Account)-[t2:TRANSFERRED_TO]->(c:Account)-[t3:TRANSFERRED_TO]->(a)
WHERE t1.ts > datetime() - duration('PT1H')
  AND t1.amount >= 5000
  AND abs(t1.amount - t2.amount) / t1.amount < 0.05
  AND abs(t2.amount - t3.amount) / t2.amount < 0.05
  AND a.id < b.id AND a.id < c.id          // report each ring once
RETURN a.id, b.id, c.id, t1.amount, t2.amount, t3.amount
LIMIT 25;

// 3. MULTI-HOP TRACE from one account (anchored + bounded => fast)
MATCH p = (a:Account {id: $id})-[:TRANSFERRED_TO*1..4]->(z:Account)
WHERE all(r IN relationships(p) WHERE r.ts > datetime() - duration('PT1H'))
RETURN z.id AS reached, length(p) AS hops, [r IN relationships(p) | r.amount] AS amounts
LIMIT 50;

// 4. RISK SCORE v1 (structuring fan-in). Run on a schedule; Week 3 adds PageRank / communities.
MATCH (m:Account)-[t:TRANSFERRED_TO]->(s:Account)
WHERE t.ts > datetime() - duration('PT1H') AND t.amount >= 9000 AND t.amount < 10000
WITH s, count(DISTINCT m) AS senders
SET s.risk_score = least(1.0, senders / 50.0),
    s.risk_reason = 'structuring fan-in',
    s.risk_updated = datetime()
RETURN count(s) AS accounts_scored;

// 5. RISK SCORE v1 (cycle flag)
MATCH (a:Account)-[t1:TRANSFERRED_TO]->(b:Account)-[t2:TRANSFERRED_TO]->(c:Account)-[t3:TRANSFERRED_TO]->(a)
WHERE t1.ts > datetime() - duration('PT1H') AND t1.amount >= 5000
  AND abs(t1.amount - t2.amount) / t1.amount < 0.05
  AND abs(t2.amount - t3.amount) / t2.amount < 0.05
WITH collect(DISTINCT a) + collect(DISTINCT b) + collect(DISTINCT c) AS ring_accts
UNWIND ring_accts AS x
WITH DISTINCT x
SET x.risk_score = coalesce(x.risk_score, 0) + 0.5, x.risk_reason = 'circular flow';
