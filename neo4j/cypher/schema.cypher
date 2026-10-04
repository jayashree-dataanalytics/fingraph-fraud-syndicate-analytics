// FinGraph schema (Week 1)
// Nodes: Person, Account, Bank   Edges: OWNS, HELD_AT, TRANSFERRED_TO

CREATE CONSTRAINT person_id IF NOT EXISTS FOR (p:Person)  REQUIRE p.id IS UNIQUE;
CREATE CONSTRAINT acct_id   IF NOT EXISTS FOR (a:Account) REQUIRE a.id IS UNIQUE;
CREATE CONSTRAINT bank_id   IF NOT EXISTS FOR (b:Bank)    REQUIRE b.id IS UNIQUE;

CREATE INDEX tx_ts     IF NOT EXISTS FOR ()-[t:TRANSFERRED_TO]-() ON (t.ts);
CREATE INDEX tx_id     IF NOT EXISTS FOR ()-[t:TRANSFERRED_TO]-() ON (t.tx_id);
CREATE INDEX acct_risk IF NOT EXISTS FOR (a:Account) ON (a.risk_score);

// Expected shape:
// (:Person {id,name})-[:OWNS]->(:Account {id,status,risk_score,community_id})
// (:Account)-[:HELD_AT]->(:Bank {id,jurisdiction})
// (:Account)-[:TRANSFERRED_TO {tx_id,amount,ts}]->(:Account)

// Week 2 addition: needed for the latency proof
CREATE INDEX tx_write_ts IF NOT EXISTS FOR ()-[t:TRANSFERRED_TO]-() ON (t.write_ts);
