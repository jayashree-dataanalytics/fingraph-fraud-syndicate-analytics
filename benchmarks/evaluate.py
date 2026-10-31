"""Week 4: evaluate detection against the simulator's ground-truth labels.

A transaction counts as DETECTED when both its accounts sit in a connected group (WCC)
that contains an account with risk_score >= 0.5. Ground truth comes from labels.jsonl.

Usage:
    python benchmarks/evaluate.py              # needs Neo4j + run_gds.py + risk-score queries
    python benchmarks/evaluate.py --selftest   # checks the maths with fake data (no database)
Only the most recent simulator run is in labels.jsonl, so clear Neo4j before a clean evaluation.
"""
import json, sys
from collections import Counter

FLAGGED_TX = """
MATCH (a:Account) WHERE a.risk_score >= 0.5 AND a.wcc IS NOT NULL
WITH collect(DISTINCT a.wcc) AS comps
MATCH (s:Account)-[t:TRANSFERRED_TO]->(d:Account)
WHERE s.wcc IN comps AND d.wcc IN comps
RETURN t.tx_id AS tx"""
PRESENT_TX = "MATCH ()-[t:TRANSFERRED_TO]->() WHERE t.tx_id IN $ids RETURN t.tx_id AS tx"


def score(labels, present, flagged):
    """labels: {tx_id: pattern}; present, flagged: sets of tx_ids."""
    rows = {tx: p for tx, p in labels.items() if tx in present}
    truth = {tx for tx, p in rows.items() if p != "benign"}
    pred = flagged & set(rows)
    tp, fp, fn = len(truth & pred), len(pred - truth), len(truth - pred)
    prec = tp / (tp + fp) if tp + fp else 0.0
    rec = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * prec * rec / (prec + rec) if prec + rec else 0.0
    per = {}
    for p in sorted(set(rows.values()) - {"benign"}):
        tx_p = {tx for tx, q in rows.items() if q == p}
        per[p] = (len(tx_p & pred), len(tx_p))
    return {"events": len(rows), "tp": tp, "fp": fp, "fn": fn, "precision": prec, "recall": rec, "f1": f1, "per_pattern": per}


def report(r):
    print(f"Events evaluated: {r['events']}   TP={r['tp']}  FP={r['fp']}  FN={r['fn']}")
    print(f"Precision: {r['precision']:.3f}   Recall: {r['recall']:.3f}   F1: {r['f1']:.3f}")
    for p, (hit, total) in r["per_pattern"].items():
        print(f"  {p:10s} detected {hit}/{total} ({hit / total:.0%})")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        labels = {"a": "starburst", "b": "starburst", "c": "layering", "d": "benign", "e": "benign"}
        r = score(labels, set(labels), {"a", "b", "d"})          # 2 TP, 1 FP, 1 FN
        assert (r["tp"], r["fp"], r["fn"]) == (2, 1, 1), r
        assert abs(r["precision"] - 2 / 3) < 1e-9 and abs(r["recall"] - 2 / 3) < 1e-9
        assert r["per_pattern"] == {"layering": (0, 1), "starburst": (2, 2)}, r
        print("selftest OK"); report(r); sys.exit()
    from neo4j import GraphDatabase
    labels = {}
    with open("labels.jsonl") as f:
        for line in f:
            d = json.loads(line); labels[d["tx_id"]] = d["pattern"]
    with GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "fingraph123")) as d:
        present = {r["tx"] for r in d.execute_query(PRESENT_TX, ids=list(labels)).records}
        flagged = {r["tx"] for r in d.execute_query(FLAGGED_TX).records}
    print(f"Labelled: {len(labels)} ({dict(Counter(labels.values()))}), found in Neo4j: {len(present)}")
    if not present:
        sys.exit("No labelled transactions in Neo4j. Run the simulator with Docker up first.")
    report(score(labels, present, flagged))