"""Precision / recall of syndicate detection against labels.jsonl.

A transaction is FLAGGED when both of its accounts sit in a flagged group:
a WCC component with MIN..MAX accounts (same rule as analytics/run_gds.py).
Ground truth: every labelled transaction whose pattern is not 'benign'.

    python evaluation/evaluate.py
    python evaluation/evaluate.py --min-size 3 --max-size 200 --out docs/evaluation_result.md

Run analytics/run_gds.py first so accounts have a `wcc` property, and do not
re-run the simulator in between (it overwrites labels.jsonl).
"""
import argparse
import collections
import json

from neo4j import GraphDatabase

EDGES_Q = "MATCH (s:Account)-[t:TRANSFERRED_TO]->(d:Account) RETURN t.tx_id AS tx_id, s.wcc AS sw, d.wcc AS dw"
SIZES_Q = "MATCH (a:Account) WHERE a.wcc IS NOT NULL RETURN a.wcc AS comp, count(*) AS size"


def evaluate(labels, edges, sizes, lo, hi):
    """labels: tx_id -> {pattern, syndicate_id}; edges: [(tx_id, wcc_src, wcc_dst)];
    sizes: wcc -> number of accounts. Returns a dict of results."""
    flagged = {c for c, n in sizes.items() if lo <= n <= hi}
    tp = fp = fn = tn = unlabelled = 0
    by_pattern = collections.defaultdict(lambda: [0, 0])    # pattern -> [detected, total]
    by_syndicate = collections.defaultdict(lambda: [0, 0])  # syndicate -> [detected, total]
    seen = set()
    for tx_id, sw, dw in edges:
        lab = labels.get(tx_id)
        if lab is None:
            unlabelled += 1
            continue
        seen.add(tx_id)
        pred = sw is not None and sw == dw and sw in flagged
        actual = lab["pattern"] != "benign"
        if pred and actual:
            tp += 1
        elif pred:
            fp += 1
        elif actual:
            fn += 1
        else:
            tn += 1
        if actual:
            by_pattern[lab["pattern"]][1] += 1
            by_pattern[lab["pattern"]][0] += int(pred)
            by_syndicate[lab["syndicate_id"]][1] += 1
            by_syndicate[lab["syndicate_id"]][0] += int(pred)
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {
        "flagged_groups": sorted((sizes[c] for c in flagged), reverse=True),
        "tp": tp, "fp": fp, "fn": fn, "tn": tn,
        "precision": precision, "recall": recall, "f1": f1,
        "by_pattern": dict(by_pattern), "by_syndicate": dict(by_syndicate),
        "unlabelled_in_graph": unlabelled,
        "labelled_missing_from_graph": len(set(labels) - seen),
    }


def render(res, lo, hi):
    out = []
    out.append("# Detection evaluation (transaction level)\n")
    out.append(f"Flagged group = WCC component with {lo} to {hi} accounts. "
               f"Flagged groups found: {len(res['flagged_groups'])} (sizes {res['flagged_groups']}).\n")
    out.append("| Metric | Value |\n|---|---|")
    out.append(f"| Precision | {res['precision']:.3f} |")
    out.append(f"| Recall | {res['recall']:.3f} |")
    out.append(f"| F1 | {res['f1']:.3f} |")
    out.append(f"| TP / FP / FN / TN | {res['tp']} / {res['fp']} / {res['fn']} / {res['tn']} |\n")
    out.append("| Pattern | Detected | Total | Recall |\n|---|---|---|---|")
    for p, (det, tot) in sorted(res["by_pattern"].items()):
        out.append(f"| {p} | {det} | {tot} | {det / tot:.3f} |")
    out.append("\n| Syndicate | Detected | Total | Recall |\n|---|---|---|---|")
    for s, (det, tot) in sorted(res["by_syndicate"].items(), key=lambda kv: str(kv[0])):
        out.append(f"| {s} | {det} | {tot} | {det / tot:.3f} |")
    out.append(f"\nLabelled transactions missing from the graph: {res['labelled_missing_from_graph']}. "
               f"Graph transactions without a label: {res['unlabelled_in_graph']}.")
    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--labels", default="labels.jsonl")
    ap.add_argument("--min-size", type=int, default=3)
    ap.add_argument("--max-size", type=int, default=200)
    ap.add_argument("--out", default=None, help="also write the report to this .md file")
    a = ap.parse_args()

    labels = {}
    with open(a.labels, encoding="utf-8-sig") as f:
        for line in f:
            if line.strip():
                r = json.loads(line)
                labels[r["tx_id"]] = r

    with GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "fingraph123")) as d:
        sizes = {r["comp"]: r["size"] for r in d.execute_query(SIZES_Q).records}
        edges = [(r["tx_id"], r["sw"], r["dw"]) for r in d.execute_query(EDGES_Q).records]

    if not sizes:
        raise SystemExit("No wcc values on accounts. Run analytics/run_gds.py first.")

    res = evaluate(labels, edges, sizes, a.min_size, a.max_size)
    report = render(res, a.min_size, a.max_size)
    print(report)
    if a.out:
        with open(a.out, "w", encoding="utf-8") as f:
            f.write(report + "\n")
        print(f"\nSaved to {a.out}")


if __name__ == "__main__":
    main()
