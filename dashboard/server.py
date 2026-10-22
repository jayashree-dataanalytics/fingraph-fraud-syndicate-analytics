"""Serves the dashboard and a small JSON API backed by Neo4j.
Run:  python dashboard/server.py   then open http://localhost:8000
(Run analytics/run_gds.py first so flagged groups can be selected by WCC.)"""
import json, os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from neo4j import GraphDatabase

DRV = GraphDatabase.driver(os.getenv("NEO4J_URI", "bolt://localhost:7687"),
                           auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "fingraph123")))
HERE = os.path.dirname(os.path.abspath(__file__))
SUSPICIOUS = """
MATCH (a:Account) WHERE a.wcc IS NOT NULL
WITH a.wcc AS c, count(*) AS n WHERE n >= 3 AND n <= 200
WITH collect(c) AS comps
MATCH (s:Account)-[t:TRANSFERRED_TO]->(d:Account) WHERE s.wcc IN comps
RETURN s.id AS source, d.id AS target, sum(t.amount) AS total LIMIT 3000"""
RECENT = """
MATCH (s:Account)-[t:TRANSFERRED_TO]->(d:Account)
WITH s, d, sum(t.amount) AS total, max(t.ts) AS ts ORDER BY ts DESC LIMIT 600
RETURN s.id AS source, d.id AS target, total"""
NODES = """MATCH (a:Account) WHERE a.id IN $ids
RETURN a.id AS id, a.pr AS pr, a.risk_score AS risk, a.status AS status"""


def graph():
    links = [dict(r) for r in DRV.execute_query(SUSPICIOUS).records] or \
            [dict(r) for r in DRV.execute_query(RECENT).records]
    ids = list({x for l in links for x in (l["source"], l["target"])})
    nodes = [dict(r) for r in DRV.execute_query(NODES, ids=ids).records]
    return {"nodes": nodes, "links": links}


class H(BaseHTTPRequestHandler):
    def _send(self, body, ctype="application/json", code=200):
        b = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.send_response(code); self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b)

    def do_GET(self):
        if self.path.startswith("/api/graph"):
            try: self._send(graph())
            except Exception as e: self._send({"error": str(e)}, code=500)
        else:
            self._send(open(os.path.join(HERE, "index.html"), "rb").read(), "text/html")

    def do_POST(self):
        if self.path == "/api/freeze":
            d = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
            DRV.execute_query("MATCH (a:Account) WHERE a.id IN $ids SET a.status = $status", ids=d["ids"], status=d["status"])
            self._send({"ok": True})


if __name__ == "__main__":
    print("Dashboard on http://localhost:8000")
    ThreadingHTTPServer(("0.0.0.0", 8000), H).serve_forever()
