"""FinGraph alert rules engine.

Checks account / group risk scores in Neo4j against thresholds and sends
Slack / email alerts, with a per-entity cooldown so the same alert is not
repeated every cycle.

    python alerts/rules_engine.py --dry-run          # print alerts only
    python alerts/rules_engine.py --every 30         # check every 30 s

Secrets come from environment variables (never commit them):
    SLACK_WEBHOOK_URL
    SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, ALERT_TO
"""
import argparse
import json
import os
import smtplib
import time
import urllib.request
from email.message import EmailMessage
from pathlib import Path

from neo4j import GraphDatabase

HERE = Path(__file__).parent
STATE_FILE = HERE / ".alert_state.json"

# Assumes risk scoring (queries 4 and 5) and run_gds.py wrote `risk_score` and
# `community` onto Account nodes, and accounts are keyed by `id`.
ACCOUNT_Q = """
MATCH (a:Account) WHERE a.risk_score >= $t
RETURN a.id AS key, a.risk_score AS score, a.community AS community
ORDER BY score DESC
"""
# A group's risk is the highest risk_score among its members.
GROUP_Q = """
MATCH (a:Account) WHERE a.community IS NOT NULL
WITH a.community AS key, max(a.risk_score) AS score, count(*) AS members
WHERE score >= $t AND members >= $m
RETURN key, score, members
ORDER BY score DESC
"""


def load_state():
    try:
        return json.loads(STATE_FILE.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_state(state):
    STATE_FILE.write_text(json.dumps(state))


def send_slack(text):
    url = os.environ.get("SLACK_WEBHOOK_URL")
    if not url:
        print("  [slack] SLACK_WEBHOOK_URL not set, skipped")
        return
    req = urllib.request.Request(
        url, json.dumps({"text": text}).encode(), {"Content-Type": "application/json"}
    )
    urllib.request.urlopen(req, timeout=10)


def send_email(subject, body):
    host, to = os.environ.get("SMTP_HOST"), os.environ.get("ALERT_TO")
    if not host or not to:
        print("  [email] SMTP_HOST / ALERT_TO not set, skipped")
        return
    msg = EmailMessage()
    msg["Subject"], msg["From"], msg["To"] = subject, os.environ.get("SMTP_USER", "fingraph@localhost"), to
    msg.set_content(body)
    with smtplib.SMTP(host, int(os.environ.get("SMTP_PORT", 587)), timeout=10) as s:
        s.starttls()
        if os.environ.get("SMTP_USER"):
            s.login(os.environ["SMTP_USER"], os.environ.get("SMTP_PASS", ""))
        s.send_message(msg)


def dispatch(rule, text, dry_run):
    print(f"ALERT [{rule['name']}] {text}")
    if dry_run:
        return
    for ch in rule.get("channels", []):
        try:
            if ch == "slack":
                send_slack(text)
            elif ch == "email":
                send_email(f"FinGraph alert: {rule['name']}", text)
        except Exception as e:  # one failing channel must not stop the others
            print(f"  [{ch}] failed: {e}")


def run_once(driver, cfg, dry_run):
    state, now = load_state(), time.time()
    with driver.session() as session:
        for rule in cfg["rules"]:
            cooldown = rule.get("cooldown_minutes", 30) * 60
            if rule["type"] == "account":
                rows = session.run(ACCOUNT_Q, t=rule["threshold"]).data()
            else:
                rows = session.run(GROUP_Q, t=rule["threshold"], m=rule.get("min_members", 3)).data()
            for r in rows:
                sid = f"{rule['name']}:{r['key']}"
                if now - state.get(sid, 0) < cooldown:
                    continue  # still cooling down
                if rule["type"] == "account":
                    text = f"Account {r['key']} risk {r['score']:.2f} >= {rule['threshold']} (community {r['community']})"
                else:
                    text = f"Group {r['key']} max risk {r['score']:.2f} across {r['members']} accounts"
                dispatch(rule, text, dry_run)
                state[sid] = now
    save_state(state)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=str(HERE / "rules.json"))
    ap.add_argument("--every", type=int, default=0, help="repeat every N seconds")
    ap.add_argument("--dry-run", action="store_true", help="print alerts, send nothing")
    args = ap.parse_args()

    cfg = json.loads(Path(args.config).read_text())
    n = cfg["neo4j"]
    driver = GraphDatabase.driver(n["uri"], auth=(n["user"], n["password"]))
    try:
        while True:
            run_once(driver, cfg, args.dry_run)
            if not args.every:
                break
            time.sleep(args.every)
    finally:
        driver.close()


if __name__ == "__main__":
    main()
