"""FinGraph transaction simulator: benign noise + labeled laundering syndicates -> Kafka."""
import argparse, json, random, time, uuid
from datetime import datetime, timezone
from confluent_kafka import Producer

TOPIC = "transactions.raw"
N_BENIGN_ACCOUNTS = 2000


def acct(n):
    return f"ACC{n:06d}"


def starburst(syn_id, n=50):
    shell = acct(900000 + syn_id)
    return [(acct(800000 + syn_id * 1000 + i), shell, random.uniform(9500, 9900), "starburst", syn_id)
            for i in range(n)]


def layering(syn_id, hops=4):
    chain = [acct(700000 + syn_id * 10 + i) for i in range(hops + 2)]
    amt, out = random.uniform(40000, 90000), []
    for a, b in zip(chain, chain[1:]):
        out.append((a, b, amt, "layering", syn_id))
        amt *= 0.97  # fee skim per hop
    return out


def cycle(syn_id, size=3):
    ring = [acct(600000 + syn_id * 10 + i) for i in range(size)]
    amt, out = random.uniform(5000, 20000), []
    for i in range(size):
        out.append((ring[i], ring[(i + 1) % size], amt * random.uniform(0.97, 1.0), "cycle", syn_id))
    return out


def benign(n):
    out = []
    for _ in range(n):
        a, b = random.sample(range(N_BENIGN_ACCOUNTS), 2)
        out.append((acct(a), acct(b), min(random.lognormvariate(5.5, 1.0), 8000), "benign", None))
    return out


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--rate", type=int, default=200, help="events per second")
    p.add_argument("--duration", type=int, default=60, help="seconds")
    p.add_argument("--syndicate-every", type=int, default=15, help="seconds between syndicates")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--labels", default="labels.jsonl")
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args()
    random.seed(args.seed)

    producer = Producer({"bootstrap.servers": args.bootstrap, "linger.ms": 5})
    syn_id, sent, start = 0, 0, time.time()
    next_syn = start

    with open(args.labels, "w") as lf:
        while time.time() - start < args.duration:
            batch = benign(args.rate)
            if time.time() >= next_syn:
                syn_id += 1
                batch += random.choice([starburst, layering, cycle])(syn_id)
                next_syn += args.syndicate_every
            random.shuffle(batch)
            interval = 1.0 / max(len(batch), 1)
            for src, dst, amount, pattern, sid in batch:
                tx_id = str(uuid.uuid4())
                now = datetime.now(timezone.utc)
                msg = {"tx_id": tx_id, "src": src, "dst": dst, "amount": round(amount, 2),
                       "currency": "USD", "ts": now.isoformat(),
                       "sim_ts": int(now.timestamp() * 1000)}  # used for latency proof in Week 2
                producer.produce(TOPIC, key=src, value=json.dumps(msg))
                lf.write(json.dumps({"tx_id": tx_id, "pattern": pattern, "syndicate_id": sid}) + "\n")
                sent += 1
                time.sleep(interval)
            producer.poll(0)
    producer.flush()
    print(f"Sent {sent} events, {syn_id} syndicates. Labels in {args.labels}")


if __name__ == "__main__":
    main()
