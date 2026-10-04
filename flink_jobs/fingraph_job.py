"""
FinGraph stream processor (PyFlink).
Kafka (transactions.raw) -> clean -> dedupe by tx_id -> micro-batched idempotent upsert into Neo4j.

Run:  python flink_jobs/fingraph_job.py            (reads only new events; start BEFORE the simulator)
      python flink_jobs/fingraph_job.py --from-beginning
Needs: flink-sql-connector-kafka-3.2.0-1.19.jar in flink_jobs/  (see README)
"""
import json, os, sys, threading, time
from pathlib import Path

from neo4j import GraphDatabase
from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Time
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.functions import KeyedProcessFunction, MapFunction
from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "transactions.raw"
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_AUTH = (os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "fingraph123"))
BATCH_SIZE = 300      # flush when this many rows are buffered...
FLUSH_MS = 200        # ...or every 200 ms, whichever comes first
JAR = Path(__file__).with_name("flink-sql-connector-kafka-3.2.0-1.19.jar").resolve()

UPSERT = """
UNWIND $rows AS r
MERGE (s:Account {id: r.src})
MERGE (d:Account {id: r.dst})
MERGE (s)-[t:TRANSFERRED_TO {tx_id: r.tx_id}]->(d)
  ON CREATE SET t.amount = r.amount,
                t.ts = datetime(r.ts),
                t.sim_ts = r.sim_ts,
                t.write_ts = timestamp()
"""


def clean(raw):
    """Drop malformed / invalid events; normalise types."""
    try:
        e = json.loads(raw)
        if not (e.get("tx_id") and e.get("src") and e.get("dst")):
            return
        if e["src"] == e["dst"]:
            return
        e["amount"] = float(e["amount"])
        if e["amount"] <= 0:
            return
        yield json.dumps(e)
    except Exception:
        return


class Dedupe(KeyedProcessFunction):
    """Keyed by tx_id: pass each transaction once (state expires after 1 hour)."""

    def open(self, runtime_context):
        desc = ValueStateDescriptor("seen", Types.BOOLEAN())
        desc.enable_time_to_live(
            StateTtlConfig.new_builder(Time.hours(1))
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build())
        self.seen = runtime_context.get_state(desc)

    def process_element(self, value, ctx):
        if self.seen.value() is None:
            self.seen.update(True)
            yield value


class Neo4jBatchWriter(MapFunction):
    """Buffers events and writes them to Neo4j in micro-batches (idempotent MERGE on tx_id)."""

    def open(self, runtime_context):
        self.driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
        self.buf, self.lock, self.flush_lock = [], threading.Lock(), threading.Lock()
        self.running = True
        threading.Thread(target=self._timer, daemon=True).start()

    def map(self, value):
        with self.lock:
            self.buf.append(json.loads(value))
            full = len(self.buf) >= BATCH_SIZE
        if full:
            self._flush()
        return value

    def _timer(self):
        while self.running:
            time.sleep(FLUSH_MS / 1000)
            self._flush()

    def _flush(self):
        with self.flush_lock:                 # one writer at a time -> no MERGE deadlocks
            with self.lock:
                rows, self.buf = self.buf, []
            if not rows:
                return
            try:
                with self.driver.session() as s:
                    s.execute_write(lambda tx: tx.run(UPSERT, rows=rows).consume())
            except Exception as ex:           # put rows back; retry on next flush
                print(f"[neo4j] write failed, will retry: {ex}", file=sys.stderr)
                with self.lock:
                    self.buf = rows + self.buf

    def close(self):
        self.running = False
        self._flush()
        self.driver.close()


def main():
    from_beginning = "--from-beginning" in sys.argv
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(5000)
    env.add_jars(JAR.as_uri())

    source = (KafkaSource.builder()
              .set_bootstrap_servers(KAFKA)
              .set_topics(TOPIC)
              .set_group_id("fingraph-flink")
              .set_starting_offsets(KafkaOffsetsInitializer.earliest() if from_beginning
                                    else KafkaOffsetsInitializer.latest())
              .set_value_only_deserializer(SimpleStringSchema())
              .build())

    (env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-transactions")
        .flat_map(clean, output_type=Types.STRING())
        .key_by(lambda s: json.loads(s)["tx_id"], key_type=Types.STRING())
        .process(Dedupe(), output_type=Types.STRING())
        .map(Neo4jBatchWriter(), output_type=Types.STRING())
        .filter(lambda x: False)              # operator must end in a sink; nothing to print
        .print())

    env.execute("fingraph-stream-processor")


if __name__ == "__main__":
    main()
