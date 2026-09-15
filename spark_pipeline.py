import json, uuid, time, random, math
from datetime import datetime, timedelta, timezone

from pyspark.sql import functions as F, types as Ttypes

CATALOG, SCHEMA, VOLUME = "workspace", "selfheal", "landing"
LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
T = lambda name: f"{CATALOG}.{SCHEMA}.{name}"

NULL_THRESH, RANGE_THRESH, DUP_THRESH = 0.50, 0.30, 0.30
RESCUE_THRESH, HUMIDITY_NULL_THRESH = 0.50, 0.50
PSI_THRESH, VOL_LOW, VOL_HIGH = 2.0, 0.50, 2.0
LATE_THRESH_MIN, EXPECTED_BATCH, MAX_RETRIES = 60, 50, 2

VALID = (
    F.col("temperature_c").isNotNull()
    & (F.col("temperature_c") >= -40) & (F.col("temperature_c") <= 80)
    & (F.col("humidity_pct").isNull() | ((F.col("humidity_pct") >= 0) & (F.col("humidity_pct") <= 100)))
)

SEED_RULES = [
    ("r_dup", 10, "DUPLICATES", "dedup", [{"metric": "dup_rate", "op": ">", "value": 0.3}]),
    ("r_empty", 20, "EMPTY_BATCH", "escalate", [{"metric": "total", "op": "<", "value": 5}]),
    ("r_spike", 30, "VOLUME_SPIKE", "escalate", [{"metric": "volume_ratio", "op": ">", "value": 3}]),
    ("r_schema", 40, "SCHEMA_DRIFT", "absorb_schema", [{"metric": "rescued_rate", "op": ">", "value": 0.4}]),
    ("r_schema2", 41, "SCHEMA_DRIFT", "absorb_schema", [{"metric": "schema_drift", "op": ">", "value": 0.5}]),
    ("r_null", 50, "NULL_FLOOD", "quarantine_correct", [{"metric": "null_rate", "op": ">", "value": 0.5}]),
    ("r_range", 60, "RANGE_VIOLATION", "clamp", [{"metric": "range_rate", "op": ">", "value": 0.3}]),
    ("r_late", 70, "LATE_DATA", "flag_backfill", [{"metric": "freshness_min", "op": ">", "value": 60}]),
]
_KLASS = {"dedup": "fix", "clamp": "fix", "absorb_schema": "fix", "flag_backfill": "fix",
          "none": "fix", "quarantine_correct": "quarantine", "escalate": "escalate"}
import operator as _op
_OPS = {">": _op.gt, "<": _op.lt, ">=": _op.ge, "<=": _op.le, "==": _op.eq}


class Escalate(Exception):
    def __init__(self, reason):
        self.reason = reason


def setup():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
    ddl = {
        "pipelines": "pipeline_id STRING, source STRING, null_thresh DOUBLE, range_thresh DOUBLE, "
                     "dup_thresh DOUBLE, psi_thresh DOUBLE, vol_low DOUBLE, vol_high DOUBLE, active BOOLEAN",
        "fault_log": "fault_id STRING, fault_type STRING, injected_at TIMESTAMP, row_count INT, path STRING",
        "metrics": "metric_id STRING, pipeline_id STRING, cycle_ts TIMESTAMP, freshness_min DOUBLE, "
                   "volume LONG, volume_ratio DOUBLE, null_rate DOUBLE, range_rate DOUBLE, dup_rate DOUBLE, "
                   "rescued_rate DOUBLE, schema_drift BOOLEAN, psi DOUBLE, verdict STRING",
        "pipeline_state": "pipeline_id STRING, state STRING, since TIMESTAMP, retry_count INT, last_metric_id STRING",
        "state_transitions": "transition_id STRING, pipeline_id STRING, from_state STRING, to_state STRING, "
                             "trigger STRING, ts TIMESTAMP",
        "remediation_log": "rem_id STRING, pipeline_id STRING, cycle_ts TIMESTAMP, diagnosis STRING, "
                           "action STRING, rows_affected LONG, outcome STRING, duration_sec DOUBLE",
        "incidents": "incident_id STRING, batch_id LONG, reason STRING, signals STRING, ts TIMESTAMP, state STRING",
        "rules": "rule_id STRING, priority INT, name STRING, condition STRING, diagnosis STRING, action STRING, "
                 "source STRING, approved_by STRING, created_at TIMESTAMP, active BOOLEAN",
    }
    for name, cols in ddl.items():
        spark.sql(f"CREATE TABLE IF NOT EXISTS {T(name)} ({cols}) USING DELTA")
    if spark.read.table(T("pipelines")).filter(F.col("pipeline_id") == "sensors_iot").count() == 0:
        spark.createDataFrame(
            [("sensors_iot", "file", 0.5, 0.3, 0.3, 2.0, 0.5, 2.0, True)], ddl["pipelines"]
        ).write.mode("append").saveAsTable(T("pipelines"))
    if not spark.catalog.tableExists(T("baseline")):
        random.seed(42)
        base = [(round(random.gauss(22, 3), 2),) for _ in range(500)]
        spark.createDataFrame(base, "temperature_c double").write.mode("overwrite").saveAsTable(T("baseline"))
    seed_rules()


def seed_rules():
    if spark.read.table(T("rules")).count() > 0:
        return
    now = datetime.now(timezone.utc)
    rows = [(rid, pr, dx.lower(), json.dumps(cond), dx, act, "hardcoded", "system", now, True)
            for (rid, pr, dx, act, cond) in SEED_RULES]
    spark.createDataFrame(
        rows,
        "rule_id string, priority int, name string, condition string, diagnosis string, action string, "
        "source string, approved_by string, created_at timestamp, active boolean",
    ).write.mode("append").saveAsTable(T("rules"))


def make_reading(now, mean=22.0, sd=3.0):
    return {"event_id": str(uuid.uuid4()), "device_id": f"dev-{random.randint(1,10):02d}",
            "event_time": now.isoformat(), "temperature_c": round(random.gauss(mean, sd), 2),
            "humidity_pct": round(random.uniform(30, 70), 1), "battery_pct": round(random.uniform(20, 100), 1),
            "status": random.choice(["OK", "OK", "OK", "WARN"])}


_last_batch = {"rows": None}


def apply_fault(rows, fault):
    now = datetime.now(timezone.utc)
    if fault == "SCHEMA_ADD":
        for r in rows:
            r["debug_note"] = "x"
    elif fault == "TYPE_FLIP":
        for r in rows:
            r["temperature_c"] = "N/A"
    elif fault == "NULL_FLOOD":
        for r in rows:
            r["temperature_c"] = None
    elif fault == "GARBAGE":
        for r in rows:
            r["temperature_c"] = 9999.0
            r["humidity_pct"] = -50.0
    elif fault == "DUPLICATE":
        rows = _last_batch["rows"] or rows
    elif fault == "LATE_DATA":
        old = (now - timedelta(hours=6)).isoformat()
        for r in rows:
            r["event_time"] = old
    elif fault == "VOLUME_DROP":
        rows = rows[:1]
    elif fault == "VOLUME_SPIKE":
        rows = rows + [make_reading(now) for _ in range(len(rows) * 9)]
    elif fault == "SCHEMA_DROP":
        for r in rows:
            r.pop("humidity_pct", None)
    return rows


def emit(fault="NONE", n=EXPECTED_BATCH):
    now = datetime.now(timezone.utc)
    rows = [make_reading(now) for _ in range(n)]
    rows = apply_fault(rows, fault)
    _last_batch["rows"] = [dict(r) for r in rows]
    fname = f"batch_{now.strftime('%Y%m%d%H%M%S%f')}_{uuid.uuid4().hex[:6]}.json"
    path = f"{LANDING}/{fname}"
    with open(path, "w") as f:
        f.write("\n".join(json.dumps(r) for r in rows))
    spark.createDataFrame(
        [(str(uuid.uuid4()), fault, now, len(rows), path)],
        "fault_id string, fault_type string, injected_at timestamp, row_count int, path string",
    ).write.mode("append").saveAsTable(T("fault_log"))
    return path


def run_chaos(steps=25, chaos_rate=0.4):
    faults = ["SCHEMA_ADD", "TYPE_FLIP", "NULL_FLOOD", "GARBAGE", "DUPLICATE",
              "LATE_DATA", "VOLUME_DROP", "VOLUME_SPIKE", "SCHEMA_DROP"]
    for _ in range(steps):
        emit(random.choice(faults) if random.random() < chaos_rate else "NONE")
        time.sleep(0.2)


def ingest_bronze():
    q = (spark.readStream.format("cloudFiles")
         .option("cloudFiles.format", "json")
         .option("cloudFiles.schemaLocation", f"{LANDING}/_schema")
         .option("cloudFiles.inferColumnTypes", "true")
         .option("cloudFiles.schemaEvolutionMode", "rescue")
         .load(LANDING)
         .withColumn("_ingest_time", F.current_timestamp())
         .withColumn("_source_file", F.col("_metadata.file_path"))
         .writeStream.format("delta")
         .option("checkpointLocation", f"{LANDING}/_ckpt_bronze")
         .trigger(availableNow=True)
         .toTable(T("bronze_readings")))
    q.awaitTermination()


def compute_psi(df, bins=8):
    import numpy as np
    if not spark.catalog.tableExists(T("baseline")):
        return None, "no_baseline"
    base = [r[0] for r in spark.read.table(T("baseline")).select("temperature_c")
            .where(F.col("temperature_c").isNotNull()).collect()]
    cur = [r[0] for r in df.select("temperature_c").where(F.col("temperature_c").isNotNull()).collect()]
    if len(base) < 20:
        return None, "no_baseline"
    if len(cur) < 15:
        return None, "insufficient"
    lo, hi = min(base), max(base)
    if hi <= lo:
        return None, "no_baseline"
    edges = np.linspace(lo, hi, bins + 1)
    e, _ = np.histogram(base, edges)
    a, _ = np.histogram(cur, edges)
    e = np.clip(e / max(e.sum(), 1), 1e-6, None)
    a = np.clip(a / max(a.sum(), 1), 1e-6, None)
    return float(np.sum((a - e) * np.log(a / e))), "ok"


def load_cfg(pid="sensors_iot"):
    return spark.read.table(T("pipelines")).filter(F.col("pipeline_id") == pid).collect()[0].asDict()


def sense(df, cfg):
    pid = cfg["pipeline_id"]
    total = df.count()
    now = datetime.now(timezone.utc)
    if total == 0:
        signals = dict(total=0, freshness_min=0.0, volume=0, volume_ratio=0.0, null_rate=0.0,
                       range_rate=0.0, dup_rate=0.0, rescued_rate=0.0, schema_drift=False,
                       psi=None, psi_available=False, sensing_failed=False, verdict="RED")
    else:
        fr = df.select((F.max(F.unix_timestamp(F.current_timestamp())
                              - F.unix_timestamp(F.to_timestamp("event_time"))) / 60.0).alias("v")
                       ).collect()[0]["v"]
        freshness_min = float(fr or 0.0)
        agg = df.select(
            (F.count(F.when(F.col("temperature_c").isNull(), 1)) / total).alias("nr"),
            (F.count(F.when(F.col("_rescued_data").isNotNull(), 1)) / total).alias("rr"),
            (F.count(F.when((F.col("temperature_c") > 80) | (F.col("temperature_c") < -40)
                            | (F.col("humidity_pct") > 100) | (F.col("humidity_pct") < 0), 1)) / total).alias("rg"),
            (F.count(F.when(F.col("humidity_pct").isNull(), 1)) / total).alias("hn"),
        ).collect()[0]
        distinct = df.select("event_id").distinct().count()
        null_rate, rescued_rate, range_rate = float(agg["nr"]), float(agg["rr"]), float(agg["rg"])
        dup_rate = float(1 - distinct / total)
        schema_drift = bool(rescued_rate > RESCUE_THRESH or float(agg["hn"]) > HUMIDITY_NULL_THRESH)
        base = (spark.read.table(T("metrics"))
                .filter((F.col("pipeline_id") == pid) & (F.col("verdict") == "GREEN"))
                .orderBy(F.col("cycle_ts").desc()).limit(10)
                .agg(F.avg("volume").alias("b")).collect()[0]["b"]) or EXPECTED_BATCH
        volume_ratio = float(total / base) if base else 1.0
        psi, psi_status = compute_psi(df)
        psi_ok = psi_status == "ok"
        sensing_failed = psi_status == "no_baseline"
        red = (schema_drift or null_rate > cfg["null_thresh"] or range_rate > cfg["range_thresh"]
               or dup_rate > cfg["dup_thresh"] or volume_ratio < cfg["vol_low"] or volume_ratio > cfg["vol_high"])
        amber = (freshness_min > LATE_THRESH_MIN) or (psi_ok and psi > cfg["psi_thresh"])
        verdict = "RED" if red else ("AMBER" if (amber or sensing_failed) else "GREEN")
        signals = dict(total=total, freshness_min=freshness_min, volume=total, volume_ratio=volume_ratio,
                       null_rate=null_rate, range_rate=range_rate, dup_rate=dup_rate,
                       rescued_rate=rescued_rate, schema_drift=schema_drift, psi=psi,
                       psi_available=psi_ok, sensing_failed=sensing_failed, verdict=verdict)
    if signals["sensing_failed"]:
        spark.createDataFrame(
            [(str(uuid.uuid4()), -1, "SENSING_UNAVAILABLE: psi baseline missing", "{}", now, "OPEN")],
            "incident_id string, batch_id long, reason string, signals string, ts timestamp, state string",
        ).write.mode("append").saveAsTable(T("incidents"))
    mid = str(uuid.uuid4())
    spark.createDataFrame(
        [(mid, pid, now, signals["freshness_min"], int(signals["volume"]), float(signals["volume_ratio"]),
          signals["null_rate"], signals["range_rate"], signals["dup_rate"], signals["rescued_rate"],
          signals["schema_drift"], signals["psi"], signals["verdict"])],
        "metric_id string, pipeline_id string, cycle_ts timestamp, freshness_min double, volume long, "
        "volume_ratio double, null_rate double, range_rate double, dup_rate double, rescued_rate double, "
        "schema_drift boolean, psi double, verdict string",
    ).write.mode("append").saveAsTable(T("metrics"))
    signals["metric_id"] = mid
    return signals


def capture_baseline(n=500):
    (spark.read.table(T("silver_readings")).limit(n).select("temperature_c")
     .where("temperature_c IS NOT NULL").write.mode("overwrite").saveAsTable(T("baseline")))


def check_source_freshness(pid="sensors_iot", max_gap_min=15):
    last = spark.read.table(T("bronze_readings")).agg(F.max("_ingest_time").alias("t")).collect()[0]["t"]
    gap = None if last is None else (datetime.now(timezone.utc) - last.replace(tzinfo=timezone.utc)).total_seconds() / 60.0
    if gap is None or gap > max_gap_min:
        spark.createDataFrame(
            [(str(uuid.uuid4()), -1, f"DEAD_SOURCE gap={gap}", json.dumps({"gap_min": gap}),
              datetime.now(timezone.utc), "OPEN")],
            "incident_id string, batch_id long, reason string, signals string, ts timestamp, state string",
        ).write.mode("append").saveAsTable(T("incidents"))
        return False
    return True


def get_state(pid):
    rows = (spark.read.table(T("pipeline_state")).filter(F.col("pipeline_id") == pid)
            .orderBy(F.col("since").desc()).limit(1).collect())
    return (rows[0]["state"], rows[0]["retry_count"]) if rows else ("HEALTHY", 0)


def set_state(pid, frm, to, trigger, retry=0, metric_id=None):
    if frm == to:
        return
    now = datetime.now(timezone.utc)
    spark.createDataFrame(
        [(str(uuid.uuid4()), pid, frm, to, trigger, now)],
        "transition_id string, pipeline_id string, from_state string, to_state string, trigger string, ts timestamp",
    ).write.mode("append").saveAsTable(T("state_transitions"))
    spark.createDataFrame(
        [(pid, to, now, int(retry), metric_id)],
        "pipeline_id string, state string, since timestamp, retry_count int, last_metric_id string",
    ).write.mode("append").saveAsTable(T("pipeline_state"))


def next_state(cur, verdict):
    if verdict == "GREEN":
        return "HEALTHY" if cur in ("HEALTHY", "RECOVERED", "HEALING") else "RECOVERED"
    return {"HEALTHY": "DEGRADED", "DEGRADED": "HEALING"}.get(cur, cur)


def diagnose(s):
    ns = {"null_rate": s["null_rate"], "range_rate": s["range_rate"], "dup_rate": s["dup_rate"],
          "rescued_rate": s["rescued_rate"], "volume_ratio": s["volume_ratio"],
          "freshness_min": s["freshness_min"], "psi": s["psi"] if s.get("psi") is not None else -1.0,
          "schema_drift": 1.0 if s["schema_drift"] else 0.0, "total": float(s["total"])}
    for r in spark.read.table(T("rules")).filter("active = true").orderBy("priority").collect():
        clauses = json.loads(r["condition"])
        if all(_OPS[c["op"]](ns.get(c["metric"], 0.0), c["value"]) for c in clauses):
            return r["diagnosis"], r["action"], _KLASS.get(r["action"], "escalate")
    return ("UNKNOWN", "escalate", "escalate") if s["verdict"] != "GREEN" else ("HEALTHY", "none", "fix")


def write_dead_letter(df, dx, pid, batch_id):
    if df is None or df.limit(1).count() == 0:
        return
    (df.withColumn("_diagnosis", F.lit(dx)).withColumn("_pipeline", F.lit(pid))
     .withColumn("_batch_id", F.lit(batch_id)).withColumn("_dead_at", F.current_timestamp())
     .write.mode("append").option("mergeSchema", "true").saveAsTable(T("dead_letter")))


def _quarantine_invalid(df, dx, pid, batch_id):
    keep = F.coalesce(VALID, F.lit(False))
    write_dead_letter(df.filter(~keep), dx, pid, batch_id)
    return df.filter(keep)


def _clamp(df, pid, batch_id):
    clamped = (df.withColumn("temperature_c",
                             F.when(F.col("temperature_c") > 80, 80.0)
                              .when(F.col("temperature_c") < -40, -40.0).otherwise(F.col("temperature_c")))
               .withColumn("humidity_pct",
                           F.when(F.col("humidity_pct") > 100, 100.0)
                            .when(F.col("humidity_pct") < 0, 0.0).otherwise(F.col("humidity_pct"))))
    return _quarantine_invalid(clamped, "RANGE_VIOLATION", pid, batch_id)


def _quarantine_correct(df, pid, batch_id):
    keep = F.coalesce(VALID, F.lit(False))
    good = df.filter(keep)
    fixed = (df.filter(~keep).withColumn("temperature_c", F.col("temperature_c").cast("double"))
             .withColumn("battery_pct", F.coalesce(F.col("battery_pct"), F.lit(-1.0))))
    keep2 = F.coalesce(VALID, F.lit(False))
    write_dead_letter(fixed.filter(~keep2), "NULL_FLOOD", pid, batch_id)
    return good.unionByName(fixed.filter(keep2), allowMissingColumns=True)


def remediate(df, action, pid, batch_id):
    if action == "escalate":
        raise Escalate("rule_action_escalate")
    if action == "dedup":
        return _quarantine_invalid(df.dropDuplicates(["event_id"]), "DUPLICATES", pid, batch_id)
    if action == "clamp":
        return _clamp(df, pid, batch_id)
    if action == "quarantine_correct":
        return _quarantine_correct(df, pid, batch_id)
    if action == "absorb_schema":
        return _quarantine_invalid(df, "SCHEMA_DRIFT", pid, batch_id)
    if action == "flag_backfill":
        return _quarantine_invalid(df, "LATE_DATA", pid, batch_id).withColumn("_late", F.lit(True))
    if action == "none":
        return _quarantine_invalid(df, "PASSTHROUGH_INVALID", pid, batch_id)
    raise Escalate(f"unknown_action:{action}")


def verify(df, cfg):
    total = df.count()
    if total == 0:
        return False
    a = df.select(
        (F.count(F.when(F.col("temperature_c").isNull(), 1)) / total).alias("nr"),
        (F.count(F.when((F.col("temperature_c") > 80) | (F.col("temperature_c") < -40), 1)) / total).alias("rr"),
    ).collect()[0]
    dup = 1 - df.select("event_id").distinct().count() / total
    return a["nr"] < cfg["null_thresh"] and a["rr"] < cfg["range_thresh"] and dup < cfg["dup_thresh"]


def heartbeat(pid="sensors_iot"):
    spark.createDataFrame([(pid, datetime.now(timezone.utc))], "pipeline_id string, beat_at timestamp") \
        .write.mode("append").option("mergeSchema", "true").saveAsTable(T("supervisor_heartbeat"))


def log_remediation(pid, dx, action, rows, outcome, dur):
    spark.createDataFrame(
        [(str(uuid.uuid4()), pid, datetime.now(timezone.utc), dx, action, int(rows), outcome, float(dur))],
        "rem_id string, pipeline_id string, cycle_ts timestamp, diagnosis string, action string, "
        "rows_affected long, outcome string, duration_sec double",
    ).write.mode("append").saveAsTable(T("remediation_log"))


def write_silver(df, batch_id):
    (df.withColumn("_batch_id", F.lit(int(batch_id)))
     .write.mode("append").option("mergeSchema", "true")
     .option("txnAppId", "selfheal_silver").option("txnVersion", int(batch_id))
     .saveAsTable(T("silver_readings")))


def escalate(batch_id, reason, signals, pid):
    summary = ""
    handler = globals().get("llm_incident_handler")
    if handler:
        try:
            summary = handler(signals, reason, pid)
        except Exception as ex:
            summary = f"(llm handler failed: {ex})"
    blob = json.dumps({k: v for k, v in signals.items() if k != "metric_id"}) + (f" | LLM: {summary}" if summary else "")
    spark.createDataFrame(
        [(str(uuid.uuid4()), int(batch_id), reason, blob, datetime.now(timezone.utc), "OPEN")],
        "incident_id string, batch_id long, reason string, signals string, ts timestamp, state string",
    ).write.mode("append").saveAsTable(T("incidents"))


def supervise(bronze_df, batch_id, pid="sensors_iot"):
    heartbeat(pid)
    cfg = load_cfg(pid)
    s = sense(bronze_df, cfg)
    cur, retry = get_state(pid)
    if s["verdict"] == "GREEN":
        write_silver(_quarantine_invalid(bronze_df, "BELOW_THRESHOLD", pid, batch_id), batch_id)
        set_state(pid, cur, next_state(cur, "GREEN"), "verdict:GREEN", 0, s["metric_id"])
        return
    dx, action, klass = diagnose(s)
    set_state(pid, cur, "DEGRADED", f"verdict:{s['verdict']}", retry, s["metric_id"])
    set_state(pid, "DEGRADED", "HEALING", f"diagnosis:{dx}", retry, s["metric_id"])
    t0 = time.time()
    try:
        if action == "escalate" or dx == "UNKNOWN":
            raise Escalate(dx)
        healed = remediate(bronze_df, action, pid, batch_id)
        if verify(healed, cfg):
            write_silver(healed, batch_id)
            log_remediation(pid, dx, action, healed.count(), klass.upper(), time.time() - t0)
            set_state(pid, "HEALING", "RECOVERED", f"{klass}:{action}", 0, s["metric_id"])
        elif retry < MAX_RETRIES:
            log_remediation(pid, dx, action, 0, "RETRY", time.time() - t0)
            set_state(pid, "HEALING", "HEALING", "verify_failed", retry + 1, s["metric_id"])
        else:
            log_remediation(pid, dx, action, 0, "ESCALATE", time.time() - t0)
            escalate(batch_id, f"unresolved:{dx}", s, pid)
            set_state(pid, "HEALING", "ESCALATED", "retries_exhausted", 0, s["metric_id"])
    except Escalate as e:
        log_remediation(pid, dx, "escalate", 0, "ESCALATE", time.time() - t0)
        escalate(batch_id, e.reason, s, pid)
        set_state(pid, "HEALING", "ESCALATED", e.reason, 0, s["metric_id"])


def run_supervisor():
    q = (spark.readStream.table(T("bronze_readings"))
         .writeStream.foreachBatch(lambda df, bid: supervise(df, bid, "sensors_iot"))
         .option("checkpointLocation", f"{LANDING}/_ckpt_supervisor")
         .trigger(availableNow=True).start())
    q.awaitTermination()


def refresh_gold():
    win = F.window(F.to_timestamp("event_time"), "5 minutes")
    (spark.read.table(T("silver_readings"))
     .groupBy("device_id", win.alias("win"))
     .agg(F.avg("temperature_c").alias("avg_temp"), F.count("*").alias("readings"))
     .select("device_id", F.col("win.start").alias("window_start"),
             F.col("win.end").alias("window_end"), "avg_temp", "readings")
     .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T("gold_device_5min")))


def gold_is_healthy():
    return spark.read.table(T("gold_device_5min")).filter("avg_temp > 80 OR avg_temp < -40 OR readings <= 0").count() == 0


def record_good_version(pid="sensors_iot"):
    spark.sql(f"CREATE TABLE IF NOT EXISTS {T('gold_checkpoints')} "
              f"(pipeline_id STRING, table_name STRING, good_version LONG, ts TIMESTAMP) USING DELTA")
    v = spark.sql(f"DESCRIBE HISTORY {T('gold_device_5min')}").agg(F.max("version").alias("v")).collect()[0]["v"]
    spark.createDataFrame(
        [(pid, "gold_device_5min", int(v), datetime.now(timezone.utc))],
        "pipeline_id string, table_name string, good_version long, ts timestamp",
    ).write.mode("append").saveAsTable(T("gold_checkpoints"))


def rollback_gold():
    rows = spark.read.table(T("gold_checkpoints")).orderBy(F.col("ts").desc()).limit(1).collect()
    if not rows:
        return None
    v = rows[0]["good_version"]
    spark.sql(f"RESTORE TABLE {T('gold_device_5min')} VERSION AS OF {v}")
    return v


def refresh_gold_safe():
    refresh_gold()
    if gold_is_healthy():
        record_good_version()
    else:
        rollback_gold()


def backfill_late():
    silver = spark.read.table(T("silver_readings"))
    if "_late" not in silver.columns:
        return
    with_win = (silver.withColumn("win", F.window(F.to_timestamp("event_time"), "5 minutes"))
                .withColumn("window_start", F.col("win.start")).withColumn("window_end", F.col("win.end")))
    late_keys = with_win.filter(F.col("_late") == True).select("device_id", "window_start").distinct()
    if late_keys.count() == 0:
        return
    affected = (with_win.join(late_keys, ["device_id", "window_start"], "inner")
                .groupBy("device_id", "window_start", "window_end")
                .agg(F.avg("temperature_c").alias("avg_temp"), F.count("*").alias("readings")))
    affected.createOrReplaceTempView("affected_updates")
    spark.sql(f"""
        MERGE INTO {T('gold_device_5min')} g USING affected_updates u
          ON g.device_id = u.device_id AND g.window_start = u.window_start
        WHEN MATCHED THEN UPDATE SET avg_temp = u.avg_temp, readings = u.readings
        WHEN NOT MATCHED THEN INSERT (device_id, window_start, window_end, avg_temp, readings)
             VALUES (u.device_id, u.window_start, u.window_end, u.avg_temp, u.readings)
    """)


_ALLOWED_ACTIONS = {"dedup", "clamp", "quarantine_correct", "absorb_schema", "flag_backfill", "escalate"}
_LLM_ENDPOINT = "databricks-meta-llama-3-1-8b-instruct"


def _ask_llm(signals):
    prompt = (
        "You are a data-reliability assistant. Given these data-quality signals from a failed batch, "
        "respond with STRICT JSON only: {\"summary\": \"<one sentence>\", \"rule\": {\"name\": \"<short>\", "
        "\"condition\": [{\"metric\": \"<null_rate|range_rate|dup_rate|rescued_rate|volume_ratio|"
        "freshness_min|psi|schema_drift|total>\", \"op\": \"<>,<,>=,<=>\", \"value\": <number>}], "
        "\"diagnosis\": \"<UPPER_SNAKE>\", \"action\": \"<dedup|clamp|quarantine_correct|absorb_schema|"
        "flag_backfill|escalate>\"}}. Signals: " + json.dumps({k: v for k, v in signals.items() if k != "metric_id"})
    )
    esc = prompt.replace("'", "''")
    text = spark.sql(f"SELECT ai_query('{_LLM_ENDPOINT}', '{esc}') AS out").collect()[0]["out"]
    text = text if isinstance(text, str) else str(text)
    return json.loads(text[text.find("{"):text.rfind("}") + 1])


def llm_incident_handler(signals, reason, pid):
    try:
        out = _ask_llm(signals)
    except Exception as ex:
        return f"LLM unavailable ({ex}); escalated to human."
    summary = str(out.get("summary", "")).strip()
    rule = out.get("rule")
    if isinstance(rule, dict) and rule.get("action") in _ALLOWED_ACTIONS and isinstance(rule.get("condition"), list):
        rid = "llm_rule_" + uuid.uuid4().hex[:8]
        spark.createDataFrame(
            [(rid, 100, str(rule.get("name", "llm proposed"))[:80], json.dumps(rule["condition"]),
              str(rule.get("diagnosis", "LLM_PROPOSED"))[:60], rule["action"], "llm", None,
              datetime.now(timezone.utc), False)],
            "rule_id string, priority int, name string, condition string, diagnosis string, action string, "
            "source string, approved_by string, created_at timestamp, active boolean",
        ).write.mode("append").saveAsTable(T("rules"))
        return f"{summary} [proposed rule {rid}, pending approval]"
    return f"{summary} [no valid rule proposed; escalated to human]"


def approve_rule(rule_id, approver="human"):
    spark.sql(f"UPDATE {T('rules')} SET active = true, approved_by = '{approver}' WHERE rule_id = '{rule_id}'")


def reject_rule(rule_id):
    spark.sql(f"DELETE FROM {T('rules')} WHERE rule_id = '{rule_id}' AND active = false")


def run(steps=25):
    setup()
    run_chaos(steps)
    ingest_bronze()
    run_supervisor()
    refresh_gold_safe()
    backfill_late()
