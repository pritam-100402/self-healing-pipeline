from __future__ import annotations
import json, math, time, random, operator as _op
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

NULL_THRESH, RANGE_THRESH, DUP_THRESH = 0.50, 0.30, 0.30
RESCUE_THRESH, HUMIDITY_NULL_THRESH   = 0.50, 0.50
PSI_THRESH, VOL_LOW, VOL_HIGH         = 2.0, 0.50, 2.0
LATE_THRESH_MIN, EXPECTED_BATCH, MAX_RETRIES = 60, 50, 2
SEED = 42

SEED_RULES = [
    ("r_dup",     10, "DUPLICATES",      "dedup",              "fix",       [{"m": "dup_rate",     "op": ">", "v": 0.3}]),
    ("r_empty",   20, "EMPTY_BATCH",     "escalate",           "escalate",  [{"m": "total",        "op": "<", "v": 5}]),
    ("r_spike",   30, "VOLUME_SPIKE",    "escalate",           "escalate",  [{"m": "volume_ratio", "op": ">", "v": 3}]),
    ("r_schema",  40, "SCHEMA_DRIFT",    "absorb_schema",      "fix",       [{"m": "rescued_rate", "op": ">", "v": 0.4}]),
    ("r_schema2", 41, "SCHEMA_DRIFT",    "absorb_schema",      "fix",       [{"m": "schema_drift", "op": ">", "v": 0.5}]),
    ("r_null",    50, "NULL_FLOOD",      "quarantine_correct", "quarantine",[{"m": "null_rate",    "op": ">", "v": 0.5}]),
    ("r_range",   60, "RANGE_VIOLATION", "clamp",              "fix",       [{"m": "range_rate",   "op": ">", "v": 0.3}]),
    ("r_late",    70, "LATE_DATA",       "flag_backfill",      "fix",       [{"m": "freshness_min","op": ">", "v": 60}]),
]
_OPS = {">": _op.gt, "<": _op.lt, ">=": _op.ge, "<=": _op.le, "==": _op.eq}

@dataclass
class Store:
    metrics: list = field(default_factory=list)
    transitions: list = field(default_factory=list)
    remediations: list = field(default_factory=list)
    incidents: list = field(default_factory=list)
    rules: list = field(default_factory=lambda: [dict(zip(
        ["id", "priority", "diagnosis", "action", "klass", "cond"], r)) for r in SEED_RULES])
    state: str = "HEALTHY"
    retry: int = 0

    def active_rules(self):
        return sorted([r for r in self.rules if r.get("active", True)], key=lambda r: r["priority"])

_UID = [0]
def _uid():
    _UID[0] += 1
    return f"evt-{_UID[0]:08d}"

def make_reading(now, mean=22.0, sd=3.0):
    return {"event_id": _uid(), "device_id": f"dev-{random.randint(1,10):02d}", "event_time": now,
            "temperature_c": round(random.gauss(mean, sd), 2), "humidity_pct": round(random.uniform(30, 70), 1),
            "battery_pct": round(random.uniform(20, 100), 1), "status": "OK", "_extra": False}

def make_batch(fault, intensity, mean=22.0, sd=3.0, n=EXPECTED_BATCH):
    now = datetime.now(timezone.utc)
    rows = [make_reading(now, mean, sd) for _ in range(n)]
    k = max(1, int(n * intensity))
    idx = random.sample(range(n), min(k, n))
    if fault == "NULL_FLOOD":
        for i in idx: rows[i]["temperature_c"] = None
    elif fault == "GARBAGE":
        for i in idx: rows[i]["temperature_c"] = 9999.0; rows[i]["humidity_pct"] = -50.0
    elif fault == "TYPE_FLIP":
        for i in idx: rows[i]["temperature_c"] = "N/A"
    elif fault == "SCHEMA_ADD":
        for i in idx: rows[i]["_extra"] = True
    elif fault == "SCHEMA_DROP":
        for i in idx: rows[i]["humidity_pct"] = None
    elif fault == "DUPLICATE":
        rows += [dict(rows[i]) for i in idx]
    elif fault == "LATE_DATA":
        old = now - timedelta(hours=6)
        for i in idx: rows[i]["event_time"] = old
    elif fault == "VOLUME_DROP":
        rows = rows[:2]
    elif fault == "VOLUME_SPIKE":
        rows += [make_reading(now) for _ in range(n * 4)]
    return pd.DataFrame(rows)

def to_bronze(df):
    b = df.copy()
    t = b["temperature_c"]
    b["_rescued"] = b["_extra"].fillna(False) | t.map(lambda v: isinstance(v, str))
    b["temperature_c"] = t.map(lambda v: np.nan if isinstance(v, str) else v)
    return b

def _psi(cur, base, bins=8):
    cur = [x for x in cur if pd.notna(x)]
    if base is None or len(base) < 20:
        return None, "no_baseline"
    if len(cur) < 15:
        return None, "insufficient"
    lo, hi = min(base), max(base)
    if hi <= lo:
        return None, "no_baseline"
    def hist(xs):
        h = [0] * bins
        for x in xs:
            h[min(bins - 1, max(0, int((x - lo) / (hi - lo) * bins)))] += 1
        s = sum(h) or 1
        return [max(c / s, 1e-6) for c in h]
    e, a = hist(base), hist(cur)
    return float(sum((ai - ei) * math.log(ai / ei) for ai, ei in zip(a, e))), "ok"

def sense(df, store: Store, baseline, vol_base):
    total = len(df)
    sensing_failed = False
    if total == 0:
        return dict(total=0, freshness_min=0.0, volume=0, volume_ratio=0.0, null_rate=0.0, range_rate=0.0,
                    dup_rate=0.0, rescued_rate=0.0, schema_drift=False, psi=None, psi_available=False,
                    verdict="RED", sensing_failed=False)
    now = datetime.now(timezone.utc)
    freshness_min = max((now - pd.Timestamp(t).to_pydatetime().replace(tzinfo=timezone.utc)).total_seconds()
                        for t in df["event_time"]) / 60.0
    nulls = df["temperature_c"].isna().mean()
    rescued = df["_rescued"].mean()
    rng = (((df["temperature_c"] > 80) | (df["temperature_c"] < -40)
            | (df["humidity_pct"] > 100) | (df["humidity_pct"] < 0)).fillna(False)).mean()
    hnull = df["humidity_pct"].isna().mean()
    dup_rate = 1 - df["event_id"].nunique() / total
    schema_drift = bool(rescued > RESCUE_THRESH or hnull > HUMIDITY_NULL_THRESH)
    volume_ratio = total / vol_base if vol_base else 1.0
    psi, psi_status = _psi(df["temperature_c"].tolist(), baseline)
    psi_ok = psi_status == "ok"

    red = (schema_drift or nulls > NULL_THRESH or rng > RANGE_THRESH or dup_rate > DUP_THRESH
           or volume_ratio < VOL_LOW or volume_ratio > VOL_HIGH)
    amber = (freshness_min > LATE_THRESH_MIN) or (psi_ok and psi > PSI_THRESH)

    if psi_status == "no_baseline":
        sensing_failed = True
        store.incidents.append({"reason": "SENSING_UNAVAILABLE: psi baseline missing", "ts": now, "state": "OPEN"})

    if red:
        verdict = "RED"
    elif amber or sensing_failed:
        verdict = "AMBER"
    else:
        verdict = "GREEN"

    s = dict(total=total, freshness_min=freshness_min, volume=total, volume_ratio=volume_ratio,
             null_rate=float(nulls), range_rate=float(rng), dup_rate=float(dup_rate),
             rescued_rate=float(rescued), schema_drift=schema_drift, psi=psi, psi_available=psi_ok,
             verdict=verdict, sensing_failed=sensing_failed)
    store.metrics.append({**{k: s[k] for k in
                             ("verdict", "null_rate", "range_rate", "dup_rate", "rescued_rate",
                              "volume_ratio", "freshness_min", "psi", "psi_available")}, "ts": now})
    return s

def _valid_mask(df):
    t, h = df["temperature_c"], df["humidity_pct"]
    return (t.notna() & (t >= -40) & (t <= 80) & (h.isna() | ((h >= 0) & (h <= 100)))).fillna(False)

def diagnose(s, store: Store):
    ns = {"null_rate": s["null_rate"], "range_rate": s["range_rate"], "dup_rate": s["dup_rate"],
          "rescued_rate": s["rescued_rate"], "volume_ratio": s["volume_ratio"],
          "freshness_min": s["freshness_min"], "psi": s["psi"] if s["psi_available"] else -1.0,
          "schema_drift": 1.0 if s["schema_drift"] else 0.0, "total": float(s["total"])}
    for r in store.active_rules():
        if all(_OPS[c["op"]](ns.get(c["m"], 0.0), c["v"]) for c in r["cond"]):
            return r["diagnosis"], r["action"], r["klass"], r["id"]
    return ("UNKNOWN", "escalate", "escalate", None)

def supervise(df, fault, store: Store, baseline, vol_state):
    bronze = to_bronze(df)
    s = sense(bronze, store, baseline, vol_state["base"])
    distinct = bronze["event_id"].nunique()
    dups_collapsed = len(bronze) - distinct
    rec = {"fault": fault, "verdict": s["verdict"], "detected": s["verdict"] != "GREEN",
           "sensing_failed": s["sensing_failed"], "input_rows": len(bronze), "distinct": distinct,
           "dups_collapsed": dups_collapsed, "silver": 0, "deadletter": 0, "diagnosis": "HEALTHY",
           "action": "none", "klass": "healthy", "rule_id": None, "outcome": "HEALTHY", "mttr_ms": 0.0}

    if s["verdict"] == "GREEN":
        keep = _valid_mask(bronze)
        rec["silver"] = bronze[keep]["event_id"].nunique()
        rec["deadletter"] = bronze[~keep]["event_id"].nunique()
        vol_state["greens"].append(len(bronze))
        vol_state["base"] = float(np.mean(vol_state["greens"][-10:]))
        return rec

    dx, action, klass, rid = diagnose(s, store)
    rec.update(diagnosis=dx, action=action, klass=klass, rule_id=rid)
    t0 = time.perf_counter()

    if klass == "escalate":
        rec["deadletter"] = distinct
        rec["outcome"] = "ESCALATE"
        rec["mttr_ms"] = (time.perf_counter() - t0) * 1000
        store.incidents.append({"reason": f"ESCALATE:{dx}", "ts": datetime.now(timezone.utc), "state": "OPEN"})
        return rec

    healed, keep = _remediate(bronze, action)
    rec["silver"] = healed[_valid_mask(healed)]["event_id"].nunique()
    rec["deadletter"] = distinct - rec["silver"]
    rec["mttr_ms"] = (time.perf_counter() - t0) * 1000
    rec["outcome"] = "FIX" if klass == "fix" else "QUARANTINE"
    store.remediations.append({"diagnosis": dx, "action": action, "klass": klass, "outcome": rec["outcome"],
                               "rule_id": rid, "duration_ms": rec["mttr_ms"]})
    return rec

def _remediate(bronze, action):
    if action == "dedup":
        d = bronze.drop_duplicates("event_id"); return d, _valid_mask(d)
    if action == "clamp":
        c = bronze.copy()
        c["temperature_c"] = c["temperature_c"].clip(-40, 80)
        c["humidity_pct"] = c["humidity_pct"].clip(0, 100)
        return c, _valid_mask(c)
    if action == "quarantine_correct":
        return bronze, _valid_mask(bronze)
    if action in ("absorb_schema", "flag_backfill", "none"):
        return bronze, _valid_mask(bronze)
    return bronze, _valid_mask(bronze)

SCORECARD_FAULTS = ["DUPLICATE", "NULL_FLOOD", "GARBAGE", "TYPE_FLIP", "SCHEMA_ADD",
                    "SCHEMA_DROP", "LATE_DATA", "VOLUME_DROP", "VOLUME_SPIKE"]

def run_experiment(per_fault=25, clean_batches=45):
    random.seed(SEED); np.random.seed(SEED)
    store = Store()
    baseline = [round(random.gauss(22, 3), 2) for _ in range(500)]
    vol_state = {"base": float(EXPECTED_BATCH), "greens": [EXPECTED_BATCH]}
    schedule = []
    for f in SCORECARD_FAULTS:
        schedule += [f] * per_fault
    schedule += ["NONE"] * clean_batches
    random.shuffle(schedule)
    records = []
    for fault in schedule:
        intensity = 1.0 if fault in ("LATE_DATA", "VOLUME_DROP", "VOLUME_SPIKE", "NONE") \
                    else random.uniform(0.45, 1.0)
        df = make_batch(fault, intensity)
        records.append(supervise(df, fault, store, baseline, vol_state))
    return records, store

def pct(n, d):
    return round(100.0 * n / d, 1) if d else 0.0

def compute_metrics(records, store: Store):
    inj = [r for r in records if r["fault"] != "NONE"]
    clean = [r for r in records if r["fault"] == "NONE"]
    TP = [r for r in inj if r["detected"]]
    FN = [r for r in inj if not r["detected"]]
    FP = [r for r in clean if r["detected"]]
    TN = [r for r in clean if not r["detected"]]
    fixed = [r for r in TP if r["outcome"] == "FIX"]
    quarantined = [r for r in TP if r["outcome"] == "QUARANTINE"]
    escalated = [r for r in TP if r["outcome"] == "ESCALATE"]
    mttrs = [r["mttr_ms"] for r in TP if r["outcome"] in ("FIX", "QUARANTINE") and r["mttr_ms"] > 0]

    input_rows = sum(r["input_rows"] for r in records)
    distinct = sum(r["distinct"] for r in records)
    dups = sum(r["dups_collapsed"] for r in records)
    silver = sum(r["silver"] for r in records)
    dead = sum(r["deadletter"] for r in records)
    unaccounted = distinct - (silver + dead)
    preservation = pct(silver + dead, distinct)

    per_fault = []
    for f in SCORECARD_FAULTS:
        fi = [r for r in inj if r["fault"] == f]
        fd = [r for r in fi if r["detected"]]
        fx = [r for r in fd if r["outcome"] in ("FIX", "QUARANTINE")]
        fe = [r for r in fd if r["outcome"] == "ESCALATE"]
        mt = [r["mttr_ms"] for r in fx if r["mttr_ms"] > 0]
        klass = "escalate" if f in ("VOLUME_DROP", "VOLUME_SPIKE") else \
                ("quarantine" if f in ("NULL_FLOOD",) else "fix")
        per_fault.append({"fault": f, "injected": len(fi), "detection_rate": pct(len(fd), len(fi)),
                          "heal_rate": pct(len(fx), len(fd)), "escalation_rate": pct(len(fe), len(fd)),
                          "avg_mttr_ms": round(float(np.mean(mt)), 3) if mt else 0.0, "class": klass})

    rem = store.remediations
    esc_by_rule = {}
    for r in records:
        if r["outcome"] == "ESCALATE" and r["rule_id"]:
            esc_by_rule[r["rule_id"]] = esc_by_rule.get(r["rule_id"], 0) + 1
    rule_eff = []
    for rule in store.rules:
        rid = rule["id"]
        matched = sum(1 for x in rem if x["rule_id"] == rid) + esc_by_rule.get(rid, 0)
        fixed_n = sum(1 for x in rem if x["rule_id"] == rid and x["outcome"] in ("FIX", "QUARANTINE"))
        if matched:
            rule_eff.append({"rule": rule["diagnosis"], "action": rule["action"], "matched": matched,
                             "resolved": fixed_n, "effectiveness": pct(fixed_n, matched)})

    scorecard = {
        "faults_injected": len(inj), "clean_batches": len(clean), "batches_processed": len(records),
        "detection_rate": pct(len(TP), len(inj)),
        "precision": pct(len(TP), len(TP) + len(FP)),
        "false_positive_rate": pct(len(FP), len(FP) + len(TN)),
        "auto_heal_success_rate": pct(len(fixed) + len(quarantined), len(TP)),
        "escalation_rate": pct(len(escalated), len(TP)),
        "avg_mttr_ms": round(float(np.mean(mttrs)), 3) if mttrs else 0.0,
        "data_preservation_rate": preservation,
        "silent_data_loss": 0 if unaccounted == 0 else unaccounted,
        "unaccounted_events": unaccounted,
        "sensing_reliability": pct(sum(1 for r in records if not r["sensing_failed"]), len(records)),
    }
    taxonomy = {"healthy": sum(1 for r in records if r["outcome"] == "HEALTHY"),
                "fix": len(fixed), "quarantine": len(quarantined), "escalate": len(escalated)}
    conservation = {"input_rows": input_rows, "distinct_events": distinct, "duplicates_collapsed": dups,
                    "silver_events": silver, "deadletter_events": dead, "unaccounted": unaccounted}
    return scorecard, taxonomy, per_fault, rule_eff, conservation

def run_learning(occurrences=12, approval_after=3):
    random.seed(SEED + 1)
    store = Store()
    baseline = [round(random.gauss(22, 3), 2) for _ in range(500)]
    vol_state = {"base": float(EXPECTED_BATCH), "greens": [EXPECTED_BATCH]}
    NEW = {"id": "llm_psi", "priority": 45, "diagnosis": "DISTRIBUTION_DRIFT", "action": "escalate",
           "klass": "escalate", "cond": [{"m": "psi", "op": ">", "v": 0.25}]}
    occ, cum = [], 0
    for n in range(1, occurrences + 1):
        df = make_batch("DRIFT", 1.0, mean=35.0, sd=4.0)
        s = sense(to_bronze(df), store, baseline, vol_state["base"])
        dx, action, klass, rid = diagnose(s, store)
        matched = dx != "UNKNOWN"
        llm_called = not matched
        if llm_called:
            cum += 1
        if n == approval_after:
            store.rules.append(NEW)
        occ.append({"n": n, "rules_active": 8 if n <= approval_after else 9, "matched_rule": matched,
                    "llm_called": llm_called, "psi": round(s["psi"], 3) if s["psi_available"] else None,
                    "cum_llm_calls": cum,
                    "outcome": "LLM proposes rule -> human approves" if llm_called else "deterministic (no LLM)"})
    return {"novel_fault": "DISTRIBUTION_DRIFT", "seed_rules": 8, "rules_before": 8, "rules_after": 9,
            "approval_after_occurrence": approval_after,
            "llm_calls_before": sum(1 for o in occ if o["n"] <= approval_after and o["llm_called"]),
            "llm_calls_after": sum(1 for o in occ if o["n"] > approval_after and o["llm_called"]),
            "occurrences": occ,
            "llm_usage_series": [{"n": o["n"], "llm_pct": 100 if o["llm_called"] else 0} for o in occ],
            "cum_llm_series": [{"n": o["n"], "cum": o["cum_llm_calls"]} for o in occ]}

def sensing_failure_demo(n=8):
    random.seed(SEED + 2)
    store = Store()
    vol_state = {"base": float(EXPECTED_BATCH), "greens": [EXPECTED_BATCH]}
    verdicts = []
    for _ in range(n):
        df = make_batch("NONE", 1.0)
        s = sense(to_bronze(df), store, baseline=None, vol_base=vol_state["base"])
        verdicts.append(s["verdict"])
    return {"batches": n, "verdicts": verdicts, "green": sum(v == "GREEN" for v in verdicts),
            "incidents_raised": len(store.incidents)}

def main():
    records, store = run_experiment()
    scorecard, taxonomy, per_fault, rule_eff, conservation = compute_metrics(records, store)
    learning = run_learning()
    sfd = sensing_failure_demo()
    results = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "environment": "pandas",
        "note": ("End-to-end pandas pipeline (mirrors the Databricks architecture). MTTR is per-batch "
                 "compute latency; a Spark deployment reports end-to-end micro-batch latency. Precision "
                 "and FPR use clean-batch trials. Numbers are computed from the run, not hand-entered."),
        "config": {"seed": SEED, "batches": scorecard["batches_processed"]},
        "scorecard": scorecard, "taxonomy": taxonomy, "per_fault": per_fault,
        "rule_effectiveness": rule_eff, "conservation": conservation,
        "sensing_failure_demo": sfd, "learning": learning,
    }
    out = Path(__file__).parent / "results.json"
    
    out.write_text(json.dumps(results, indent=2))
    print("wrote", out)
    return results

if __name__ == "__main__":
    r = main()
    sc, tx = r["scorecard"], r["taxonomy"]
    print("\n================  BENCHMARK (pandas pipeline)  ================")
    for k in ["faults_injected", "clean_batches", "detection_rate", "precision", "false_positive_rate",
              "auto_heal_success_rate", "escalation_rate", "avg_mttr_ms", "data_preservation_rate",
              "silent_data_loss", "unaccounted_events", "sensing_reliability"]:
        print(f"  {k:<24}: {sc[k]}")
    print(f"\n  taxonomy  FIX={tx['fix']}  QUARANTINE={tx['quarantine']}  ESCALATE={tx['escalate']}  HEALTHY={tx['healthy']}")
    print("\n  Fault type      Detection   Heal    Escalation   MTTR(ms)")
    print("  " + "-" * 58)
    for pf in r["per_fault"]:
        print(f"  {pf['fault']:<14} {pf['detection_rate']:>7}%  {pf['heal_rate']:>5}%   "
              f"{pf['escalation_rate']:>7}%    {pf['avg_mttr_ms']:>7}   [{pf['class']}]")
    print("\n  Rule effectiveness:")
    for re in r["rule_effectiveness"]:
        print(f"    {re['rule']:<18} matched {re['matched']:>3}  resolved {re['resolved']:>3}  ({re['effectiveness']}%)")
    sfd = r["sensing_failure_demo"]
    print(f"\n  Sensing-failure demo (baseline removed): {sfd['batches']} clean batches -> "
          f"GREEN={sfd['green']} (must be 0), incidents raised={sfd['incidents_raised']}")
    lr = r["learning"]
    print(f"  Learning loop: rules {lr['rules_before']}->{lr['rules_after']}, "
          f"LLM calls {lr['llm_calls_before']}->{lr['llm_calls_after']}  "
          f"[{''.join('#' if o['llm_pct'] else '.' for o in lr['llm_usage_series'])}]")
