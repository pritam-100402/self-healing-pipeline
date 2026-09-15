from __future__ import annotations
import io, json, warnings
import numpy as np
import pandas as pd

warnings.simplefilter("ignore")

NULL_TOKENS = {"", "na", "n/a", "null", "none", "nan", "-", "--", "?", "unknown", "missing"}

DEFAULT_RULES = [
    {"id": "r_schema", "priority": 10, "issue": "SCHEMA_DRIFT",       "scope": "dataset",
     "metric": "raggedness", "op": ">", "value": 0.0, "action": "align_schema",
     "desc": "Records with inconsistent / missing fields"},
    {"id": "r_dup",    "priority": 20, "issue": "DUPLICATE_ROWS",     "scope": "dataset",
     "metric": "dup_rate", "op": ">", "value": 0.0, "action": "dedup",
     "desc": "Exact duplicate rows (redundant copies)"},
    {"id": "r_type",   "priority": 30, "issue": "TYPE_INCONSISTENCY", "scope": "column",
     "metric": "mixed_rate", "op": ">", "value": 0.0, "action": "coerce",
     "desc": "Values that don't match the column's type"},
    {"id": "r_null",   "priority": 40, "issue": "MISSING_VALUES",     "scope": "column",
     "metric": "null_rate", "op": ">", "value": 0.0, "action": "impute_or_quarantine",
     "desc": "Missing / empty values in a column"},
    {"id": "r_outlier","priority": 50, "issue": "OUTLIERS",           "scope": "column",
     "metric": "outlier_rate", "op": ">", "value": 0.0, "action": "clamp_or_quarantine",
     "desc": "Numeric values far outside the normal range"},
]

_OPS = {">": lambda a, b: a > b, ">=": lambda a, b: a >= b,
        "<": lambda a, b: a < b, "<=": lambda a, b: a <= b, "==": lambda a, b: a == b}

def load_any(data: bytes, filename: str):
    name = (filename or "").lower()
    meta = {"source_type": None, "n_input": 0, "raggedness": 0.0, "ragged_records": 0,
            "union_fields": [], "notes": []}
    text = None
    if name.endswith((".xlsx", ".xls")):
        df = pd.read_excel(io.BytesIO(data))
        meta["source_type"] = "excel"
    else:
        text = data.decode("utf-8-sig", errors="replace") if isinstance(data, (bytes, bytearray)) else str(data)
        stripped = text.strip()
        is_jsonl = name.endswith(".jsonl") or ("\n" in stripped and stripped[0] in "{[" and not stripped.startswith("["))
        if name.endswith(".json") or is_jsonl or stripped[:1] in "[{":
            records = _parse_json_records(stripped, is_jsonl)
            meta["source_type"] = "json"
            df, rag, nrag, union = _records_to_df(records)
            meta.update(raggedness=rag, ragged_records=nrag, union_fields=union)
        else:
            sep = "\t" if name.endswith(".tsv") else None
            df = pd.read_csv(io.StringIO(text), sep=sep, engine="python", dtype=str,
                             keep_default_na=False)
            meta["source_type"] = "csv"
    df = df.reset_index(drop=True)
    df = df.astype(object)
    meta["n_input"] = len(df)
    return df, meta

def _parse_json_records(s: str, is_jsonl: bool):
    if is_jsonl:
        out = []
        for line in s.splitlines():
            line = line.strip()
            if line:
                try: out.append(json.loads(line))
                except Exception: out.append({"_malformed": line})
        return out
    obj = json.loads(s)
    if isinstance(obj, dict):
        for k in ("data", "records", "rows", "items"):
            if isinstance(obj.get(k), list):
                return obj[k]
        return [obj]
    return obj if isinstance(obj, list) else [obj]

def _records_to_df(records):
    records = [r if isinstance(r, dict) else {"value": r} for r in records]
    union = []
    for r in records:
        for k in r.keys():
            if k not in union: union.append(k)
    ragged = sum(1 for r in records if set(r.keys()) != set(union))
    rows = []
    for r in records:
        row = {}
        for k in union:
            v = r.get(k, np.nan)
            if isinstance(v, (dict, list)):
                v = json.dumps(v, ensure_ascii=False)
            row[k] = v
        rows.append(row)
    df = pd.DataFrame(rows, columns=union).astype(object)
    rag_rate = ragged / len(records) if records else 0.0
    return df, rag_rate, ragged, union

def _norm_null(series: pd.Series) -> pd.Series:
    def f(v):
        if v is None or (isinstance(v, float) and np.isnan(v)): return np.nan
        if isinstance(v, str) and v.strip().lower() in NULL_TOKENS: return np.nan
        return v
    return series.map(f)

def _guess_type(non_null: pd.Series):
    s = non_null.astype(str).str.strip()
    if len(s) == 0: return "empty", 1.0
    num = pd.to_numeric(s, errors="coerce").notna().mean()
    dt  = pd.to_datetime(s, errors="coerce", format="mixed").notna().mean()
    bl  = s.str.lower().isin(["true", "false", "yes", "no", "0", "1"]).mean()
    cand = {"numeric": num, "datetime": dt, "boolean": bl}
    best = max(cand, key=cand.get)
    if cand[best] >= 0.6:
        return best, cand[best]
    return "categorical", 1.0

def profile_columns(df: pd.DataFrame) -> dict:
    prof = {}
    n = len(df)
    for col in df.columns:
        raw = df[col]
        s = _norm_null(raw)
        null_rate = float(s.isna().mean()) if n else 0.0
        non_null = s.dropna()
        ctype, fit = _guess_type(non_null)
        mixed_rate = float(1 - fit) if ctype in ("numeric", "datetime", "boolean") else 0.0
        outlier_rate, bounds = 0.0, None
        if ctype == "numeric" and len(non_null) >= 8:
            vals = pd.to_numeric(non_null, errors="coerce").dropna()
            q1, q3 = np.percentile(vals, [25, 75])
            iqr = q3 - q1
            lo, hi = q1 - 3 * iqr, q3 + 3 * iqr
            if iqr > 0:
                outlier_rate = float(((vals < lo) | (vals > hi)).mean())
                bounds = (float(lo), float(hi))
        n_unique = int(non_null.nunique())
        is_key = (null_rate == 0 and n_unique == len(non_null) and n > 1)
        prof[col] = {"type": ctype, "null_rate": round(null_rate, 4),
                     "mixed_rate": round(mixed_rate, 4), "outlier_rate": round(outlier_rate, 4),
                     "n_unique": n_unique, "is_key": is_key, "bounds": bounds}
    return prof

def dataset_signals(df: pd.DataFrame, meta: dict) -> dict:
    n = len(df)
    dup_rate = float(df.astype(str).duplicated().mean()) if n else 0.0
    return {"n_rows": n, "dup_rate": round(dup_rate, 4),
            "raggedness": round(meta.get("raggedness", 0.0), 4)}

def verdict_of(findings):
    if not findings: return "GREEN"
    sev = max((f["severity"] for f in findings), default="low")
    return "RED" if sev == "high" else "AMBER"

def diagnose(col_prof, ds_sig, rules):
    active = [r for r in rules if r.get("active", True)]
    findings = []
    for r in sorted(active, key=lambda x: x["priority"]):
        if r["scope"] == "dataset":
            val = ds_sig.get(r["metric"], 0.0)
            if _OPS[r["op"]](val, r["value"]) and val > 0:
                findings.append(_finding(r, None, val, ds_sig["n_rows"]))
        else:
            for col, p in col_prof.items():
                val = p.get(r["metric"], 0.0)
                if _OPS[r["op"]](val, r["value"]) and val > 0:
                    findings.append(_finding(r, col, val, None))
    return findings

def _finding(rule, col, val, n):
    sev = "high" if val >= 0.5 else ("medium" if val >= 0.15 else "low")
    return {"rule_id": rule["id"], "issue": rule["issue"], "action": rule["action"],
            "column": col, "metric": rule["metric"], "value": round(float(val), 4),
            "severity": sev, "desc": rule["desc"],
            "count": int(round(val * n)) if n else None}

def remediate(df: pd.DataFrame, findings, col_prof, options=None):
    opt = {"null_policy": "quarantine", "outlier_policy": "quarantine"}
    opt.update(options or {})
    work = df.copy().reset_index(drop=True)
    n_input = len(work)
    reason = pd.Series([None] * n_input, index=work.index, dtype=object)
    changes = {"schema_aligned": 0, "duplicates_removed": 0, "cells_coerced": 0,
               "cells_imputed": 0, "cells_clamped": 0, "rows_quarantined": 0, "actions": []}

    for col in work.columns:
        work[col] = _norm_null(work[col])

    for f in findings:
        if f["issue"] == "SCHEMA_DRIFT":
            changes["schema_aligned"] = f["count"] or 0
            changes["actions"].append(f"Aligned {changes['schema_aligned']} ragged record(s) to a common schema")

    if any(f["issue"] == "DUPLICATE_ROWS" for f in findings):
        dup_mask = work.astype(str).duplicated(keep="first")
        changes["duplicates_removed"] = int(dup_mask.sum())
        if changes["duplicates_removed"]:
            changes["actions"].append(f"Removed {changes['duplicates_removed']} duplicate row(s)")
        work = work[~dup_mask].reset_index(drop=True)
        reason = reason[~dup_mask.values] if False else pd.Series([None] * len(work), dtype=object)

    for f in [x for x in findings if x["issue"] == "TYPE_INCONSISTENCY"]:
        col = f["column"]
        if col not in work.columns: continue
        ctype = col_prof.get(col, {}).get("type")
        before = work[col].copy()
        if ctype == "numeric":
            work[col] = pd.to_numeric(work[col], errors="coerce")
        elif ctype == "datetime":
            work[col] = pd.to_datetime(work[col], errors="coerce", format="mixed")
        elif ctype == "boolean":
            work[col] = work[col].map(lambda v: _to_bool(v))
        newly_null = before.notna() & work[col].isna()
        changes["cells_coerced"] += int((before.astype(str) != work[col].astype(str)).sum())
        for i in work.index[newly_null]:
            if reason.iat[i] is None:
                reason.iat[i] = f"{col}: value not parseable as {ctype}"
        if newly_null.any():
            changes["actions"].append(f"Coerced column '{col}' to {ctype}; "
                                      f"{int(newly_null.sum())} unparseable value(s) quarantined")

    changes["tolerated_sparse"] = []
    for f in [x for x in findings if x["issue"] == "MISSING_VALUES"]:
        col = f["column"]
        if col not in work.columns: continue
        null_mask = work[col].isna()
        if not null_mask.any(): continue
        col_null_rate = col_prof.get(col, {}).get("null_rate", 0.0)
        ctype = col_prof.get(col, {}).get("type")
        if col_null_rate >= 0.5:
            changes["tolerated_sparse"].append(col)
            changes["actions"].append(f"Column '{col}' is {int(col_null_rate*100)}% empty — "
                                      f"treated as optional; nulls left as-is")
            continue
        if opt["null_policy"] == "impute" and ctype in ("numeric", "categorical"):
            work[col] = work[col].astype(object)
            if ctype == "numeric":
                col_num = pd.to_numeric(work[col], errors="coerce")
                fill = col_num.median()
                work[col] = col_num.fillna(fill)
            else:
                mode = work[col].dropna().mode()
                fill = mode.iat[0] if len(mode) else "UNKNOWN"
                work.loc[null_mask, col] = fill
            changes["cells_imputed"] += int(null_mask.sum())
            changes["actions"].append(f"Imputed {int(null_mask.sum())} missing value(s) in '{col}' "
                                      f"({'median' if ctype=='numeric' else 'mode'})")
        else:
            for i in work.index[null_mask]:
                if reason.iat[i] is None:
                    reason.iat[i] = f"{col}: missing value"
            changes["actions"].append(f"Quarantined rows with missing '{col}' "
                                      f"({int(null_mask.sum())} row(s))")

    for f in [x for x in findings if x["issue"] == "OUTLIERS"]:
        col = f["column"]
        if col not in work.columns: continue
        bounds = col_prof.get(col, {}).get("bounds")
        if not bounds: continue
        lo, hi = bounds
        num = pd.to_numeric(work[col], errors="coerce")
        out_mask = (num < lo) | (num > hi)
        if not out_mask.any(): continue
        if opt["outlier_policy"] == "clamp":
            work.loc[num < lo, col] = lo
            work.loc[num > hi, col] = hi
            changes["cells_clamped"] += int(out_mask.sum())
            changes["actions"].append(f"Clamped {int(out_mask.sum())} outlier(s) in '{col}' to [{lo:.1f}, {hi:.1f}]")
        else:
            for i in work.index[out_mask.fillna(False)]:
                if reason.iat[i] is None:
                    reason.iat[i] = f"{col}: outlier (outside [{lo:.1f}, {hi:.1f}])"
            changes["actions"].append(f"Quarantined {int(out_mask.sum())} outlier row(s) in '{col}'")

    quar_mask = reason.notna()
    changes["rows_quarantined"] = int(quar_mask.sum())
    silver = work[~quar_mask].reset_index(drop=True)
    dlq = work[quar_mask].copy()
    dlq.insert(0, "_quarantine_reason", reason[quar_mask].values)
    dlq = dlq.reset_index(drop=True)

    stats = {"n_input": n_input, "duplicates_removed": changes["duplicates_removed"],
             "silver_rows": len(silver), "quarantined_rows": len(dlq)}
    stats["unaccounted"] = n_input - (stats["duplicates_removed"] + stats["silver_rows"] + stats["quarantined_rows"])
    return {"silver": silver, "dlq": dlq, "changes": changes, "stats": stats}

def _to_bool(v):
    if isinstance(v, float) and np.isnan(v): return np.nan
    s = str(v).strip().lower()
    if s in ("true", "yes", "1"): return True
    if s in ("false", "no", "0"): return False
    return np.nan

def heal(df: pd.DataFrame, meta: dict, rules=None, options=None) -> dict:
    rules = rules or DEFAULT_RULES
    col_prof = profile_columns(df)
    ds_sig = dataset_signals(df, meta)
    findings = diagnose(col_prof, ds_sig, rules)
    verdict = verdict_of(findings)
    result = remediate(df, findings, col_prof, options)
    tolerated = set(result["changes"].get("tolerated_sparse", []))
    v_prof = profile_columns(result["silver"]) if len(result["silver"]) else {}
    residual = [c for c, p in v_prof.items()
                if p["mixed_rate"] > 0 or p["outlier_rate"] > 0
                or (p["null_rate"] > 0 and c not in tolerated)]
    return {"meta": meta, "col_profile": col_prof, "ds_signals": ds_sig, "verdict": verdict,
            "findings": findings, **result, "verify": {"clean": len(residual) == 0,
            "residual_columns": residual}}

def synthesize(n=60, faults=None):
    rng = np.random.default_rng(7)
    faults = set(faults or [])
    rows = []
    for i in range(n):
        rows.append({"order_id": f"ORD-{1000+i}",
                     "customer": rng.choice(["Asha", "Ravi", "Meera", "John", "Li", "Sam"]),
                     "amount": round(float(rng.normal(500, 120)), 2),
                     "quantity": int(rng.integers(1, 6)),
                     "city": rng.choice(["Pune", "Mumbai", "Delhi", "Chennai"]),
                     "order_date": f"2026-0{rng.integers(1,9)}-{rng.integers(10,28)}"})
    df = pd.DataFrame(rows).astype(object)
    if "MISSING_VALUES" in faults:
        for i in rng.choice(n, size=max(3, n//8), replace=False): df.loc[i, "amount"] = None
        for i in rng.choice(n, size=max(2, n//12), replace=False): df.loc[i, "city"] = "N/A"
    if "TYPE_INCONSISTENCY" in faults:
        for i in rng.choice(n, size=max(2, n//15), replace=False): df.loc[i, "amount"] = "N/A"
        for i in rng.choice(n, size=max(2, n//20), replace=False): df.loc[i, "quantity"] = "two"
    if "OUTLIERS" in faults:
        for i in rng.choice(n, size=max(2, n//20), replace=False): df.loc[i, "amount"] = 999999.0
    if "DUPLICATE_ROWS" in faults:
        df = pd.concat([df, df.iloc[rng.choice(n, size=max(3, n//10), replace=False)]], ignore_index=True)
    if "SCHEMA_DRIFT" in faults:
        recs = df.to_dict("records")
        for r in recs[: max(3, len(recs)//8)]: r.pop("city", None)
        for r in recs[-max(2, len(recs)//10):]: r["discount_code"] = "SAVE10"
        d2, rag, nrag, union = _records_to_df(recs)
        return d2, {"source_type": "json", "n_input": len(d2), "raggedness": rag,
                    "ragged_records": nrag, "union_fields": union, "notes": ["synthetic + faults"]}
    return df, {"source_type": "csv", "n_input": len(df), "raggedness": 0.0,
                "ragged_records": 0, "union_fields": list(df.columns), "notes": ["synthetic + faults"]}

import re as _re
EMAIL_RE = _re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

def propose_rules(df: pd.DataFrame, approved: list) -> list:
    approved_keys = {(r["kind"], r.get("column")) for r in approved}
    n = len(df)
    props = []
    for col in df.columns:
        s = _norm_null(df[col])
        non_null = s.dropna().astype(str).str.strip()
        non_null = non_null[non_null != ""]
        if len(non_null) < 3:
            continue
        email_hit = non_null.str.match(EMAIL_RE).mean()
        if (("email" in col.lower()) or email_hit >= 0.6) and email_hit >= 0.5:
            bad = int((~non_null.str.match(EMAIL_RE)).sum())
            if bad > 0 and ("regex", col) not in approved_keys:
                props.append({"id": f"L_email_{col}", "kind": "regex", "column": col, "priority": 60,
                              "issue": "FORMAT_ANOMALY", "action": "quarantine",
                              "params": {"pattern": EMAIL_RE.pattern, "label": "email format"},
                              "desc": f"'{col}' has {bad} value(s) that aren't valid emails", "affected": bad})
                continue
        uniq_ratio = non_null.nunique() / len(non_null)
        if 0.8 <= uniq_ratio < 1.0 and s.notna().mean() >= 0.9 and ("unique_key", col) not in approved_keys:
            dups = len(non_null) - non_null.nunique()
            props.append({"id": f"L_key_{col}", "kind": "unique_key", "column": col, "priority": 50,
                          "issue": "DUPLICATE_KEY", "action": "quarantine", "params": {},
                          "desc": f"'{col}' looks like a unique ID but has {dups} duplicate value(s)",
                          "affected": int(dups)})
            continue
        num = pd.to_numeric(non_null, errors="coerce")
        if num.notna().mean() >= 0.8:
            neg, pos = int((num < 0).sum()), int((num > 0).sum())
            if neg > 0 and pos >= 3 * neg and ("min_value", col) not in approved_keys:
                props.append({"id": f"L_pos_{col}", "kind": "min_value", "column": col, "priority": 55,
                              "issue": "RANGE_ANOMALY", "action": "quarantine", "params": {"min": 0},
                              "desc": f"'{col}' is mostly positive but has {neg} negative value(s)",
                              "affected": neg})
                continue
        norm = non_null.str.title()
        if norm.nunique() < non_null.nunique() and non_null.nunique() <= max(20, n // 3) \
                and ("normalize_case", col) not in approved_keys:
            changed = int((non_null != norm).sum())
            props.append({"id": f"L_case_{col}", "kind": "normalize_case", "column": col, "priority": 45,
                          "issue": "INCONSISTENT_FORMAT", "action": "normalize", "params": {},
                          "desc": f"'{col}' has the same value in different case/spacing ({changed} row(s))",
                          "affected": changed})
    return props

def apply_learned(df: pd.DataFrame, learned: list) -> dict:
    work = df.copy().reset_index(drop=True)
    reason = pd.Series([None] * len(work), index=work.index, dtype=object)
    changes = []
    for rule in sorted(learned, key=lambda r: r.get("priority", 100)):
        col, kind = rule.get("column"), rule["kind"]
        if col is not None and col not in work.columns:
            continue
        if kind == "regex":
            pat = _re.compile(rule["params"]["pattern"])
            s = _norm_null(work[col]).astype("object")
            bad = s.notna() & ~s.astype(str).str.strip().str.match(pat)
            for i in work.index[bad.fillna(False)]:
                if reason.iat[i] is None:
                    reason.iat[i] = f"{col}: fails {rule['params'].get('label','format')}"
            if bad.fillna(False).any():
                changes.append(f"Quarantined {int(bad.sum())} row(s): '{col}' fails {rule['params'].get('label','format')}")
        elif kind == "min_value":
            mn = rule["params"]["min"]
            num = pd.to_numeric(work[col], errors="coerce")
            bad = num < mn
            if rule.get("action") == "clamp":
                work.loc[bad.fillna(False), col] = mn
                if bad.fillna(False).any():
                    changes.append(f"Clamped {int(bad.sum())} value(s) in '{col}' up to {mn}")
            else:
                for i in work.index[bad.fillna(False)]:
                    if reason.iat[i] is None:
                        reason.iat[i] = f"{col}: below minimum {mn}"
                if bad.fillna(False).any():
                    changes.append(f"Quarantined {int(bad.sum())} row(s): '{col}' < {mn}")
        elif kind == "unique_key":
            dup = work[col].astype(str).duplicated(keep="first")
            for i in work.index[dup]:
                if reason.iat[i] is None:
                    reason.iat[i] = f"{col}: duplicate key"
            if dup.any():
                changes.append(f"Quarantined {int(dup.sum())} row(s): duplicate '{col}'")
        elif kind == "normalize_case":
            before = work[col].astype(str)
            norm = before.str.strip().str.title()
            changed = int((before != norm).sum())
            work[col] = norm
            if changed:
                changes.append(f"Normalized {changed} value(s) in '{col}' (trim + title-case)")
    keep = reason.isna()
    silver = work[keep].reset_index(drop=True)
    dlq = work[~keep].copy()
    if len(dlq):
        dlq.insert(0, "_quarantine_reason", reason[~keep].values)
    return {"silver": silver, "dlq": dlq.reset_index(drop=True), "changes": changes}
