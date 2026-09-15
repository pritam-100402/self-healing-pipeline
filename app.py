import json
from pathlib import Path

import pandas as pd
import streamlit as st

import healing_engine as HE

st.set_page_config(page_title="Self-Healing Data Pipeline", layout="wide",
                   initial_sidebar_state="expanded")

st.markdown("""
<style>
  .stApp { background:#0b0f14; }
  h1,h2,h3,h4 { color:#e8eef5 !important; letter-spacing:-0.01em; }
  .card { background:linear-gradient(160deg,#131a22,#0e141b); border:1px solid #1e2a36;
          border-radius:14px; padding:16px 18px; }
  .lbl { color:#7d8ea0; font-size:0.74rem; text-transform:uppercase; letter-spacing:0.08em; }
  .val { color:#e8eef5; font-size:1.9rem; font-weight:700; line-height:1.1; margin-top:4px; }
  .good{color:#35d0a5!important;} .warn{color:#f5b544!important;} .bad{color:#e0556b!important;}
  .badge{display:inline-block;padding:6px 16px;border-radius:999px;font-weight:700;font-size:1rem;}
  .b-green{background:#0f2a20;color:#35d0a5;border:1px solid #1c5040;}
  .b-amber{background:#2e2410;color:#f5b544;border:1px solid #5a4718;}
  .b-red{background:#2e1218;color:#e0556b;border:1px solid #5a1f2b;}
  .pill{display:inline-block;padding:3px 10px;border-radius:999px;font-size:0.72rem;
        background:#12202b;color:#57b6ff;border:1px solid #1d3a4d;margin-right:6px;}
  .flow{background:#0e141b;border:1px solid #1e2a36;border-radius:12px;padding:12px 16px;
        color:#c6d2df;font-size:0.9rem;}
  .arrow{color:#41566b;padding:0 6px;}
</style>
""", unsafe_allow_html=True)

def kpi(col, label, value, cls="", sub=""):
    col.markdown(f'<div class="card"><div class="lbl">{label}</div>'
                 f'<div class="val {cls}">{value}</div>'
                 f'<div class="lbl" style="text-transform:none;color:#5f7080">{sub}</div></div>',
                 unsafe_allow_html=True)

SAMPLE_CSV = """order_id,customer,amount,quantity,city,order_date
ORD-1,Asha,500.00,2,Pune,2026-01-15
ORD-2,Ravi,N/A,1,Mumbai,2026-02-10
ORD-1,Asha,500.00,2,Pune,2026-01-15
ORD-3,Meera,twelve,3,,2026-03-05
ORD-4,John,999999.0,2,Delhi,2026-04-01
ORD-5,Li,450.00,,Chennai,2026-05-20
ORD-6,Sam,480.00,4,Pune,not-a-date
ORD-7,Asha,,2,Mumbai,2026-06-11
ORD-8,Ravi,510.25,1,delhi,2026-07-19
ORD-9,Meera,495.00,3,pune,2026-08-02"""

SAMPLE_JSONL = "\n".join(json.dumps(r) for r in [
    {"id": "U1", "name": "Asha", "email": "a@x.com", "age": 29},
    {"id": "U2", "name": "Ravi", "age": 34},
    {"id": "U3", "name": "Meera", "email": "m@x.com", "age": "N/A", "phone": "999"},
    {"id": "U4", "name": "John", "email": "j@x.com", "age": 41, "tags": ["vip", "new"]},
    {"id": "U1", "name": "Asha", "email": "a@x.com", "age": 29},
    {"id": "U5", "name": "Li", "email": "l@x.com", "age": 37},
    {"id": "U6", "name": "Sam", "email": "bad-email", "age": 45},
])

def _is_nullish(v):
    if v is None: return True
    if isinstance(v, float) and pd.isna(v): return True
    return isinstance(v, str) and v.strip().lower() in HE.NULL_TOKENS

def highlight_problems(df: pd.DataFrame, prof: dict):
    def style_col(col):
        p = prof.get(col.name, {})
        bounds = p.get("bounds")
        out = []
        for v in col:
            s = ""
            if _is_nullish(v):
                s = "background-color:#3a2f10;color:#f5b544"
            elif p.get("type") == "numeric":
                num = pd.to_numeric(pd.Series([v]), errors="coerce").iloc[0]
                if pd.isna(num):
                    s = "background-color:#2a1030;color:#c07de0"
                elif bounds and (num < bounds[0] or num > bounds[1]):
                    s = "background-color:#3a1720;color:#e0556b"
            elif p.get("type") == "datetime":
                dt = pd.to_datetime(pd.Series([v]), errors="coerce", format="mixed").iloc[0]
                if pd.isna(dt):
                    s = "background-color:#2a1030;color:#c07de0"
            out.append(s)
        return out
    return df.style.apply(style_col, axis=0)

def live_healing():
    st.sidebar.markdown("### Healing options")
    null_policy = st.sidebar.radio("Missing values", ["quarantine", "impute"], index=0,
                                   help="Quarantine preserves rows untouched; impute fills numeric "
                                        "with median / categorical with mode.")
    outlier_policy = st.sidebar.radio("Outliers", ["quarantine", "clamp"], index=0,
                                      help="Quarantine preserves suspect rows; clamp winsorises.")
    st.sidebar.markdown("### Active rules")
    rules = []
    for r in HE.DEFAULT_RULES:
        on = st.sidebar.checkbox(f'{r["issue"]}', value=True, key=f'rule_{r["id"]}', help=r["desc"])
        rr = dict(r); rr["active"] = on; rules.append(rr)

    st.markdown("## Live self-healing")
    st.caption("Drop in any tabular file — structured (CSV/TSV/Excel) or semi-structured "
               "(JSON/JSONL with varying fields). The engine profiles it, flags problems, repairs "
               "what's safe, quarantines what isn't, and proves nothing is lost.")

    mode = st.radio("Data source", ["Upload a file", "Generate demo data", "Sample: messy CSV",
                                    "Sample: ragged JSON"], horizontal=True)

    df = meta = None
    if mode == "Upload a file":
        up = st.file_uploader("CSV, TSV, JSON, JSONL, or Excel",
                              type=["csv", "tsv", "json", "jsonl", "xlsx", "xls"])
        if up:
            df, meta = HE.load_any(up.getvalue(), up.name)
    elif mode == "Generate demo data":
        cols = st.columns(5)
        picks = []
        for c, (lbl, key) in zip(cols, [("Missing", "MISSING_VALUES"), ("Bad types", "TYPE_INCONSISTENCY"),
                                        ("Outliers", "OUTLIERS"), ("Duplicates", "DUPLICATE_ROWS"),
                                        ("Schema drift", "SCHEMA_DRIFT")]):
            if c.checkbox(lbl, value=True, key=f"gen_{key}"): picks.append(key)
        n = st.slider("Rows", 30, 300, 80, 10)
        if st.button("Generate", type="primary"):
            st.session_state["gen"] = HE.synthesize(n, faults=picks)
        if "gen" in st.session_state:
            df, meta = st.session_state["gen"]
    elif mode == "Sample: messy CSV":
        df, meta = HE.load_any(SAMPLE_CSV.encode(), "orders.csv")
    else:
        df, meta = HE.load_any(SAMPLE_JSONL.encode(), "users.jsonl")

    if df is None:
        st.info("Choose a data source above to begin.")
        return

    rep = HE.heal(df, meta, rules=rules,
                  options={"null_policy": null_policy, "outlier_policy": outlier_policy})
    prof, sig, stats = rep["col_profile"], rep["ds_signals"], dict(rep["stats"])

    learned = st.session_state.setdefault("learned_rules", [])
    la = HE.apply_learned(rep["silver"], learned)
    final_silver = la["silver"]
    combined_dlq = pd.concat([rep["dlq"], la["dlq"]], ignore_index=True) if len(la["dlq"]) else rep["dlq"]
    learned_changes = la["changes"]
    stats["silver_rows"] = len(final_silver)
    stats["quarantined_rows"] = len(combined_dlq)
    stats["unaccounted"] = stats["n_input"] - (stats["duplicates_removed"]
                                               + stats["silver_rows"] + stats["quarantined_rows"])

    st.markdown("### 1 · Ingested")
    c = st.columns(4)
    kpi(c[0], "Rows", meta["n_input"])
    kpi(c[1], "Columns", len(df.columns))
    kpi(c[2], "Source", meta["source_type"].upper())
    kpi(c[3], "Schema drift", f'{int(sig["raggedness"]*100)}%',
        "warn" if sig["raggedness"] > 0 else "good",
        "records with inconsistent fields" if sig["raggedness"] > 0 else "consistent")
    with st.expander("Detected schema & types"):
        st.dataframe(pd.DataFrame([{"column": k, **{kk: vv for kk, vv in v.items() if kk != "bounds"}}
                                   for k, v in prof.items()]), use_container_width=True, hide_index=True)

    st.markdown("### 2 · Health scan")
    v = rep["verdict"]
    bcls = {"GREEN": "b-green", "AMBER": "b-amber", "RED": "b-red"}[v]
    st.markdown(f'<span class="badge {bcls}">{v}</span>', unsafe_allow_html=True)
    if rep["findings"]:
        st.dataframe(pd.DataFrame([{"issue": f["issue"], "column": f["column"] or "—",
                                    "affected": f'{f["value"]*100:.0f}%', "severity": f["severity"],
                                    "action": f["action"], "what": f["desc"]} for f in rep["findings"]]),
                     use_container_width=True, hide_index=True)
    else:
        st.success("No problems detected — data is clean.")
    st.markdown("**Problem cells** "
                '<span class="pill">missing</span><span class="pill">type mismatch</span>'
                '<span class="pill">outlier</span>', unsafe_allow_html=True)
    st.dataframe(highlight_problems(df.head(200), prof), use_container_width=True, height=280)

    st.markdown("### 3 · Remediation")
    ch = rep["changes"]
    c = st.columns(5)
    kpi(c[0], "Duplicates removed", ch["duplicates_removed"])
    kpi(c[1], "Cells coerced", ch["cells_coerced"])
    kpi(c[2], "Cells imputed", ch["cells_imputed"])
    kpi(c[3], "Cells clamped", ch["cells_clamped"])
    kpi(c[4], "Rows quarantined", stats["quarantined_rows"],
        "warn" if stats["quarantined_rows"] else "good")
    all_actions = ch["actions"] + [f"(learned rule) {a}" for a in learned_changes]
    if all_actions:
        st.markdown('<div class="flow">' + "<br>".join(f"• {a}" for a in all_actions) + "</div>",
                    unsafe_allow_html=True)

    st.markdown("### 4 · Output")
    t_clean, t_quar = st.tabs([f'✅ Clean ({stats["silver_rows"]})',
                               f'⚠️ Quarantined ({stats["quarantined_rows"]})'])
    with t_clean:
        st.dataframe(final_silver.head(300), use_container_width=True, height=300)
        st.download_button("Download clean data (CSV)", final_silver.to_csv(index=False),
                           "clean.csv", "text/csv")
    with t_quar:
        if stats["quarantined_rows"]:
            st.dataframe(combined_dlq.head(300), use_container_width=True, height=300)
            st.download_button("Download quarantined data (CSV)", combined_dlq.to_csv(index=False),
                               "quarantined.csv", "text/csv")
            st.caption("Every quarantined row carries the reason it couldn't be auto-fixed — "
                       "preserved and reprocessable, never discarded.")
        else:
            st.success("Nothing quarantined.")

    st.markdown("### 5 · Conservation — proof nothing is silently dropped")
    c = st.columns(5)
    kpi(c[0], "Input rows", stats["n_input"])
    kpi(c[1], "Duplicates collapsed", stats["duplicates_removed"], sub="redundant copies")
    kpi(c[2], "→ Clean", stats["silver_rows"], "good")
    kpi(c[3], "→ Quarantined", stats["quarantined_rows"], "good", "with a reason")
    kpi(c[4], "Unaccounted", stats["unaccounted"], "good" if stats["unaccounted"] == 0 else "bad")
    st.markdown(f'<div class="flow">input = duplicates + clean + quarantined → '
                f'{stats["n_input"]} = {stats["duplicates_removed"]} + {stats["silver_rows"]} + '
                f'{stats["quarantined_rows"]}. Unaccounted rows: '
                f'<b class="good">{stats["unaccounted"]}</b>. Verify (clean output re-scanned): '
                f'{"<b class=good>passes</b>" if rep["verify"]["clean"] else "residual issues"}.'
                f'</div>', unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("### 6 · Learning loop — approve rules the engine doesn't yet have")
    st.caption("These are UNKNOWN anomalies: patterns the base rules passed over. In production a "
               "novel failure like this escalates to the LLM, which proposes a rule a human approves. "
               "Approve one here and watch it join the engine and re-heal the data — no LLM needed "
               "the next time it appears.")

    proposals = HE.propose_rules(rep["silver"], learned)

    if st.session_state.get("just_approved"):
        st.success(f'Rule added to the engine: {st.session_state.pop("just_approved")}. '
                   f'Data re-healed with it — see the updated Output and Conservation above.')

    if proposals:
        st.markdown(f'**{len(proposals)} proposed rule(s)** awaiting your approval:')
        for p in proposals:
            b = st.container()
            cc = b.columns([0.62, 0.18, 0.20])
            cc[0].markdown(f'<div class="card"><b>{p["issue"]}</b> — {p["desc"]}<br>'
                           f'<span class="lbl" style="text-transform:none">column: {p["column"]} · '
                           f'action: {p["action"]} · affects {p["affected"]} row(s)</span></div>',
                           unsafe_allow_html=True)
            if cc[1].button("✓ Approve", key=f'appr_{p["id"]}', type="primary"):
                st.session_state["learned_rules"].append(
                    {k: p[k] for k in ("id", "kind", "column", "priority", "issue", "action", "params")})
                st.session_state["just_approved"] = f'{p["issue"]} on "{p["column"]}"'
                st.rerun()
            if cc[2].button("✕ Reject", key=f'rej_{p["id"]}'):
                st.session_state.setdefault("rejected", []).append(p["id"])
    else:
        st.success("No unknown anomalies outstanding — every detected pattern is covered by a rule.")

    st.markdown("#### Active rule set")
    base_view = [{"source": "base", "issue": r["issue"], "column": "—", "action": r["action"],
                  "active": r["active"]} for r in rules]
    learned_view = [{"source": "learned ✎", "issue": r["issue"], "column": r.get("column", "—"),
                     "action": r["action"], "active": True} for r in st.session_state["learned_rules"]]
    st.dataframe(pd.DataFrame(base_view + learned_view), use_container_width=True, hide_index=True)
    if st.session_state["learned_rules"]:
        cc = st.columns([0.25, 0.75])
        if cc[0].button("Reset approved rules"):
            st.session_state["learned_rules"] = []
            st.rerun()
        cc[1].caption(f'{len(st.session_state["learned_rules"])} learned rule(s) now part of the '
                      f'engine — they run deterministically on every batch, no LLM.')

    st.markdown('<div class="flow"><b>Same engine as Databricks.</b> This is the identical '
                'sense → diagnose → remediate → verify loop and rules-as-data design from the pipeline, '
                'in pure Python so you can watch — and now drive — it. Each rule you approve shrinks the '
                'unknown-failure space, exactly like the benchmark\'s LLM-usage-to-zero demonstration.</div>',
                unsafe_allow_html=True)

def benchmark():
    import plotly.graph_objects as go
    here = Path(__file__).parent
    up = st.sidebar.file_uploader("Load results.json (your pipeline export)", type="json", key="bench")
    if up:
        R = json.load(up)
    else:
        p = here / "results.json"
        if not p.exists():
            st.info("Run ../pipeline_pandas.py to generate results.json.")
            return
        R = json.loads(p.read_text())

    sc, pf, cons, lr = R["scorecard"], R["per_fault"], R["conservation"], R["learning"]
    tx = R.get("taxonomy", {})
    st.markdown("## Benchmark scorecard")
    st.markdown(f'<span class="pill">environment: {R.get("environment")}</span>'
                f'<span class="pill">{sc["batches_processed"]} batches</span>'
                f'<span class="pill">{sc.get("faults_injected","?")} faults + '
                f'{sc.get("clean_batches","?")} clean</span>', unsafe_allow_html=True)
    st.caption(R.get("note", ""))

    c = st.columns(4)
    kpi(c[0], "Detection rate", f'{sc["detection_rate"]}%', "good", "recall on injected faults")
    kpi(c[1], "Precision", f'{sc.get("precision","—")}%', "good", "flagged that were real")
    kpi(c[2], "False-positive rate", f'{sc.get("false_positive_rate","—")}%',
        "good" if sc.get("false_positive_rate", 0) < 5 else "warn", "clean batches misflagged")
    kpi(c[3], "Auto-heal success", f'{sc.get("auto_heal_success_rate", sc.get("auto_recovery_rate"))}%',
        "good", "of detected faults")
    c = st.columns(4)
    kpi(c[0], "Escalation rate", f'{sc["escalation_rate"]}%', "warn", "routed to human, by design")
    kpi(c[1], "Data preservation", f'{sc.get("data_preservation_rate","—")}%', "good", "silent loss = 0")
    kpi(c[2], "Sensing reliability", f'{sc.get("sensing_reliability","—")}%', "good",
        "batches fully measured")
    kpi(c[3], "Avg MTTR", f'{sc.get("avg_mttr_ms",0)} ms', sub="compute (pandas ref)")

    if tx:
        st.markdown("### Outcome taxonomy — a mature system doesn't heal everything")
        c = st.columns([1, 1.4])
        with c[0]:
            fig = go.Figure(go.Bar(
                x=[tx.get("fix", 0), tx.get("quarantine", 0), tx.get("escalate", 0)],
                y=["FIX  (known + safe)", "QUARANTINE  (known + unsafe)", "ESCALATE  (unknown / ops)"],
                orientation="h", marker_color=["#35d0a5", "#f5b544", "#e0556b"],
                text=[tx.get("fix", 0), tx.get("quarantine", 0), tx.get("escalate", 0)], textposition="outside"))
            fig.update_layout(template="plotly_dark", height=230, margin=dict(l=10, r=30, t=10, b=10),
                              paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="#c6d2df",
                              xaxis_title="batches")
            st.plotly_chart(fig, use_container_width=True)
        c[1].markdown('<div class="flow"><b>FIX</b> — auto-corrected and verified (dedup, clamp, coerce, '
                      'absorb schema, backfill).<br><b>QUARANTINE</b> — known but unsafe to auto-correct '
                      '(e.g. null floods): good rows kept, bad rows preserved to dead-letter.<br>'
                      '<b>ESCALATE</b> — unknown signatures or operational anomalies (volume drop/spike) '
                      'routed to a human. Escalation is correct behaviour, not failure.</div>',
                      unsafe_allow_html=True)

    st.markdown("### Per-fault: detection · heal · escalation · MTTR")
    dfp = pd.DataFrame(pf)
    fig = go.Figure()
    fig.add_bar(name="Detection %", x=dfp["fault"], y=dfp["detection_rate"], marker_color="#57b6ff")
    heal_col = "heal_rate" if "heal_rate" in dfp.columns else "recovery_rate"
    fig.add_bar(name="Heal %", x=dfp["fault"], y=dfp[heal_col], marker_color="#35d0a5")
    if "escalation_rate" in dfp.columns:
        fig.add_bar(name="Escalation %", x=dfp["fault"], y=dfp["escalation_rate"], marker_color="#e0556b")
    fig.update_layout(template="plotly_dark", height=340, barmode="group",
                      paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                      font_color="#c6d2df", legend=dict(orientation="h", y=1.12),
                      margin=dict(l=10, r=10, t=30, b=80))
    fig.update_xaxes(tickangle=-40)
    st.plotly_chart(fig, use_container_width=True)
    show_cols = [c for c in ["fault", "injected", "detection_rate", heal_col, "escalation_rate",
                             "avg_mttr_ms", "class"] if c in dfp.columns]
    st.dataframe(dfp[show_cols], use_container_width=True, hide_index=True)

    cols = st.columns(2)
    if R.get("rule_effectiveness"):
        cols[0].markdown("#### Rule effectiveness")
        cols[0].dataframe(pd.DataFrame(R["rule_effectiveness"]), use_container_width=True, hide_index=True)
    sfd = R.get("sensing_failure_demo")
    if sfd:
        cols[1].markdown("#### Sensing-failure safeguard")
        cols[1].markdown(
            f'<div class="flow">Baseline removed → drift unmeasurable. Across {sfd["batches"]} clean '
            f'batches the system reported <b class="good">{sfd["green"]}</b> GREEN (must be 0) and raised '
            f'<b>{sfd["incidents_raised"]}</b> sensing incidents.<br>'
            f'<b>System failure is never converted into "healthy."</b></div>', unsafe_allow_html=True)

    st.markdown("### The learning loop — why the LLM exists")
    st.markdown(f'<div class="flow">The deterministic engine handles known failure modes. The LLM is used '
                f'<b>only for unknown signatures</b> (<b>{lr["novel_fault"]}</b>) → it proposes a structured '
                f'rule → a <b>human approves</b> → the rule joins the deterministic system. The LLM never '
                f'modifies data or executes code; its job is to shrink the unknown-failure space.</div>',
                unsafe_allow_html=True)
    c = st.columns(3)
    kpi(c[0], "Known rules", f'{lr["rules_before"]} → {lr["rules_after"]}', "good")
    kpi(c[1], "LLM calls before", lr["llm_calls_before"], "warn")
    kpi(c[2], "LLM calls after", lr["llm_calls_after"], "good", "now deterministic")

    g1, g2 = st.columns(2)
    us = pd.DataFrame(lr["llm_usage_series"]); cm = pd.DataFrame(lr["cum_llm_series"])
    with g1:
        fig = go.Figure(go.Scatter(x=us["n"], y=us["llm_pct"], line_shape="hv",
                                   line=dict(color="#57b6ff", width=3), fill="tozeroy",
                                   fillcolor="rgba(87,182,255,0.12)"))
        fig.add_vline(x=lr["approval_after_occurrence"] + 0.5, line_dash="dash", line_color="#35d0a5")
        fig.update_layout(template="plotly_dark", height=280, title="LLM usage per occurrence",
                          paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                          font_color="#c6d2df", yaxis=dict(range=[-5, 110], title="% via LLM"),
                          xaxis_title="occurrence #", margin=dict(l=10, r=10, t=40, b=10))
        st.plotly_chart(fig, use_container_width=True)
    with g2:
        fig = go.Figure(go.Scatter(x=cm["n"], y=cm["cum"], line=dict(color="#35d0a5", width=3),
                                   mode="lines+markers"))
        fig.update_layout(template="plotly_dark", height=280, title="Cumulative LLM calls (flattens)",
                          paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                          font_color="#c6d2df", yaxis_title="total calls", xaxis_title="occurrence #",
                          margin=dict(l=10, r=10, t=40, b=10))
        st.plotly_chart(fig, use_container_width=True)

st.markdown("# Self-Healing Data Pipeline")
tab1, tab2 = st.tabs(["🔧  Live Healing", "📊  Benchmark"])
with tab1:
    live_healing()
with tab2:
    benchmark()
