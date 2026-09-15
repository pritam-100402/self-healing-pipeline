# Self-Healing Data Pipeline

A data pipeline that watches its own output, catches quality problems, and either fixes them, quarantines them, or escalates them — instead of silently passing bad data downstream.

I built this around a simple idea. Most data-quality tooling just flags problems and leaves you to clean them up by hand. I wanted something that decides what to do with each batch on its own, while staying honest about the limits: it only auto-fixes what's safe, and it never pretends a failure is healthy.

## How it decides

Every batch is profiled and sorted into one of three outcomes:

- **Fix** — a known, safe problem. Duplicates get deduped, out-of-range values clamped, type mismatches coerced, additive schema drift absorbed. The result is verified before it's written.
- **Quarantine** — a known but unsafe problem, like a batch full of nulls. The good rows go through; the bad rows are set aside in a dead-letter table with the reason they failed, not dropped.
- **Escalate** — something unknown, or an operational failure (a dead source, a volume spike that isn't just resent duplicates). It goes to a human.

The rules driving this live in a table, not in code, so I can add new ones at runtime. When a batch matches no rule, that's an "unknown", and that's where the LLM fits: it proposes a new rule as a structured condition (never a code change or a direct edit to the data), a human approves it, and from then on that failure is handled deterministically with no model in the loop. The LLM isn't meant to run forever — its job is to shrink the set of unknowns over time.

One thing I was deliberate about: a monitor that *can't* measure something (for example, drift detection when the baseline is missing) reports "unavailable" and forces a non-green verdict. A broken sensor never becomes a green light.

## Two implementations

Same design, two runtimes, no shared code — just a shared results file.

- **`spark_pipeline.py`** — the PySpark / Databricks version: Structured Streaming, Delta tables, a bronze → silver → gold layout. This is the version meant to run at scale.
- **`pipeline.py` and the Streamlit app** — a pandas version that runs anywhere with no cluster. It powers the demo and produces the benchmark numbers.

## The app

`app.py` is a Streamlit app with two tabs:

- **Live Healing** — drop in any CSV, TSV, JSON, JSONL, or Excel file (structured or semi-structured, with varying fields) and watch it get profiled, the problem cells flagged, the fixes applied, and the clean-vs-quarantined split laid out with a conservation check. The learning loop is here too: when the engine finds an anomaly no active rule covers, it proposes a rule you approve with a button, and the data re-heals with it.
- **Benchmark** — the measured scorecard: detection, precision, false-positive rate, the fix/quarantine/escalate breakdown, per-fault results, and the learning-loop chart.

## Running it locally

```bash
pip install -r requirements.txt
python pipeline.py       # runs the pipeline + benchmark, writes results.json
streamlit run app.py     # opens the dashboard
```

The Spark version runs as a single file inside a Databricks workspace (it uses the `spark` session and Unity Catalog volumes; pyspark is provided by the runtime, so it isn't in requirements.txt). Import it and call `run()`.

## Numbers

From the pandas benchmark — 225 injected faults plus 45 clean batches, fixed seed:

- Detection 92%, precision 100%, false-positive rate 0%
- Auto-heal 75.8% of detected, escalation 24.2% (by design — volume anomalies and sub-threshold unknowns should escalate, not be force-fixed)
- Data preservation 100%, zero unaccounted rows
- Sensing reliability 100%; with the baseline removed, clean batches report 0 green and raise an incident each
- Learning loop: rules 8 → 9, LLM calls 3 → 0 on the novel fault

Run `python pipeline.py` to regenerate them. On Databricks the same experiment runs through the real Spark functions and produces a results file in the same shape.

## What it doesn't do

- It handles a bounded (but growing) set of known problems. You can only detect what you measure, so a corruption that moves no signal is invisible to it.
- Auto-correction can mask a real upstream issue, so everything is logged, retries are capped, and anything unknown escalates.
- Single-batch drift detection is noisy on small batches. I set the threshold from measured noise; a production version should track drift across a window of batches instead.
- The Databricks side runs on a workspace — it's a pipeline, not a hosted service. The Streamlit app is the part meant to be deployed.

## Deploying the app for free

Push this repo to GitHub, then on share.streamlit.io create a new app pointing at `app.py`. It installs from `requirements.txt` and gives you a public URL. The Live Healing tab is fully interactive there — it runs the pandas engine on Streamlit's server, so it needs no Databricks connection.
