"""
pipeline_lineage.py
-------------------
Shared utility for writing data lineage entries to lineage_log.json.

Each agent calls log_lineage() at the end of its run() / main() function.
The log file is append-based — every pipeline run adds new entries.

Output: /data/logs/lineage_log.json

Entry structure:
{
    "agent":      "governance_agent",
    "run_id":     "manual__2026-06-09T13:15:17+00:00",  # from Airflow env
    "timestamp":  "2026-06-09T13:16:45.123456+00:00",
    "inputs":     ["s3://data/bronze/orders_data.parquet"],
    "outputs":    ["s3://data/silver/orders_enriched.parquet"],
    "rows_in":    10000,
    "rows_out":   10000,
    "status":     "success",
    "notes":      "optional free-text"
}
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

LOG_DIR     = Path(os.getenv("LOG_DIR", "/data/logs"))
LINEAGE_FILE = LOG_DIR / "lineage_log.json"


def log_lineage(
    agent:    str,
    inputs:   list[str],
    outputs:  list[str],
    rows_in:  int  = 0,
    rows_out: int  = 0,
    status:   str  = "success",
    notes:    str  = "",
) -> None:
    """
    Append a lineage entry to lineage_log.json.

    Parameters
    ----------
    agent    : name of the calling agent (e.g. "governance_agent")
    inputs   : list of input paths / URIs consumed by this agent
    outputs  : list of output paths / URIs produced by this agent
    rows_in  : total rows read (0 if not applicable)
    rows_out : total rows written (0 if not applicable)
    status   : "success" | "partial" | "failed"
    notes    : optional free-text comment
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    entry = {
        "agent":     agent,
        "run_id":    os.getenv("AIRFLOW_CTX_DAG_RUN_ID", "local"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "inputs":    inputs,
        "outputs":   outputs,
        "rows_in":   rows_in,
        "rows_out":  rows_out,
        "status":    status,
        "notes":     notes,
    }

    # Load existing entries (or start fresh)
    if LINEAGE_FILE.exists():
        try:
            with open(LINEAGE_FILE) as f:
                log = json.load(f)
        except (json.JSONDecodeError, ValueError):
            log = []
    else:
        log = []

    log.append(entry)

    with open(LINEAGE_FILE, "w") as f:
        json.dump(log, f, indent=2, default=str)

    print(f"[lineage] {agent} → {status} | in={rows_in:,} out={rows_out:,} | logged to {LINEAGE_FILE}")