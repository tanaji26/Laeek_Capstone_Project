
"""
pipeline_monitor.py
-------------------
Structured logging, run metadata, and observability layer.
Every agent imports this module to emit consistent, JSON-structured logs
that can be ingested by any log aggregator (ELK, Loki, CloudWatch, etc.).

Features:
  - Unique run_id per pipeline execution
  - Step-level timing and status tracking
  - JSON log output to stdout + flat file
  - Run manifest written to /data/logs/run_manifest.json
"""

import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

LOG_DIR = Path(os.getenv("LOG_DIR", "/data/logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ── Run identity ─────────────────────────────────────────────────────────────
RUN_ID        = os.getenv("PIPELINE_RUN_ID", str(uuid.uuid4())[:8])
PIPELINE_NAME = os.getenv("PIPELINE_NAME", "smartretail-pipeline")
RUN_TS        = datetime.now(timezone.utc).isoformat()

# ── File handler ─────────────────────────────────────────────────────────────
_log_file_path = LOG_DIR / f"run_{RUN_ID}.log"
_step_registry: list[dict] = []          # in-memory step journal


class JsonFormatter(logging.Formatter):
    """Emit every log record as a single JSON line."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts":        datetime.now(timezone.utc).isoformat(),
            "run_id":    RUN_ID,
            "pipeline":  PIPELINE_NAME,
            "level":     record.levelname,
            "agent":     getattr(record, "agent", "pipeline"),
            "step":      getattr(record, "step", "—"),
            "message":   record.getMessage(),
        }
        # Attach any extra keys passed via the `extra` dict
        for key, val in record.__dict__.items():
            if key not in ("msg", "args", "levelname", "levelno", "pathname",
                           "filename", "module", "exc_info", "exc_text",
                           "stack_info", "lineno", "funcName", "created",
                           "msecs", "relativeCreated", "thread", "threadName",
                           "processName", "process", "name", "message",
                           "agent", "step"):
                payload[key] = val
        return json.dumps(payload)


def _build_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.DEBUG)

    # stdout handler
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(JsonFormatter())
    logger.addHandler(sh)

    # file handler
    fh = logging.FileHandler(_log_file_path)
    fh.setFormatter(JsonFormatter())
    logger.addHandler(fh)

    return logger


_root_logger = _build_logger(PIPELINE_NAME)


# ── Public helpers ────────────────────────────────────────────────────────────

def get_logger(agent_name: str) -> logging.LoggerAdapter:
    """Return a logger pre-tagged with agent_name."""
    logger = _build_logger(f"{PIPELINE_NAME}.{agent_name}")
    return logging.LoggerAdapter(logger, extra={"agent": agent_name, "step": "—"})


class StepTracker:
    """
    Context manager that records timing and status for a named pipeline step.

    Usage:
        with StepTracker(logger, "validate_nulls") as step:
            step.set_meta(rows_checked=5000)
            ... do work ...
        # On exit: logs duration + status, appends to _step_registry
    """

    def __init__(self, logger: logging.LoggerAdapter, step_name: str, agent: str = ""):
        self.logger    = logger
        self.step_name = step_name
        self.agent     = agent or getattr(logger.extra, "get", lambda k, d: d)("agent", "")
        self._meta: dict[str, Any] = {}
        self._start: float = 0.0
        self.status  = "running"

    def set_meta(self, **kwargs):
        self._meta.update(kwargs)

    def __enter__(self):
        self._start = time.perf_counter()
        self.logger.info(
            f"START {self.step_name}",
            extra={"agent": self.agent, "step": self.step_name, "status": "start"}
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = round(time.perf_counter() - self._start, 3)
        self.status = "failed" if exc_type else "success"
        record = {
            "run_id":    RUN_ID,
            "agent":     self.agent,
            "step":      self.step_name,
            "status":    self.status,
            "elapsed_s": elapsed,
            **self._meta,
        }
        _step_registry.append(record)

        level = logging.ERROR if exc_type else logging.INFO
        self.logger.log(
            level,
            f"END {self.step_name} [{self.status}] ({elapsed}s)",
            extra={"agent": self.agent, "step": self.step_name,
                   "status": self.status, "elapsed_s": elapsed, **self._meta}
        )
        return False   # do not suppress exceptions


def write_run_manifest(extra_meta: Optional[dict] = None) -> Path:
    """
    Write a JSON manifest summarising the entire pipeline run.
    Called by the orchestration agent at the very end.
    """
    manifest = {
        "run_id":       RUN_ID,
        "pipeline":     PIPELINE_NAME,
        "run_ts":       RUN_TS,
        "end_ts":       datetime.now(timezone.utc).isoformat(),
        "steps":        _step_registry,
        "total_steps":  len(_step_registry),
        "failed_steps": [s for s in _step_registry if s["status"] == "failed"],
        **(extra_meta or {}),
    }
    manifest_path = LOG_DIR / "run_manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2, default=str)

    _root_logger.info(
        f"Run manifest written → {manifest_path}",
        extra={"agent": "monitor", "step": "manifest"}
    )
    return manifest_path


def log_metric(agent: str, step: str, **metrics):
    """Emit a structured metric line (row counts, scores, durations, etc.)."""
    _root_logger.info(
        f"METRIC [{agent}:{step}] {metrics}",
        extra={"agent": agent, "step": step, "metric": True, **metrics}
    )