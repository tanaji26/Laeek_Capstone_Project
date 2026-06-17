"""
insight_agent.py
-----------------
Enterprise Insight & Reporting Agent

Code overview:
  1. Read Silver layer Parquet files (enriched, joined)
  2. Build Gold layer aggregations (KPI tables) → s3://data/gold/
  3. Derive metrics dict from Gold tables for LLM prompts
  4. Call OpenAI API with domain-specific prompts to generate:
       - Executive summary
       - Revenue insights
       - Customer segment analysis
       - Product performance narrative
       - Operational risk flags
  5. Write full report to /data/logs/insight_report.json
     and a human-readable Markdown summary to /data/logs/insight_summary.md

Input  : s3://data/silver/<table>.parquet  (enriched by governance_agent)
Output : s3://data/gold/<table>.parquet    (aggregated KPI tables)
         insight_report.json + insight_summary.md
"""

import io
import json
import os
import sys
import time
import openai
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pandas as pd
import numpy as np

from pipeline_monitor import get_logger, StepTracker, log_metric, LOG_DIR

AGENT  = "insight_agent"
logger = get_logger(AGENT)

S3 = boto3.client(
    "s3",
    endpoint_url          = os.environ["MINIO_ENDPOINT"],
    aws_access_key_id     = os.environ["MINIO_ACCESS_KEY"],
    aws_secret_access_key = os.environ["MINIO_SECRET_KEY"],
    region_name           = "us-east-1",
)
BUCKET      = "data"
BRONZE_PFX  = "bronze"
SILVER_PFX  = "silver"
GOLD_PFX    = "gold"

OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
OPENAI_MODEL   = os.getenv("OPENAI_MODEL", "gpt-4o")

openai.api_key = OPENAI_API_KEY


# MinIO helpers

def _read_parquet(prefix: str, table_name: str) -> pd.DataFrame:
    key = f"{prefix}/{table_name}.parquet"
    obj = S3.get_object(Bucket=BUCKET, Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


def _write_gold(df: pd.DataFrame, table_name: str) -> str:
    """Upload a DataFrame as Parquet to the gold zone."""
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    key = f"{GOLD_PFX}/{table_name}.parquet"
    S3.put_object(Bucket=BUCKET, Key=key, Body=buffer.getvalue())
    return f"s3://{BUCKET}/{key}"


# Gold layer builder

def build_gold(orders_enriched: pd.DataFrame, customers_clean: pd.DataFrame,
               products_clean: pd.DataFrame, feedback_enriched: pd.DataFrame) -> dict:
    """
    Compute all Gold aggregations, write to s3://data/gold/, and return
    the metrics dict consumed by the LLM prompts.

    Gold tables produced:
      revenue_summary            — single-row overall KPIs
      monthly_revenue_trend      — revenue + orders per month
      category_performance       — revenue/orders/AOV per category
      product_performance        — per-product revenue, units, review score
      customer_segment_summary   — Prime vs Non-Prime breakdown
      regional_performance       — revenue/orders per region
      payment_method_summary     — order count + revenue per payment method
      feedback_score_distribution — count + share per star rating
      feedback_category_csat     — avg review score per category
    """
    delivered = orders_enriched[orders_enriched["order_status"] == "Delivered"].copy()
    cancelled = orders_enriched[orders_enriched["order_status"] == "Cancelled"].copy()
    returned  = orders_enriched[orders_enriched["order_status"] == "Returned"].copy()
    total_orders = len(orders_enriched)

    # ── gold/revenue_summary ──────────────────────────────────────────────────
    revenue_summary = pd.DataFrame([{
        "total_orders":          total_orders,
        "delivered_orders":      len(delivered),
        "cancelled_orders":      len(cancelled),
        "returned_orders":       len(returned),
        "cancellation_rate_pct": round(len(cancelled) / total_orders * 100, 2),
        "return_rate_pct":       round(len(returned)  / total_orders * 100, 2),
        "total_gmv_inr":         round(float(orders_enriched["final_amount"].sum()), 2),
        "total_revenue_inr":     round(float(delivered["final_amount"].sum()), 2),
        "total_discount_inr":    round(float(orders_enriched["total_discount"].sum()), 2),
        "avg_order_value_inr":   round(float(delivered["final_amount"].mean()), 2),
        "total_customers":       len(customers_clean),
        "prime_customers":       int(customers_clean["is_prime_customer"].sum()),
        "total_products":        len(products_clean),
        "avg_review_score":      round(float(feedback_enriched["review_score"].mean()), 2),
        "low_reviews_count":     int((feedback_enriched["review_score"] <= 2).sum()),
        "high_reviews_count":    int((feedback_enriched["review_score"] >= 4).sum()),
        "generated_at":          datetime.now(timezone.utc).isoformat(),
    }])
    _write_gold(revenue_summary, "revenue_summary")

    # ── gold/monthly_revenue_trend ────────────────────────────────────────────
    monthly_df = (
        delivered.groupby(
            delivered["order_date"].dt.to_period("M").astype(str)
        )["final_amount"]
        .agg(revenue="sum", orders="count")
        .round(2)
        .reset_index()
        .rename(columns={"order_date": "month"})
        .sort_values("month")
    )
    _write_gold(monthly_df, "monthly_revenue_trend")

    # ── gold/category_performance ─────────────────────────────────────────────
    cat_perf = (
        delivered.groupby("product_category")
        .agg(
            total_revenue_inr = ("final_amount",   "sum"),
            total_orders      = ("order_id",       "count"),
            avg_order_value   = ("final_amount",   "mean"),
            total_discount    = ("total_discount", "sum"),
            avg_discount_pct  = ("discount_pct",   "mean"),
        )
        .round(2)
        .reset_index()
        .sort_values("total_revenue_inr", ascending=False)
    )
    cat_perf["revenue_share_pct"] = (
        cat_perf["total_revenue_inr"] / cat_perf["total_revenue_inr"].sum() * 100
    ).round(2)
    _write_gold(cat_perf, "category_performance")

    # ── gold/product_performance ──────────────────────────────────────────────
    prod_rev = (
        delivered.groupby(["product_id", "product_name", "product_category"])
        .agg(
            total_revenue_inr = ("final_amount", "sum"),
            total_orders      = ("order_id",     "count"),
            total_units_sold  = ("order_qty",    "sum"),
            avg_order_value   = ("final_amount", "mean"),
        )
        .round(2)
        .reset_index()
    )
    prod_feedback = (
        feedback_enriched.groupby("product_id")["review_score"]
        .agg(avg_review_score="mean", review_count="count")
        .round(2)
        .reset_index()
    )
    prod_perf = prod_rev.merge(prod_feedback, on="product_id", how="left")
    prod_perf["revenue_rank"] = prod_perf["total_revenue_inr"].rank(
        ascending=False, method="dense"
    ).astype(int)
    prod_perf = prod_perf.sort_values("total_revenue_inr", ascending=False)
    _write_gold(prod_perf, "product_performance")

    # ── gold/customer_segment_summary ─────────────────────────────────────────
    delivered["segment"] = delivered["is_prime_active"].map(
        {True: "Prime", False: "Non-Prime"}
    )
    segment = (
        delivered.groupby("segment")
        .agg(
            total_revenue_inr = ("final_amount", "sum"),
            total_orders      = ("order_id",     "count"),
            unique_customers  = ("customer_id",  "nunique"),
            avg_order_value   = ("final_amount", "mean"),
            avg_discount_pct  = ("discount_pct", "mean"),
        )
        .round(2)
        .reset_index()
    )
    segment["revenue_share_pct"] = (
        segment["total_revenue_inr"] / segment["total_revenue_inr"].sum() * 100
    ).round(2)
    _write_gold(segment, "customer_segment_summary")

    # ── gold/regional_performance ─────────────────────────────────────────────
    regional = (
        delivered.groupby("customer_region")
        .agg(
            total_revenue_inr = ("final_amount", "sum"),
            total_orders      = ("order_id",     "count"),
            unique_customers  = ("customer_id",  "nunique"),
            avg_order_value   = ("final_amount", "mean"),
        )
        .round(2)
        .reset_index()
        .sort_values("total_revenue_inr", ascending=False)
    )
    regional["revenue_share_pct"] = (
        regional["total_revenue_inr"] / regional["total_revenue_inr"].sum() * 100
    ).round(2)
    _write_gold(regional, "regional_performance")

    # ── gold/payment_method_summary ───────────────────────────────────────────
    payment = (
        orders_enriched.groupby("payment_method")
        .agg(
            total_orders      = ("order_id",     "count"),
            total_revenue_inr = ("final_amount", "sum"),
            avg_order_value   = ("final_amount", "mean"),
        )
        .round(2)
        .reset_index()
        .sort_values("total_orders", ascending=False)
    )
    payment["order_share_pct"] = (
        payment["total_orders"] / payment["total_orders"].sum() * 100
    ).round(2)
    _write_gold(payment, "payment_method_summary")

    # ── gold/feedback_score_distribution ─────────────────────────────────────
    score_dist = (
        feedback_enriched.groupby("review_score")
        .agg(count=("feedback_id", "count"))
        .reset_index()
    )
    score_dist["share_pct"] = (
        score_dist["count"] / score_dist["count"].sum() * 100
    ).round(2)
    _write_gold(score_dist, "feedback_score_distribution")

    # ── gold/feedback_category_csat ───────────────────────────────────────────
    cat_csat = (
        feedback_enriched.groupby("product_category")["review_score"]
        .agg(avg_score="mean", review_count="count")
        .round(2)
        .reset_index()
        .sort_values("avg_score", ascending=False)
    )
    _write_gold(cat_csat, "feedback_category_csat")

    # ── Build metrics dict for LLM prompts (same shape as before) ─────────────
    rs   = revenue_summary.iloc[0]
    top5 = prod_perf.head(5)[["product_name", "product_category", "total_revenue_inr"]].copy()
    top5 = top5.rename(columns={"total_revenue_inr": "final_amount"})

    prime_row    = segment[segment["segment"] == "Prime"]
    nonprime_row = segment[segment["segment"] == "Non-Prime"]
    prime_rev    = float(prime_row["total_revenue_inr"].iloc[0])    if len(prime_row)    else 0.0
    nonprime_rev = float(nonprime_row["total_revenue_inr"].iloc[0]) if len(nonprime_row) else 0.0

    monthly_trend = dict(zip(monthly_df["month"], monthly_df["revenue"].tail(6)))

    metrics = {
        "total_orders":           int(rs["total_orders"]),
        "delivered_orders":       int(rs["delivered_orders"]),
        "cancelled_orders":       int(rs["cancelled_orders"]),
        "returned_orders":        int(rs["returned_orders"]),
        "cancellation_rate_pct":  float(rs["cancellation_rate_pct"]),
        "return_rate_pct":        float(rs["return_rate_pct"]),
        "total_gmv_inr":          float(rs["total_gmv_inr"]),
        "total_revenue_inr":      float(rs["total_revenue_inr"]),
        "total_discount_inr":     float(rs["total_discount_inr"]),
        "avg_order_value_inr":    float(rs["avg_order_value_inr"]),
        "order_status_dist":      orders_enriched["order_status"].value_counts().to_dict(),
        "top_5_products":         top5.to_dict("records"),
        "category_revenue":       dict(zip(cat_perf["product_category"],
                                           cat_perf["total_revenue_inr"])),
        "prime_revenue_inr":      prime_rev,
        "non_prime_revenue_inr":  nonprime_rev,
        "regional_revenue":       dict(zip(regional["customer_region"],
                                           regional["total_revenue_inr"])),
        "payment_method_split":   dict(zip(payment["payment_method"],
                                           payment["total_orders"])),
        "avg_review_score":       float(rs["avg_review_score"]),
        "review_score_dist":      {str(int(r["review_score"])): int(r["count"])
                                   for _, r in score_dist.iterrows()},
        "low_reviews_count":      int(rs["low_reviews_count"]),
        "high_reviews_count":     int(rs["high_reviews_count"]),
        "monthly_revenue_trend":  monthly_trend,
        "total_customers":        int(rs["total_customers"]),
        "prime_customers":        int(rs["prime_customers"]),
        "total_products":         int(rs["total_products"]),
    }
    return metrics


# Open AI LLM calls

def _call_llm(prompt: str, system: str = "") -> str:
    """Send a prompt to OpenAI and return the response text."""
    try:
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        response = openai.chat.completions.create(
            model    = OPENAI_MODEL,
            messages = messages,
        )
        return response.choices[0].message.content.strip()
    except openai.AuthenticationError:
        return "[OpenAI error: Invalid API key]"
    except openai.RateLimitError:
        return "[OpenAI error: Rate limit hit — retry later]"
    except Exception as e:
        return f"[LLM error: {e}]"

SYSTEM_PROMPT = """You are a senior retail analytics AI for SmartRetail Analytics, 
an Indian e-commerce platform. You analyse structured business metrics and produce 
concise, data-driven insights for C-level executives. Use INR currency. 
Be specific, cite numbers, and flag risks clearly."""


def generate_executive_summary(metrics: dict) -> str:
    prompt = f"""
Based on the following retail performance metrics for SmartRetail Analytics, 
write a 3-paragraph executive summary covering overall business health, 
revenue performance, and one key risk or opportunity.

Metrics:
- Total orders: {metrics['total_orders']:,}
- Delivered: {metrics['delivered_orders']:,} | Cancelled: {metrics['cancelled_orders']:,} | Returned: {metrics['returned_orders']:,}
- Cancellation rate: {metrics['cancellation_rate_pct']}% | Return rate: {metrics['return_rate_pct']}%
- Total GMV: ₹{metrics['total_gmv_inr']:,.2f}
- Net revenue (delivered): ₹{metrics['total_revenue_inr']:,.2f}
- Total discounts given: ₹{metrics['total_discount_inr']:,.2f}
- Average order value: ₹{metrics['avg_order_value_inr']:,.2f}
- Prime customer revenue: ₹{metrics['prime_revenue_inr']:,.2f}
- Non-prime revenue: ₹{metrics['non_prime_revenue_inr']:,.2f}
- Average review score: {metrics['avg_review_score']}/5
- Low reviews (1-2 stars): {metrics['low_reviews_count']}
"""
    return _call_llm(prompt, system=SYSTEM_PROMPT)


def generate_product_insights(metrics: dict) -> str:
    top = "\n".join([
        f"  {i+1}. {p['product_name']} ({p['product_category']}): ₹{p['final_amount']:,.2f}"
        for i, p in enumerate(metrics['top_5_products'])
    ])
    cat = "\n".join([f"  {k}: ₹{v:,.2f}" for k, v in metrics['category_revenue'].items()])
    prompt = f"""
Analyse the following product performance data for SmartRetail Analytics 
and provide 3 actionable recommendations for the merchandising team.

Top 5 products by revenue:
{top}

Revenue by category:
{cat}

Write 2–3 sentences of analysis followed by 3 numbered recommendations.
"""
    return _call_llm(prompt, system=SYSTEM_PROMPT)


def generate_customer_insights(metrics: dict) -> str:
    regional = "\n".join([f"  {k}: ₹{v:,.2f}" for k, v in metrics['regional_revenue'].items()])
    prompt = f"""
Analyse the customer behaviour data below for SmartRetail Analytics 
and provide insights on prime membership value, regional performance, 
and payment preferences. Suggest one growth initiative.

- Total customers: {metrics['total_customers']:,}
- Prime customers: {metrics['prime_customers']:,} ({round(metrics['prime_customers']/metrics['total_customers']*100, 1)}%)
- Prime revenue: ₹{metrics['prime_revenue_inr']:,.2f}
- Non-prime revenue: ₹{metrics['non_prime_revenue_inr']:,.2f}
- Payment methods: {metrics['payment_method_split']}

Revenue by region:
{regional}

Write a concise paragraph of analysis and one specific growth initiative.
"""
    return _call_llm(prompt, system=SYSTEM_PROMPT)


def generate_risk_flags(metrics: dict, quality_report: dict) -> str:
    overall_dq = quality_report.get("overall_score", "N/A")
    failed_checks = []
    for tbl in quality_report.get("tables", []):
        for chk in tbl.get("checks", []):
            if not chk.get("passed"):
                failed_checks.append(f"  [{tbl['table']}] {chk['check']}: {chk['rows_failed']} rows")

    failed_str = "\n".join(failed_checks[:10]) if failed_checks else "  None"
    prompt = f"""
You are a data governance officer. Based on the pipeline metrics below, 
identify the top 3 operational risks and recommend mitigations for each.

Pipeline health:
- Overall DQ score: {overall_dq}/100
- Cancellation rate: {metrics['cancellation_rate_pct']}%
- Return rate: {metrics['return_rate_pct']}%
- Low review count (1-2 stars): {metrics['low_reviews_count']}
- Average review score: {metrics['avg_review_score']}/5

Failed data quality checks:
{failed_str}

Format as: Risk 1: [description] → Mitigation: [action]
"""
    return _call_llm(prompt, system=SYSTEM_PROMPT)


#Report writer

def write_markdown_report(metrics: dict, insights: dict, ts: str) -> Path:
    monthly_rows = "\n".join(
        [f"| {m} | ₹{v:,.2f} |" for m, v in metrics["monthly_revenue_trend"].items()]
    )
    top_prod_rows = "\n".join(
        [f"| {p['product_name'][:40]} | {p['product_category']} | ₹{p['final_amount']:,.2f} |"
         for p in metrics["top_5_products"]]
    )

    md = f"""# SmartRetail Analytics — Pipeline Insight Report
**Generated:** {ts}  
**Model:** {OPENAI_MODEL} (via OpenAI)

---

## 📊 Key Metrics at a Glance

| Metric | Value |
|--------|-------|
| Total Orders | {metrics['total_orders']:,} |
| Delivered | {metrics['delivered_orders']:,} |
| Cancellation Rate | {metrics['cancellation_rate_pct']}% |
| Return Rate | {metrics['return_rate_pct']}% |
| Total GMV | ₹{metrics['total_gmv_inr']:,.2f} |
| Net Revenue | ₹{metrics['total_revenue_inr']:,.2f} |
| Avg Order Value | ₹{metrics['avg_order_value_inr']:,.2f} |
| Prime Customers | {metrics['prime_customers']:,} / {metrics['total_customers']:,} |
| Avg Review Score | {metrics['avg_review_score']} / 5 |

---

## 📈 Monthly Revenue Trend (Last 6 Months)

| Month | Revenue |
|-------|---------|
{monthly_rows}

---

## 🏆 Top 5 Products by Revenue

| Product | Category | Revenue |
|---------|----------|---------|
{top_prod_rows}

---

## 🤖 AI-Generated Executive Summary

{insights.get('executive_summary', '_Not generated_')}

---

## 📦 Product & Category Insights

{insights.get('product_insights', '_Not generated_')}

---

## 👥 Customer & Regional Insights

{insights.get('customer_insights', '_Not generated_')}

---

## ⚠️ Risk Flags & Mitigations

{insights.get('risk_flags', '_Not generated_')}

---
*Report generated by SmartRetail Insight Agent | OpenAI/{OPENAI_MODEL}*
"""
    path = LOG_DIR / "insight_summary.md"
    path.write_text(md)
    return path


# Entry point

def run() -> bool:
    logger.info("Insight Agent starting", extra={"agent": AGENT, "step": "init"})

    # 1. Load Silver layer
    with StepTracker(logger, "load_data", AGENT):
        orders    = _read_parquet(SILVER_PFX, "orders_enriched")
        customers = _read_parquet(SILVER_PFX, "customers_clean")
        products  = _read_parquet(SILVER_PFX, "products_clean")
        feedback  = _read_parquet(SILVER_PFX, "feedback_enriched")
        logger.info(
            f"Loaded silver: orders={len(orders)}, customers={len(customers)}, "
            f"products={len(products)}, feedback={len(feedback)}",
            extra={"agent": AGENT, "step": "load_data"}
        )

    # 2. Build Gold layer + derive metrics dict
    logger.info("Building Gold layer...", extra={"agent": AGENT, "step": "gold_init"})
    with StepTracker(logger, "build_gold", AGENT):
        metrics = build_gold(orders, customers, products, feedback)
        log_metric(AGENT, "metrics", **{k: v for k, v in metrics.items()
                                        if isinstance(v, (int, float))})

    # 3. Load quality report for risk context
    quality_report = {}
    qr_path = LOG_DIR / "quality_report.json"
    if qr_path.exists():
        with open(qr_path) as f:
            quality_report = json.load(f)

    # 4. Generate LLM insights
    logger.info("Calling OpenAI API for insights",
            extra={"agent": AGENT, "step": "llm_init"})
    insights = {}
    with StepTracker(logger, "llm_executive_summary", AGENT):
        insights["executive_summary"] = generate_executive_summary(metrics)
    with StepTracker(logger, "llm_product_insights", AGENT):
        insights["product_insights"] = generate_product_insights(metrics)
    with StepTracker(logger, "llm_customer_insights", AGENT):
        insights["customer_insights"] = generate_customer_insights(metrics)
    with StepTracker(logger, "llm_risk_flags", AGENT):
        insights["risk_flags"] = generate_risk_flags(metrics, quality_report)

    # 5. Write outputs
    ts = datetime.now(timezone.utc).isoformat()
    report = {
        "agent":    AGENT,
        "ts":       ts,
        "model":    OPENAI_MODEL,
        "metrics":  metrics,
        "insights": insights,
    }
    report_path = LOG_DIR / "insight_report.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    md_path = write_markdown_report(metrics, insights, ts)

    logger.info(f"Insight report → {report_path}",
                extra={"agent": AGENT, "step": "report"})
    logger.info(f"Markdown summary → {md_path}",
                extra={"agent": AGENT, "step": "report"})

    return True


if __name__ == "__main__":
    run()