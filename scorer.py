"""
scorer.py  —  Multi-Dimensional Data Quality Scoring Engine
============================================================
Finance Cleaning Pipeline  |  by Akshay

Overview
--------
Every record is scored across FIVE independent quality dimensions.
Each dimension produces its own 0-100 sub-score, and those are combined
into a single weighted overall dq_score.

Why five dimensions?
--------------------
A single flat score hides the *reason* a record is bad.
A record can score 40 on Accuracy (wrong amounts) but 100 on Completeness
(all fields present). Knowing which dimension failed tells downstream teams
exactly what action to take — remediate in source, fix the ETL, or quarantine.

Dimensions & Business Rationale
--------------------------------

1. COMPLETENESS  (weight 20%)
   Question: "Are all required fields populated?"
   Business impact: Missing vendor/amount means a transaction can't be
   posted, reconciled, or audited. Scales with the proportion of critical
   fields that are null — a row missing 3 fields is worse than one missing 1.

   Critical fields scored: vendor_name, invoice_number, amount,
                            total_amount, currency, status

2. VALIDITY  (weight 25%)
   Question: "Do values conform to defined business rules?"
   Business impact: Invalid currency codes block ERP payment runs.
   Invalid statuses break workflow routing. Unparseable dates mean
   the record can't be aged, scheduled, or reported on.

   Checks:
     - currency_invalid         → -60 pts  (blocks payment processing)
     - status_invalid           → -50 pts  (breaks workflow routing)
     - invoice_date_parse_error → -30 pts  (can't age or schedule)
     - due_date_parse_error     → -25 pts  (SLA tracking breaks)
     - payment_date_parse_error → -20 pts  (reconciliation impact)

3. ACCURACY  (weight 35%  ← highest, because this is finance)
   Question: "Are values mathematically and financially correct?"
   Business impact: A negative invoice amount processed downstream causes
   real monetary error. A suspiciously large outlier could be a fat-finger
   entry or fraud. These carry the heaviest penalties because the cost of
   a wrong number is higher than the cost of a missing one.

   Checks:
     - amount_negative          → -70 pts  (impossible for an invoice)
     - total_amount_negative    → -70 pts  (same)
     - tax_amount_negative      → -40 pts  (less critical but still wrong)
     - amount_above_max         → -40 pts  (fraud / data entry error risk)
     - total_amount_above_max   → -40 pts  (same)
     - amount_outlier           → -20 pts  (statistical anomaly — could be legit)
     - total_amount_outlier     → -20 pts  (same)
     - tax_amount_outlier       → -10 pts  (least impactful outlier)

4. CONSISTENCY  (weight 15%)
   Question: "Are fields logically coherent with each other?"
   Business impact: A PAID invoice with no payment_date is internally
   contradictory — impossible to reconcile. A total that doesn't match
   amount + tax is an accounting error. A due_date before invoice_date
   is chronologically impossible.

   Checks (computed fresh on raw field values, not on flags):
     - PAID status but payment_date is UNKNOWN/missing → -60 pts
     - |total_amount - (amount + tax_amount)| > 0.02   → -60 pts  (reconciliation failure)
     - due_date parsed earlier than invoice_date        → -40 pts  (timeline impossible)

5. UNIQUENESS  (weight 5%)
   Question: "Does this record represent a distinct real-world event?"
   Business impact: Duplicates inflate counts, distort aggregates, and
   create double-payment risk. Weight is lower because the cleaner already
   removed exact duplicates — remaining rows are flagged as near-duplicates
   if they share invoice_number with a different invoice_id.

   Check:
     - duplicate invoice_number (shared across multiple rows) → -100 pts

Severity Classification
-----------------------
After scoring, each row is assigned a dq_severity label based on which
dimensions failed and by how much:

  CRITICAL  : Accuracy < 30  OR  Consistency < 40
              → Do not use. Requires manual remediation before processing.

  HIGH      : Accuracy < 60  OR  Validity < 40  OR  overall < 50
              → Significant issues. Use with caution; flag for review.

  MEDIUM    : overall < 70  OR  any single dimension < 60
              → Moderate issues. Review recommended for key use cases.

  LOW       : overall < 85
              → Minor issues. Acceptable for most analytical use.

  CLEAN     : overall >= 85  AND  all dimensions >= 70
              → Publish to gold layer as-is.

Grade Bands (overall dq_score)
-------------------------------
  A  90–100  Excellent
  B  75–89   Good
  C  60–74   Fair
  D  40–59   Poor
  F   0–39   Critical

Output Columns Added
--------------------
  dq_score                  overall weighted score  (0–100)
  dq_grade                  A / B / C / D / F
  dq_severity               CLEAN / LOW / MEDIUM / HIGH / CRITICAL
  dq_score_completeness     dimension sub-score  (0–100)
  dq_score_validity         dimension sub-score  (0–100)
  dq_score_accuracy         dimension sub-score  (0–100)
  dq_score_consistency      dimension sub-score  (0–100)
  dq_score_uniqueness       dimension sub-score  (0–100)
  dq_issues                 pipe-separated list of failed checks with dimension tag
"""

import logging
import re
from typing import Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Dimension weights (must sum to 1.0)
# ---------------------------------------------------------------------------
DIM_WEIGHTS = {
    "completeness": 0.20,
    "validity":     0.25,
    "accuracy":     0.35,
    "consistency":  0.15,
    "uniqueness":   0.05,
}

# ---------------------------------------------------------------------------
# Grade bands
# ---------------------------------------------------------------------------
GRADE_BANDS = [(90, "A"), (75, "B"), (60, "C"), (40, "D"), (0, "F")]

def _grade(score: float) -> str:
    for threshold, letter in GRADE_BANDS:
        if score >= threshold:
            return letter
    return "F"

def _clamp(val: float) -> float:
    return max(0.0, min(100.0, val))

# ---------------------------------------------------------------------------
# Severity classification
# ---------------------------------------------------------------------------
def _severity(row: pd.Series) -> str:
    acc  = row["dq_score_accuracy"]
    cons = row["dq_score_consistency"]
    val  = row["dq_score_validity"]
    ovr  = row["dq_score"]

    if acc < 30 or cons < 40:
        return "CRITICAL"
    if acc < 60 or val < 40 or ovr < 50:
        return "HIGH"
    if ovr < 70 or min(acc, cons, val, row["dq_score_completeness"]) < 60:
        return "MEDIUM"
    if ovr < 85:
        return "LOW"
    return "CLEAN"

# ---------------------------------------------------------------------------
# 1. COMPLETENESS
# ---------------------------------------------------------------------------
CRITICAL_FIELDS = ["vendor_name", "invoice_number", "amount",
                   "total_amount", "currency", "status"]
PLACEHOLDER_VALUES = {"UNKNOWN", "N/A", "NA", "NONE", ""}

def _score_completeness(df: pd.DataFrame) -> Tuple[pd.Series, list]:
    """
    Per-row completeness: 100 minus penalty proportional to how many
    critical fields are null or placeholder-valued.
    Penalty per missing critical field = 100 / len(CRITICAL_FIELDS)
    """
    present_fields = [f for f in CRITICAL_FIELDS if f in df.columns]
    if not present_fields:
        return pd.Series(100.0, index=df.index), []

    penalty_per_field = 100.0 / len(present_fields)
    score = pd.Series(100.0, index=df.index)
    issue_cols = []

    for field in present_fields:
        is_missing = (
            df[field].isna() |
            df[field].astype(str).str.strip().str.upper().isin(PLACEHOLDER_VALUES)
        )
        score -= is_missing.astype(float) * penalty_per_field
        issue_cols.append((is_missing, f"[Completeness] Missing critical field: {field}"))

    return score.apply(_clamp), issue_cols

# ---------------------------------------------------------------------------
# 2. VALIDITY
# ---------------------------------------------------------------------------
VALIDITY_RULES = [
    ("currency_invalid",          60, "[Validity] Invalid currency code — blocks payment processing"),
    ("status_invalid",            50, "[Validity] Invalid invoice status — breaks workflow routing"),
    ("invoice_date_parse_error",  30, "[Validity] Invoice date unparseable — can't age or schedule"),
    ("due_date_parse_error",      25, "[Validity] Due date unparseable — SLA tracking broken"),
    ("payment_date_parse_error",  20, "[Validity] Payment date unparseable — reconciliation risk"),
]

def _score_validity(df: pd.DataFrame) -> Tuple[pd.Series, list]:
    score = pd.Series(100.0, index=df.index)
    issue_cols = []
    for col, penalty, label in VALIDITY_RULES:
        if col not in df.columns:
            continue
        flag = df[col].fillna(0).astype(int)
        score -= flag * penalty
        issue_cols.append((flag == 1, label))
    return score.apply(_clamp), issue_cols

# ---------------------------------------------------------------------------
# 3. ACCURACY
# ---------------------------------------------------------------------------
ACCURACY_RULES = [
    ("amount_negative",       70, "[Accuracy] Negative invoice amount — financially impossible"),
    ("total_amount_negative", 70, "[Accuracy] Negative total amount — financially impossible"),
    ("tax_amount_negative",   40, "[Accuracy] Negative tax amount — accounting error"),
    ("amount_above_max",      40, "[Accuracy] Amount exceeds max threshold — fraud/entry risk"),
    ("total_amount_above_max",40, "[Accuracy] Total exceeds max threshold — fraud/entry risk"),
    ("amount_outlier",        20, "[Accuracy] Amount is a statistical outlier (z-score)"),
    ("total_amount_outlier",  20, "[Accuracy] Total is a statistical outlier (z-score)"),
    ("tax_amount_outlier",    10, "[Accuracy] Tax amount is a statistical outlier"),
]

def _score_accuracy(df: pd.DataFrame) -> Tuple[pd.Series, list]:
    score = pd.Series(100.0, index=df.index)
    issue_cols = []
    for col, penalty, label in ACCURACY_RULES:
        if col not in df.columns:
            continue
        flag = df[col].fillna(0).astype(int)
        score -= flag * penalty
        issue_cols.append((flag == 1, label))
    return score.apply(_clamp), issue_cols

# ---------------------------------------------------------------------------
# 4. CONSISTENCY  (cross-field logic — freshly computed here)
# ---------------------------------------------------------------------------

def _score_consistency(df: pd.DataFrame) -> Tuple[pd.Series, list]:
    """
    Three consistency checks:
      C1. PAID status but payment_date is missing/UNKNOWN          → -60
      C2. total_amount ≠ amount + tax_amount  (tolerance ±0.02)    → -60
      C3. due_date parsed before invoice_date (chronology broken)  → -40
    """
    score = pd.Series(100.0, index=df.index)
    issue_cols = []

    # C1 — PAID but no payment_date
    if "status" in df.columns and "payment_date" in df.columns:
        is_paid = df["status"].astype(str).str.upper() == "PAID"
        no_pay_date = (
            df["payment_date"].isna() |
            df["payment_date"].astype(str).str.strip().str.upper().isin(PLACEHOLDER_VALUES)
        )
        c1 = is_paid & no_pay_date
        score -= c1.astype(float) * 60
        issue_cols.append((c1, "[Consistency] Status=PAID but payment_date is missing"))

    # C2 — total ≠ amount + tax  (only where all three are numeric and non-null)
    if all(c in df.columns for c in ["amount", "tax_amount", "total_amount"]):
        amt   = pd.to_numeric(df["amount"],       errors="coerce")
        tax   = pd.to_numeric(df["tax_amount"],   errors="coerce").fillna(0)
        total = pd.to_numeric(df["total_amount"], errors="coerce")
        all_present = amt.notna() & total.notna()
        mismatch = all_present & ((total - (amt + tax)).abs() > 0.02)
        score -= mismatch.astype(float) * 60
        issue_cols.append((mismatch, "[Consistency] total_amount ≠ amount + tax_amount (reconciliation failure)"))

    # C3 — due_date before invoice_date
    if "invoice_date" in df.columns and "due_date" in df.columns:
        inv = pd.to_datetime(df["invoice_date"], errors="coerce")
        due = pd.to_datetime(df["due_date"],     errors="coerce")
        both_valid = inv.notna() & due.notna()
        reversed_dates = both_valid & (due < inv)
        score -= reversed_dates.astype(float) * 40
        issue_cols.append((reversed_dates, "[Consistency] due_date is before invoice_date (chronology impossible)"))

    return score.apply(_clamp), issue_cols

# ---------------------------------------------------------------------------
# 5. UNIQUENESS
# ---------------------------------------------------------------------------

def _score_uniqueness(df: pd.DataFrame) -> Tuple[pd.Series, list]:
    """
    Flag rows where invoice_number appears more than once in the dataset.
    (Exact duplicates were removed by cleaner; these are near-duplicates —
    same invoice number, different row content.)
    """
    score = pd.Series(100.0, index=df.index)
    issue_cols = []

    if "invoice_number" in df.columns:
        dup_mask = df["invoice_number"].duplicated(keep=False)
        score -= dup_mask.astype(float) * 100
        issue_cols.append((dup_mask, "[Uniqueness] invoice_number appears on multiple rows (near-duplicate risk)"))

    return score.apply(_clamp), issue_cols

# ---------------------------------------------------------------------------
# Master scorer
# ---------------------------------------------------------------------------

def score_dataframe(df: pd.DataFrame) -> Tuple[pd.DataFrame, dict]:
    """
    Compute all five dimension scores and the weighted overall dq_score.

    Returns (df_with_scores, stats_dict)
    """
    df = df.copy()

    scorers = {
        "completeness": _score_completeness,
        "validity":     _score_validity,
        "accuracy":     _score_accuracy,
        "consistency":  _score_consistency,
        "uniqueness":   _score_uniqueness,
    }

    all_issue_masks = []   # list of (bool_series, label_string)
    dim_scores = {}

    for dim, fn in scorers.items():
        dim_score, issue_cols = fn(df)
        dim_scores[dim] = dim_score
        df[f"dq_score_{dim}"] = dim_score.round(1)
        all_issue_masks.extend(issue_cols)

    # Weighted overall score
    overall = sum(
        dim_scores[dim] * DIM_WEIGHTS[dim]
        for dim in DIM_WEIGHTS
    ).apply(_clamp).round(1)
    df["dq_score"] = overall.astype(int)
    df["dq_grade"] = overall.apply(_grade)

    # Severity label
    df["dq_severity"] = df[[
        "dq_score", "dq_score_accuracy",
        "dq_score_consistency", "dq_score_validity",
        "dq_score_completeness"
    ]].apply(_severity, axis=1)

    # Build dq_issues: pipe-separated list of all failed checks with dimension tag
    issue_lists = [""] * len(df)
    for mask_series, label in all_issue_masks:
        mask = mask_series.fillna(False).tolist()
        for i, failed in enumerate(mask):
            if failed:
                issue_lists[i] = (issue_lists[i] + " | " + label
                                  if issue_lists[i] else label)
    df["dq_issues"] = issue_lists

    # ── Stats ────────────────────────────────────────────────────────────────
    grade_dist = df["dq_grade"].value_counts().to_dict()
    sev_dist   = df["dq_severity"].value_counts().to_dict()

    stats = {
        # Overall
        "dq_mean_score":           round(float(overall.mean()), 1),
        "dq_median_score":         round(float(overall.median()), 1),
        "dq_min_score":            int(overall.min()),
        "dq_perfect_rows":         int((overall >= 99).sum()),
        # Dimension averages
        "dq_avg_completeness":     round(float(dim_scores["completeness"].mean()), 1),
        "dq_avg_validity":         round(float(dim_scores["validity"].mean()), 1),
        "dq_avg_accuracy":         round(float(dim_scores["accuracy"].mean()), 1),
        "dq_avg_consistency":      round(float(dim_scores["consistency"].mean()), 1),
        "dq_avg_uniqueness":       round(float(dim_scores["uniqueness"].mean()), 1),
        # Grade distribution
        "dq_grade_A":  grade_dist.get("A", 0),
        "dq_grade_B":  grade_dist.get("B", 0),
        "dq_grade_C":  grade_dist.get("C", 0),
        "dq_grade_D":  grade_dist.get("D", 0),
        "dq_grade_F":  grade_dist.get("F", 0),
        # Severity distribution
        "dq_sev_clean":    sev_dist.get("CLEAN",    0),
        "dq_sev_low":      sev_dist.get("LOW",      0),
        "dq_sev_medium":   sev_dist.get("MEDIUM",   0),
        "dq_sev_high":     sev_dist.get("HIGH",     0),
        "dq_sev_critical": sev_dist.get("CRITICAL", 0),
    }

    logger.info(
        f"[Score] Overall mean: {stats['dq_mean_score']} | "
        f"Dimensions — "
        f"Completeness:{stats['dq_avg_completeness']} "
        f"Validity:{stats['dq_avg_validity']} "
        f"Accuracy:{stats['dq_avg_accuracy']} "
        f"Consistency:{stats['dq_avg_consistency']} "
        f"Uniqueness:{stats['dq_avg_uniqueness']}"
    )
    logger.info(
        f"[Score] Severity — "
        f"CLEAN:{stats['dq_sev_clean']} LOW:{stats['dq_sev_low']} "
        f"MEDIUM:{stats['dq_sev_medium']} HIGH:{stats['dq_sev_high']} "
        f"CRITICAL:{stats['dq_sev_critical']}"
    )

    return df, stats
