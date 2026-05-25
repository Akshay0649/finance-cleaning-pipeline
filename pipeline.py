"""
pipeline.py  —  Finance Data Cleaning Pipeline
"""

import csv
import logging
import os
import sys
from datetime import datetime

import pandas as pd

from db_connector import get_engine
from cleaner import run_all_cleaners
from scorer import score_dataframe, DIM_WEIGHTS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

BANNER = """
╔══════════════════════════════════════════════════════════╗
║     Finance Data Cleaning Pipeline  |  by Akshay         ║
╚══════════════════════════════════════════════════════════╝
"""

def load_config(path="config.csv"):
    if not os.path.exists(path):
        logger.error(f"config.csv not found at '{path}'. Aborting.")
        sys.exit(1)
    config = {}
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            config[row["parameter"].strip()] = row["value"].strip()
    logger.info(f"[Config] Loaded {len(config)} parameters from '{path}'")
    return config

def extract_data(engine, config):
    mode  = config.get("db_mode", "demo").lower()
    table = config["source_table"]
    query = f"SELECT * FROM {table}" if mode == "demo" \
            else f"SELECT * FROM [{config.get('source_schema','dbo')}].[{table}]"
    logger.info(f"[Extract] {query}")
    df = pd.read_sql(query, engine)
    logger.info(f"[Extract] Loaded {len(df):,} rows × {len(df.columns)} columns")
    return df

def export_csv(df, config):
    path = config.get("export_csv_path", "output/invoices_cleaned.csv")
    os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8")
    logger.info(f"[Export] CSV → {path}  ({len(df):,} rows)")
    return path

def write_to_db(df, engine, config):
    mode  = config.get("db_mode", "demo").lower()
    table = config.get("cleaned_table", "invoices_cleaned")
    schema = config.get("cleaned_schema", "dbo")
    if mode == "demo":
        df.to_sql(table, engine, if_exists="replace", index=False)
    else:
        df.to_sql(table, engine, schema=schema, if_exists="replace", index=False, chunksize=500)
    logger.info(f"[Write-back] '{table}' written to DB")

def append_run_log(config, clean_stats, score_stats, rows_in, rows_out):
    path = config.get("log_csv_path", "output/pipeline_run_log.csv")
    os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)
    record = {
        "run_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source_table":  config.get("source_table"),
        "cleaned_table": config.get("cleaned_table"),
        "rows_input":    rows_in,
        "rows_output":   rows_out,
        **clean_stats, **score_stats,
    }
    exists = os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=record.keys())
        if not exists:
            w.writeheader()
        w.writerow(record)
    logger.info(f"[Audit] Run logged → {path}")

# ---------------------------------------------------------------------------
# Summary report
# ---------------------------------------------------------------------------
SEV_COLORS = {"CLEAN": "✅", "LOW": "🟡", "MEDIUM": "🟠", "HIGH": "🔴", "CRITICAL": "🚨"}

def _bar(score: float, width: int = 18) -> str:
    filled = round((score / 100) * width)
    return "█" * filled + "░" * (width - filled)

def print_summary(clean_stats, score_stats, rows_in, rows_out, csv_path):
    W = 68
    print("\n" + "═" * W)
    print("  PIPELINE SUMMARY")
    print("═" * W)
    print(f"  {'Rows loaded (raw):':<40} {rows_in:>6,}")
    print(f"  {'Rows output (cleaned):':<40} {rows_out:>6,}")
    print(f"  {'Duplicates removed:':<40} {clean_stats.get('duplicates_removed',0):>6,}")
    print("─" * W)
    print(f"  {'Null cells filled:':<40} {clean_stats.get('nulls_filled',0):>6,}")
    print(f"  {'Date parse errors:':<40} {clean_stats.get('date_parse_errors',0):>6,}")
    print(f"  {'Negative amount flags:':<40} {clean_stats.get('amount_negative_flags',0):>6,}")
    print(f"  {'Above-max amount flags:':<40} {clean_stats.get('amount_above_max_flags',0):>6,}")
    print(f"  {'Invalid currency codes fixed:':<40} {clean_stats.get('currency_invalid',0):>6,}")
    print(f"  {'Invalid statuses corrected:':<40} {clean_stats.get('status_invalid',0):>6,}")
    print(f"  {'Outliers flagged:':<40} {clean_stats.get('outliers_flagged',0):>6,}")

    print("\n" + "═" * W)
    print("  DATA QUALITY SCORES  (weighted composite)")
    print("─" * W)
    overall = score_stats.get("dq_mean_score", 0)
    print(f"  Overall DQ Score   {_bar(overall)}  {overall:>5.1f} / 100")
    print()

    dims = [
        ("Completeness", "dq_avg_completeness", 20,
         "Required fields present"),
        ("Validity",     "dq_avg_validity",     25,
         "Values conform to business rules"),
        ("Accuracy",     "dq_avg_accuracy",     35,
         "Mathematically & financially correct"),
        ("Consistency",  "dq_avg_consistency",  15,
         "Cross-field logic is coherent"),
        ("Uniqueness",   "dq_avg_uniqueness",    5,
         "No duplicate invoice numbers"),
    ]
    print(f"  {'Dimension':<14} {'Wt':>3}  {'Score':>5}  {'Bar':<20}  What it measures")
    print(f"  {'─'*13} {'─'*3}  {'─'*5}  {'─'*20}  {'─'*28}")
    for name, key, wt, desc in dims:
        s = score_stats.get(key, 0)
        print(f"  {name:<14} {wt:>2}%  {s:>5.1f}  {_bar(s):<20}  {desc}")

    print("\n" + "─" * W)
    print("  Severity Breakdown:")
    total = rows_out
    for sev, key in [("CLEAN",    "dq_sev_clean"),
                     ("LOW",      "dq_sev_low"),
                     ("MEDIUM",   "dq_sev_medium"),
                     ("HIGH",     "dq_sev_high"),
                     ("CRITICAL", "dq_sev_critical")]:
        n   = score_stats.get(key, 0)
        pct = f"{n/total*100:.1f}%" if total else "0%"
        icon = SEV_COLORS.get(sev, "")
        bar  = _bar(n / total * 100) if total else _bar(0)
        print(f"  {icon} {sev:<10}  {bar}  {n:>4} rows ({pct})")

    print("\n" + "─" * W)
    print(f"  Grade   A:{score_stats.get('dq_grade_A',0):>4}  "
          f"B:{score_stats.get('dq_grade_B',0):>4}  "
          f"C:{score_stats.get('dq_grade_C',0):>4}  "
          f"D:{score_stats.get('dq_grade_D',0):>4}  "
          f"F:{score_stats.get('dq_grade_F',0):>4}")
    print(f"  Lowest single score: {score_stats.get('dq_min_score','?')}  |  "
          f"Perfect rows: {score_stats.get('dq_perfect_rows',0)}")
    print("─" * W)
    print(f"  CSV → {csv_path}")
    print("═" * W + "\n")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print(BANNER)
    start = datetime.now()

    config  = load_config("config.csv")
    engine  = get_engine(config)
    df_raw  = extract_data(engine, config)
    rows_in = len(df_raw)

    logger.info("[Clean] Running 8 cleaning operations...")
    df_clean, clean_stats = run_all_cleaners(df_raw, config)

    logger.info("[Score] Computing 5-dimension DQ scores...")
    df_scored, score_stats = score_dataframe(df_clean)
    rows_out = len(df_scored)

    csv_path = export_csv(df_scored, config)
    write_to_db(df_scored, engine, config)
    append_run_log(config, clean_stats, score_stats, rows_in, rows_out)

    elapsed = (datetime.now() - start).total_seconds()
    print_summary(clean_stats, score_stats, rows_in, rows_out, csv_path)
    logger.info(f"[Done] Completed in {elapsed:.2f}s")

if __name__ == "__main__":
    main()
