# Finance Data Cleaning Pipeline

A production-ready Python pipeline that connects to **SQL Server / Azure SQL**, loads financial data (invoices, payments, vendors), applies **8 automated cleaning operations**, scores every record across **5 data quality dimensions**, and delivers a cleaned CSV, a scored database table, and a **visual analytics dashboard** — all driven by a single `config.csv` file.

---

## Project Structure

```
finance_pipeline/
├── pipeline.py              # Main orchestrator — run this
├── cleaner.py               # 8 cleaning operations
├── scorer.py                # 5-dimension DQ scoring engine
├── db_connector.py          # SQL Server + SQLite demo mode connector
├── generate_sample_data.py  # Synthetic dirty-data generator (demo/testing)
├── dq_dashboard.html        # Standalone visual analytics dashboard
├── config.csv               # All pipeline parameters (no code edits needed)
├── .env.example             # Credential template — copy to .env
├── requirements.txt         # Python dependencies
└── output/                  # Generated at runtime (gitignored)
    ├── invoices_cleaned.csv
    └── pipeline_run_log.csv
```

---

## Quick Start (Demo Mode — no DB needed)

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Generate synthetic dirty finance data into a local SQLite DB
python generate_sample_data.py

# 3. Run the full pipeline
python pipeline.py

# 4. Open the dashboard
start dq_dashboard.html
```

Cleaned CSV → `output/invoices_cleaned.csv`
Run audit log → `output/pipeline_run_log.csv`

---

## Connecting to SQL Server / Azure SQL

1. Copy the credentials template:
   ```bash
   cp .env.example .env
   ```

2. Fill in your real values in `.env`:
   ```
   DB_SERVER=your-server.database.windows.net
   DB_NAME=FinanceDB
   DB_USER=your_username
   DB_PASSWORD=your_password
   DB_DRIVER=ODBC Driver 18 for SQL Server
   ```

3. Set `db_mode` to `sqlserver` in `config.csv`.

4. Run:
   ```bash
   python pipeline.py
   ```

> **Note:** Requires the [Microsoft ODBC Driver for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server).

---

## Cleaning Operations (8 total)

| # | Operation | What it does |
|---|-----------|--------------|
| 1 | **Null handling** | Fills missing strings → `UNKNOWN`, numerics → `0.0`; flags rows that had nulls |
| 2 | **Duplicate removal** | Drops duplicate rows on configurable key columns; keeps first occurrence |
| 3 | **Date normalization** | Parses all date columns to `YYYY-MM-DD`; flags unparseable values |
| 4 | **Amount validation** | Flags negative, below-minimum, and above-maximum amounts |
| 5 | **Currency standardization** | Upper-cases codes; validates against accepted ISO list |
| 6 | **Vendor name cleanup** | Strips whitespace, title-cases, replaces placeholder values |
| 7 | **Status normalization** | Strips/upper-cases status field; validates against accepted list |
| 8 | **Outlier flagging** | Z-score based detection on amount columns; configurable threshold |

---

## Data Quality Scoring Engine

Every record is scored across **five independent quality dimensions**. Each dimension produces its own 0–100 sub-score, combined into a single weighted `dq_score`.

### Dimensions & Weights

| Dimension | Weight | Question it answers | Business impact |
|-----------|--------|---------------------|-----------------|
| **Completeness** | 20% | Are all required fields populated? | Missing vendor/amount means a transaction can't be posted, reconciled, or audited |
| **Validity** | 25% | Do values conform to business rules? | Invalid currency codes block ERP payment runs; invalid statuses break workflow routing |
| **Accuracy** | 35% | Are values mathematically correct? | Negative amounts or out-of-range values cause real monetary errors downstream |
| **Consistency** | 15% | Are fields logically coherent with each other? | PAID invoice with no payment date is unreconcilable; total ≠ amount + tax is an accounting error |
| **Uniqueness** | 5% | Is each record distinct? | Duplicate invoice numbers create double-payment risk |

### Accuracy carries the highest weight (35%) because in finance, a wrong number processed downstream costs real money.

### Consistency Checks (cross-field logic)

Three checks not possible with single-column validation:

- `status = PAID` but `payment_date` is missing → **−60 pts on Consistency**
- `|total_amount − (amount + tax_amount)| > 0.02` → **−60 pts** (reconciliation failure)
- `due_date` parsed before `invoice_date` → **−40 pts** (chronologically impossible)

### Severity Classification

| Severity | Condition | Action |
|----------|-----------|--------|
| `CLEAN` | score ≥ 85 and all dimensions ≥ 70 | Publish to gold layer as-is |
| `LOW` | score < 85 | Acceptable for most analytical use |
| `MEDIUM` | score < 70 or any dimension < 60 | Review recommended |
| `HIGH` | Accuracy < 60 or Validity < 40 or score < 50 | Use with caution; flag for review |
| `CRITICAL` | Accuracy < 30 or Consistency < 40 | Do not use; manual remediation required |

### Output Columns Added to Every Row

| Column | Description |
|--------|-------------|
| `dq_score` | Weighted overall score (0–100) |
| `dq_grade` | A / B / C / D / F |
| `dq_severity` | CLEAN / LOW / MEDIUM / HIGH / CRITICAL |
| `dq_score_completeness` | Completeness dimension sub-score |
| `dq_score_validity` | Validity dimension sub-score |
| `dq_score_accuracy` | Accuracy dimension sub-score |
| `dq_score_consistency` | Consistency dimension sub-score |
| `dq_score_uniqueness` | Uniqueness dimension sub-score |
| `dq_issues` | Pipe-separated list of every failed check with dimension tag |

### Example `dq_issues` output

```
[Accuracy] Negative invoice amount — financially impossible | [Validity] Invalid currency code — blocks payment processing | [Consistency] due_date is before invoice_date (chronology impossible)
```

---

## Visual Analytics Dashboard

Open `dq_dashboard.html` in any browser — no server required.

Includes:
- **KPI cards** — overall score, total records, critical count, perfect rows
- **Dimension scores** — horizontal bar chart with weights
- **Severity donut** — CLEAN / LOW / MEDIUM / HIGH / CRITICAL distribution
- **Score histogram** — record count per score band (colour-coded by quality)
- **Top failing checks** — most frequent issues coloured by dimension

---

## Sample Pipeline Output

```
════════════════════════════════════════════════════════════════════
  PIPELINE SUMMARY
════════════════════════════════════════════════════════════════════
  Rows loaded (raw):                          320
  Rows output (cleaned):                      299
  Duplicates removed:                          21
────────────────────────────────────────────────────────────────────
  Null cells filled:                          484
  Date parse errors:                          198
  Negative amount flags:                       38
  Invalid currency codes fixed:                99
  Invalid statuses corrected:                 117
  Outliers flagged:                            22

════════════════════════════════════════════════════════════════════
  DATA QUALITY SCORES  (weighted composite)
────────────────────────────────────────────────────────────────────
  Overall DQ Score   ██████████████░░░░   77.6 / 100

  Dimension       Wt  Score  Bar                   What it measures
  ───────────── ───  ─────  ────────────────────  ────────────────────────────
  Completeness   20%   84.6  ███████████████░░░    Required fields present
  Validity       25%   47.7  █████████░░░░░░░░░    Values conform to business rules
  Accuracy       35%   91.5  ████████████████░░    Mathematically & financially correct
  Consistency    15%   80.2  ██████████████░░░░    Cross-field logic is coherent
  Uniqueness      5%   94.0  █████████████████░    No duplicate invoice numbers

  Severity:  ✅ CLEAN:77  🟡 LOW:2  🟠 MEDIUM:78  🔴 HIGH:102  🚨 CRITICAL:40
════════════════════════════════════════════════════════════════════
```

---

## Configuration Reference (`config.csv`)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `db_mode` | `demo` | `demo` (SQLite) or `sqlserver` |
| `source_table` | `invoices` | Table to load from DB |
| `cleaned_table` | `invoices_cleaned` | Table to write cleaned data to |
| `duplicate_subset` | `vendor_name;invoice_number` | Columns for duplicate detection |
| `date_columns` | `invoice_date;due_date;payment_date` | Columns to parse as dates |
| `amount_columns` | `amount;tax_amount;total_amount` | Columns to validate |
| `valid_currencies` | `USD;EUR;GBP;...` | Accepted ISO currency codes |
| `valid_statuses` | `PAID;PENDING;OVERDUE;...` | Accepted status values |
| `outlier_zscore_threshold` | `3.0` | Z-score cutoff for outlier flagging |
| `null_fill_string` | `UNKNOWN` | Fill value for missing text fields |
| `null_fill_numeric` | `0.0` | Fill value for missing numeric fields |

Credentials (`db_server`, `db_user`, `db_password`, etc.) are set as `${ENV_VAR}` placeholders in `config.csv` and resolved from your `.env` file at runtime — never hardcoded.

---

## Requirements

- Python 3.8+
- `pandas`, `numpy`, `sqlalchemy`
- `pyodbc` (SQL Server mode only)

---

## Future Scope

- **Airflow / Azure Data Factory scheduling** — run the pipeline on a daily cadence with retries and alerting
- **Expand to more tables** — `payments`, `vendors`, `purchase_orders`, `general_ledger` via config change only
- **Reconciliation layer** — cross-table validation (every PAID invoice has a matching payment record)
- **dbt integration** — use `invoices_cleaned` as a silver layer source model
- **Power BI / Grafana** — push DQ metrics to a live monitoring dashboard

---

*Built by Akshay — Data Engineer*
