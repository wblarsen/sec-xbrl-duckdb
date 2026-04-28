# sec-xbrl-duckdb — SEC EDGAR XBRL ingestion into DuckDB
# Copyright (C) 2026  wblarsen
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.
"""
Validate standardized_financials against yfinance's independently-processed data.

yfinance pulls from the same SEC XBRL source through a different pipeline, making
it a useful cross-check for extraction bugs, scale errors, and concept mapping issues.

Results are written to xbrl_validation_log and can be queried with --report.

Requires: pip install yfinance pandas

Usage (via CLI):
    sec-xbrl validate AAPL
    sec-xbrl validate AAPL --form 10-Q --periods 4
    sec-xbrl validate AAPL --dry-run
    sec-xbrl validate --report
    sec-xbrl validate --report --days 30
"""
import logging
import math
from datetime import date
from typing import Optional

import duckdb

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# yfinance → standardized_financials column mappings
#
# Format: yf_key → (our_col, tolerance, use_abs)
#   tolerance: max acceptable |pct_diff| (0.05 = 5%)
#   use_abs:   True when yfinance reports opposite sign (e.g. capex is negative in yf)
# ---------------------------------------------------------------------------

INCOME_MAP: dict[str, tuple[str, float, bool]] = {
    "Total Revenue":    ("revenue",    0.05, False),
    "Net Income":       ("net_income", 0.10, False),
}

BALANCE_MAP: dict[str, tuple[str, float, bool]] = {
    "Total Assets":               ("total_assets", 0.02, False),
    "Cash And Cash Equivalents":  ("cash",         0.03, False),
}

CASHFLOW_MAP: dict[str, tuple[str, float, bool]] = {
    "Operating Cash Flow": ("operating_cash_flow", 0.10, False),
}

# Skip comparison when the reference value is below this (% diff is meaningless for tiny values)
_MIN_VALUE = 1_000_000


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_LOG_DDL = """
CREATE TABLE IF NOT EXISTS xbrl_validation_log (
    run_date        DATE    NOT NULL,
    ticker          VARCHAR NOT NULL,
    cik             VARCHAR,
    period_end_date DATE,
    yf_date         DATE,
    metric          VARCHAR NOT NULL,
    our_value       DOUBLE,
    yf_value        DOUBLE,
    pct_diff        DOUBLE,
    tolerance       DOUBLE,
    is_pass         BOOLEAN,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


def ensure_log_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(_LOG_DDL)


# ---------------------------------------------------------------------------
# yfinance fetch
# ---------------------------------------------------------------------------

def _fetch_yfinance(ticker: str) -> dict:
    """Return {income, balance, cashflow} DataFrames from yfinance, or {} on failure."""
    try:
        import yfinance as yf  # optional dependency
        yt = yf.Ticker(ticker)
        return {
            "income":   yt.quarterly_income_stmt,
            "balance":  yt.quarterly_balance_sheet,
            "cashflow": yt.quarterly_cashflow,
        }
    except ImportError:
        raise ImportError("yfinance is required for validation: pip install yfinance pandas")
    except Exception as e:
        logger.debug("%s: yfinance fetch failed — %s", ticker, e)
        return {}


def _find_matching_date(target: date, available, max_days: int = 20):
    """Return the closest timestamp in available to target, or None if >max_days away."""
    import pandas as pd
    valid = [d for d in available if hasattr(d, "year") and 1900 <= d.year <= 2100]
    if not valid:
        return None
    target_ts = pd.Timestamp(target)
    best = min(valid, key=lambda d: abs((d - target_ts).days))
    return best if abs((best - target_ts).days) <= max_days else None


def _safe_float(v) -> Optional[float]:
    try:
        f = float(v)
        return None if math.isnan(f) or math.isinf(f) else f
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------

def _compare(
    ours: Optional[float],
    theirs: Optional[float],
    use_abs: bool,
    tolerance: float,
) -> tuple[Optional[bool], Optional[float]]:
    if ours is None or theirs is None:
        return None, None
    if use_abs:
        ours = abs(ours)
        theirs = abs(theirs)
    if abs(theirs) < _MIN_VALUE:
        return None, None
    pct = abs(ours - theirs) / abs(theirs)
    return pct <= tolerance, pct


# ---------------------------------------------------------------------------
# Per-ticker validation (no DB writes — safe for parallel use)
# ---------------------------------------------------------------------------

def validate_ticker_rows(
    ticker: str,
    sf_rows,        # pandas DataFrame
    cik: Optional[str],
    run_date: date,
) -> list[dict]:
    """
    Compare standardized_financials rows against yfinance for one ticker.
    Returns a list of comparison dicts (no DB access here).
    """
    import pandas as pd
    yf_data = _fetch_yfinance(ticker)
    if not yf_data:
        return []

    all_maps = [
        ("income",   INCOME_MAP),
        ("balance",  BALANCE_MAP),
        ("cashflow", CASHFLOW_MAP),
    ]

    records = []
    for _, sf_row in sf_rows.iterrows():
        sf_date = sf_row.get("period_end_date")
        if sf_date is None or pd.isna(sf_date):
            continue

        for source_key, mapping in all_maps:
            df = yf_data.get(source_key)
            if df is None or df.empty:
                continue

            matched_date = _find_matching_date(sf_date, list(df.columns))
            if matched_date is None:
                continue

            for yf_key, (sf_col, tol, use_abs) in mapping.items():
                if yf_key not in df.index or sf_col not in sf_row:
                    continue
                yf_val  = _safe_float(df.loc[yf_key, matched_date])
                our_val = _safe_float(sf_row.get(sf_col))

                is_pass, pct_diff = _compare(our_val, yf_val, use_abs, tol)
                records.append({
                    "run_date":        run_date,
                    "ticker":          ticker,
                    "cik":             cik,
                    "period_end_date": sf_date,
                    "yf_date":         matched_date.date() if matched_date else None,
                    "metric":          sf_col,
                    "our_value":       our_val,
                    "yf_value":        yf_val,
                    "pct_diff":        pct_diff,
                    "tolerance":       tol,
                    "is_pass":         is_pass,
                })

    return records


def validate_ticker(
    conn: duckdb.DuckDBPyConnection,
    ticker: str,
    cik: str,
    run_date: Optional[date] = None,
    form_type: Optional[str] = None,
    periods: int = 6,
) -> list[dict]:
    """
    Full validate-one-ticker flow: fetch SF data from DB, compare to yfinance.

    Returns comparison records (call write_records to persist them).
    """
    import pandas as pd
    run_date = run_date or date.today()

    # Pull the standardized columns that appear in our comparison maps
    sf_cols = {col for _, (col, _, _) in {**INCOME_MAP, **BALANCE_MAP, **CASHFLOW_MAP}.items()}
    # Intersect with columns that actually exist in the table
    existing_cols = {
        r[0]
        for r in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'standardized_financials'"
        ).fetchall()
    }
    select_cols = ", ".join(["period_end_date", "form_type"] + sorted(sf_cols & existing_cols))

    filters = ["cik = ?"]
    params: list = [cik]
    if form_type:
        filters.append("form_type = ?")
        params.append(form_type)

    sf = conn.execute(f"""
        SELECT {select_cols}
        FROM standardized_financials
        WHERE {' AND '.join(filters)}
          AND period_end_date IS NOT NULL
        ORDER BY period_end_date DESC
        LIMIT ?
    """, params + [periods]).fetchdf()

    if sf.empty:
        logger.warning("%s: no rows in standardized_financials for CIK %s", ticker, cik)
        return []

    return validate_ticker_rows(ticker, sf, cik, run_date)


# ---------------------------------------------------------------------------
# Write results
# ---------------------------------------------------------------------------

def write_records(conn: duckdb.DuckDBPyConnection, records: list[dict]) -> int:
    """Persist comparison records to xbrl_validation_log."""
    if not records:
        return 0
    import pandas as pd
    ensure_log_table(conn)
    df = pd.DataFrame(records)
    conn.register("_val_staging", df)
    conn.execute("""
        INSERT INTO xbrl_validation_log
            (run_date, ticker, cik, period_end_date, yf_date,
             metric, our_value, yf_value, pct_diff, tolerance, is_pass)
        SELECT run_date, ticker, cik, period_end_date, yf_date,
               metric, our_value, yf_value, pct_diff, tolerance, is_pass
        FROM _val_staging
    """)
    conn.unregister("_val_staging")
    return len(records)


# ---------------------------------------------------------------------------
# Summary helpers
# ---------------------------------------------------------------------------

def print_ticker_summary(ticker: str, records: list[dict]) -> bool:
    """Log per-ticker pass/fail summary. Returns True if any flags."""
    comparable = [r for r in records if r["is_pass"] is not None]
    if not comparable:
        logger.info("  %s: no comparable data", ticker)
        return False

    flagged = [r for r in comparable if not r["is_pass"]]
    status = "PASS" if not flagged else f"FLAG ({len(flagged)}/{len(comparable)})"
    logger.info("  %s: %s", ticker, status)
    for f in flagged:
        pct = f["pct_diff"] * 100 if f["pct_diff"] is not None else 0
        logger.warning(
            "    [%s] %s: ours=%.0f  yf=%.0f  diff=%.1f%%  (tol=%.0f%%)",
            f["period_end_date"],
            f["metric"],
            f["our_value"] or 0,
            f["yf_value"] or 0,
            pct,
            f["tolerance"] * 100,
        )
    return bool(flagged)


def print_report(conn: duckdb.DuckDBPyConnection, days: int = 14) -> None:
    """Print a metric-level summary of recent validation runs."""
    ensure_log_table(conn)
    rows = conn.execute(f"""
        SELECT
            metric,
            COUNT(*)        FILTER (WHERE is_pass IS NOT NULL) AS checks,
            COUNT(*)        FILTER (WHERE is_pass = TRUE)      AS passed,
            COUNT(*)        FILTER (WHERE is_pass = FALSE)     AS flagged,
            AVG(pct_diff)   FILTER (WHERE is_pass IS NOT NULL) * 100 AS avg_pct_diff,
            MAX(pct_diff)                                       * 100 AS max_pct_diff
        FROM xbrl_validation_log
        WHERE run_date >= CURRENT_DATE - INTERVAL {days} DAYS
          AND is_pass IS NOT NULL
        GROUP BY metric
        ORDER BY flagged DESC, avg_pct_diff DESC
    """).fetchall()

    if not rows:
        print(f"No validation data in the last {days} days.")
        return

    totals = conn.execute(f"""
        SELECT COUNT(DISTINCT run_date), COUNT(DISTINCT ticker)
        FROM xbrl_validation_log
        WHERE run_date >= CURRENT_DATE - INTERVAL {days} DAYS
    """).fetchone()

    print(f"\n=== yfinance validation report (last {days} days) ===")
    print(f"    Runs: {totals[0]}   Tickers checked: {totals[1]}")
    print(f"\n{'Metric':<26} {'Checks':>7} {'Pass':>6} {'Flag':>6} {'AvgDiff':>9} {'MaxDiff':>9}")
    print("-" * 64)
    for metric, checks, passed, flagged, avg_diff, max_diff in rows:
        flag_rate = flagged / checks * 100 if checks else 0
        avg_str = f"{avg_diff:.1f}%" if avg_diff is not None else "n/a"
        max_str = f"{max_diff:.1f}%" if max_diff is not None else "n/a"
        marker = " ◄" if flag_rate > 20 else ""
        print(
            f"{metric:<26} {checks:>7} {passed:>6} {flagged:>6} "
            f"{avg_str:>9} {max_str:>9}{marker}"
        )
