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
#!/usr/bin/env python3
"""
sec-xbrl — fetch SEC EDGAR filings and load XBRL data into DuckDB.

Commands:
  company    TICKER_OR_CIK   Load all XBRL facts via companyfacts API
  filing     TICKER_OR_CIK   Download and parse individual filing documents
  query      TICKER_OR_CIK   Read raw facts from DuckDB
  normalize  TICKER_OR_CIK   Normalize xbrl_facts → standardized_financials
  validate   TICKER_OR_CIK   Cross-check standardized_financials against yfinance

Examples:
  sec-xbrl company AAPL
  sec-xbrl filing AAPL --form 10-K --count 3
  sec-xbrl query AAPL --concept Revenues --form 10-K
  sec-xbrl normalize AAPL
  sec-xbrl validate AAPL
  sec-xbrl validate --report
"""
import argparse
import logging
import os
import sys
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def _build_client():
    from .client import SECClient
    ua = os.getenv("SEC_USER_AGENT", "")
    if not ua:
        sys.exit(
            "ERROR: SEC_USER_AGENT is not set.\n"
            "Add it to .env or export it: SEC_USER_AGENT='MyApp/1.0 (you@example.com)'"
        )
    return SECClient(ua)


def _build_conn():
    from .db import init_db
    path = os.getenv("DB_PATH", "./sec_xbrl_duckdb.duckdb")
    return init_db(path)


def _resolve_cik(cik_or_ticker: str) -> str:
    """Return zero-padded 10-digit CIK, resolving ticker if needed."""
    if cik_or_ticker.isdigit():
        return cik_or_ticker.zfill(10)
    client = _build_client()
    cik = client.resolve_ticker(cik_or_ticker)
    if not cik:
        sys.exit(f"ERROR: Could not resolve ticker '{cik_or_ticker}'")
    return cik


# ---------------------------------------------------------------------------
# Ingest commands
# ---------------------------------------------------------------------------

def cmd_company(args):
    from .pipeline import run_companyfacts
    client = _build_client()
    conn = _build_conn()
    count = run_companyfacts(client, conn, args.target)
    conn.close()
    print(f"Loaded {count:,} XBRL facts for {args.target}")


def cmd_filing(args):
    from .pipeline import run_filing
    client = _build_client()
    conn = _build_conn()
    count = run_filing(client, conn, args.target, form_type=args.form, n=args.count)
    conn.close()
    print(f"Loaded {count:,} XBRL facts for {args.target} ({args.form} ×{args.count})")


def cmd_query(args):
    from .db import query_facts
    conn = _build_conn()
    cik = _resolve_cik(args.target)

    rows = query_facts(conn, cik, concept=args.concept, form_type=args.form, limit=args.limit)
    conn.close()

    if not rows:
        print("No results found.")
        return

    col_w = {
        "xbrl_tag": 45, "period_start_date": 12, "period_end_date": 12,
        "instant_date": 12, "value_numeric": 18, "unit_ref": 10,
        "form_type": 8, "filing_date": 12,
    }
    header = "  ".join(k.ljust(col_w[k]) for k in col_w)
    print(header)
    print("-" * len(header))
    for row in rows:
        print("  ".join(str(row.get(k) or "").ljust(col_w[k])[:col_w[k]] for k in col_w))
    print(f"\n{len(rows)} rows (--limit {args.limit})")


# ---------------------------------------------------------------------------
# Normalize command
# ---------------------------------------------------------------------------

def cmd_normalize(args):
    from .registry import STUB_REGISTRY, materialize_to_duckdb, registry_version_hash
    from .normalize import normalize_facts, query_standardized

    conn = _build_conn()
    cik = _resolve_cik(args.target)

    registry = STUB_REGISTRY  # swap for your extended registry here
    version = registry_version_hash(registry)
    logger.info("Registry version: %s  (%d concepts)", version, len(registry))

    materialize_to_duckdb(conn, registry)
    rows = normalize_facts(conn, registry, cik=cik)

    if rows and not args.quiet:
        results = query_standardized(conn, registry, cik, limit=args.limit)
        cols = ["period_end_date", "form_type"] + [m.output_column for m in registry]
        col_w = {c: max(len(c), 12) for c in cols}
        header = "  ".join(c.ljust(col_w[c]) for c in cols)
        print(header)
        print("-" * len(header))
        for r in results:
            def _fmt(v):
                if v is None:
                    return ""
                if isinstance(v, float):
                    return f"{v:,.0f}"
                return str(v)
            print("  ".join(_fmt(r.get(c)).ljust(col_w[c])[:col_w[c]] for c in cols))

    conn.close()
    print(f"\nNormalized {rows} periods into standardized_financials (registry={version})")


# ---------------------------------------------------------------------------
# Validate command
# ---------------------------------------------------------------------------

def cmd_validate(args):
    from .validate import (
        validate_ticker, write_records, print_ticker_summary, print_report,
        ensure_log_table,
    )
    from datetime import date

    conn = _build_conn()
    ensure_log_table(conn)

    if args.report:
        print_report(conn, days=args.days)
        conn.close()
        return

    cik = _resolve_cik(args.target)
    ticker = args.target.upper() if not args.target.isdigit() else args.target

    logger.info("Validating %s (CIK %s) against yfinance…", ticker, cik)
    records = validate_ticker(
        conn, ticker, cik,
        run_date=date.today(),
        form_type=args.form or None,
        periods=args.periods,
    )

    had_flags = print_ticker_summary(ticker, records)

    if not args.dry_run:
        written = write_records(conn, records)
        logger.info("Wrote %d comparison records to xbrl_validation_log", written)

    conn.close()
    sys.exit(1 if had_flags else 0)


# ---------------------------------------------------------------------------
# CLI wiring
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Fetch SEC EDGAR XBRL data into DuckDB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # company
    p = sub.add_parser("company", help="Load all facts via companyfacts API")
    p.add_argument("target", help="Ticker (AAPL) or CIK")
    p.set_defaults(func=cmd_company)

    # filing
    p = sub.add_parser("filing", help="Download and parse filing documents")
    p.add_argument("target", help="Ticker or CIK")
    p.add_argument("--form", default="10-K", help="Form type (default: 10-K)")
    p.add_argument("--count", type=int, default=1, help="Number of filings (default: 1)")
    p.set_defaults(func=cmd_filing)

    # query
    p = sub.add_parser("query", help="Query raw xbrl_facts from DuckDB")
    p.add_argument("target", help="Ticker or CIK")
    p.add_argument("--concept", help="Filter by concept name (e.g. Revenues)")
    p.add_argument("--form", help="Filter by form type")
    p.add_argument("--limit", type=int, default=20)
    p.set_defaults(func=cmd_query)

    # normalize
    p = sub.add_parser("normalize", help="Normalize xbrl_facts → standardized_financials")
    p.add_argument("target", help="Ticker or CIK")
    p.add_argument("--quiet", action="store_true", help="Skip table printout")
    p.add_argument("--limit", type=int, default=10, help="Rows to display (default: 10)")
    p.set_defaults(func=cmd_normalize)

    # validate
    p = sub.add_parser("validate", help="Cross-check against yfinance (requires yfinance)")
    p.add_argument("target", nargs="?", help="Ticker or CIK (omit with --report)")
    p.add_argument("--form", help="Filter by form type")
    p.add_argument("--periods", type=int, default=6, help="Recent periods to compare (default: 6)")
    p.add_argument("--dry-run", action="store_true", help="Compare but don't write to DB")
    p.add_argument("--report", action="store_true", help="Print trend report (no new validation)")
    p.add_argument("--days", type=int, default=14, help="Days for --report (default: 14)")
    p.set_defaults(func=cmd_validate)

    args = parser.parse_args()

    if args.command == "validate" and not args.report and not args.target:
        parser.error("validate requires a TICKER_OR_CIK unless --report is used")

    args.func(args)


if __name__ == "__main__":
    main()
