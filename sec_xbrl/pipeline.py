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
Orchestration: two end-to-end pipelines.

  run_companyfacts(client, conn, cik_or_ticker)
      → Fetches all XBRL facts via EDGAR's pre-aggregated companyfacts JSON.
        Best for bulk loads and full history. One HTTP call per company.

  run_filing(client, conn, cik_or_ticker, form_type, n)
      → Downloads and parses the n most-recent filings of a given form type.
        Handles both inline iXBRL (2019+) and legacy XML XBRL (2009-2018).
"""
import re
import logging
from typing import Optional
import duckdb

from .client import SECClient
from .db import upsert_company, upsert_filing, load_facts
from .parser import (
    parse_companyfacts,
    parse_inline_xbrl,
    parse_xml_xbrl,
    extract_instance_document,
)

logger = logging.getLogger(__name__)


def _resolve(client: SECClient, cik_or_ticker: str) -> str:
    """Return a zero-padded 10-digit CIK."""
    if re.fullmatch(r"\d{1,10}", cik_or_ticker):
        return cik_or_ticker.zfill(10)
    cik = client.resolve_ticker(cik_or_ticker)
    if not cik:
        raise ValueError(f"Could not resolve ticker '{cik_or_ticker}' to a CIK")
    return cik


def _company_row(submissions: dict, cik: str, ticker: Optional[str]) -> dict:
    return {
        "cik": cik,
        "name": submissions.get("name"),
        "ticker": ticker or submissions.get("tickers", [None])[0],
        "sic": submissions.get("sic"),
        "state_of_inc": submissions.get("stateOfIncorporation"),
        "fiscal_year_end": submissions.get("fiscalYearEnd"),
    }


# ---------------------------------------------------------------------------
# Pipeline A: companyfacts (all history, one call)
# ---------------------------------------------------------------------------

def run_companyfacts(
    client: SECClient,
    conn: duckdb.DuckDBPyConnection,
    cik_or_ticker: str,
) -> int:
    """Load all XBRL facts for a company via the companyfacts API. Returns fact count."""
    cik = _resolve(client, cik_or_ticker)
    ticker = cik_or_ticker if not cik_or_ticker.isdigit() else None

    logger.info("[companyfacts] CIK %s — fetching submissions…", cik)
    submissions = client.get_submissions(cik)
    upsert_company(conn, _company_row(submissions, cik, ticker))

    logger.info("[companyfacts] CIK %s — fetching all XBRL facts…", cik)
    raw = client.get_company_facts(cik)
    facts = parse_companyfacts(raw)

    return load_facts(conn, facts)


# ---------------------------------------------------------------------------
# Pipeline B: download & parse individual filings
# ---------------------------------------------------------------------------

def _get_recent_filings(submissions: dict, form_type: str, n: int) -> list[dict]:
    """Zip the columnar arrays in submissions['filings']['recent'] into dicts."""
    recent = submissions.get("filings", {}).get("recent", {})
    keys = [
        "accessionNumber", "filingDate", "reportDate", "form",
        "primaryDocument", "isXBRL", "isInlineXBRL",
    ]
    arrays = {k: recent.get(k, []) for k in keys}
    length = min(len(v) for v in arrays.values() if v)

    results = []
    for i in range(length):
        if arrays["form"][i] != form_type:
            continue
        results.append({k: (arrays[k][i] if i < len(arrays[k]) else None) for k in keys})
        if len(results) >= n:
            break
    return results


def run_filing(
    client: SECClient,
    conn: duckdb.DuckDBPyConnection,
    cik_or_ticker: str,
    form_type: str = "10-K",
    n: int = 1,
) -> int:
    """Download and parse the n most-recent filings. Returns total fact count inserted."""
    cik = _resolve(client, cik_or_ticker)
    ticker = cik_or_ticker if not cik_or_ticker.isdigit() else None

    logger.info("[filing] CIK %s — fetching submissions…", cik)
    submissions = client.get_submissions(cik)
    upsert_company(conn, _company_row(submissions, cik, ticker))

    filing_metas = _get_recent_filings(submissions, form_type, n)
    if not filing_metas:
        logger.warning("[filing] No %s filings found for CIK %s", form_type, cik)
        return 0

    total = 0
    for meta in filing_metas:
        accn = meta["accessionNumber"]
        filing_date = meta["filingDate"]
        primary_doc = meta["primaryDocument"]
        is_inline = bool(meta.get("isInlineXBRL"))

        logger.info("[filing] %s (%s) — downloading…", accn, filing_date)

        upsert_filing(conn, {
            "accession_number": accn,
            "cik": cik,
            "form_type": form_type,
            "filing_date": filing_date,
            "period_of_report": meta.get("reportDate"),
            "is_xbrl": bool(meta.get("isXBRL")),
            "is_inline_xbrl": is_inline,
            "primary_document": primary_doc,
        })

        if is_inline and primary_doc:
            url = client.filing_url(cik, accn, primary_doc)
            html = client.download_text(url)
            facts = parse_inline_xbrl(html, accn, cik, form_type, filing_date)
        else:
            # Legacy XML XBRL: pull from the complete submission bundle
            url = client.full_submission_url(cik, accn)
            txt = client.download_text(url)
            instance = extract_instance_document(txt)
            if not instance:
                logger.warning("[filing] No XBRL instance doc found in %s", accn)
                continue
            facts = parse_xml_xbrl(instance, accn, cik, form_type, filing_date)

        total += load_facts(conn, facts)

    return total
