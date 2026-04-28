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
DuckDB schema and data loading.

Tables:
  companies   — one row per CIK
  filings     — one row per accession number
  xbrl_facts  — XBRL financial facts (deduped by accession+tag+context)
"""
import uuid
import logging
from typing import Optional
import duckdb
from .models import XBRLFact

logger = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS companies (
    cik              VARCHAR PRIMARY KEY,
    name             VARCHAR,
    ticker           VARCHAR,
    sic              VARCHAR,
    state_of_inc     VARCHAR,
    fiscal_year_end  VARCHAR,
    updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS filings (
    accession_number     VARCHAR PRIMARY KEY,
    cik                  VARCHAR,
    form_type            VARCHAR,
    filing_date          DATE,
    period_of_report     DATE,
    is_xbrl              BOOLEAN DEFAULT FALSE,
    is_inline_xbrl       BOOLEAN DEFAULT FALSE,
    primary_document     VARCHAR,
    loaded_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS xbrl_facts (
    fact_id          VARCHAR PRIMARY KEY,
    accession_number VARCHAR NOT NULL,
    cik              VARCHAR NOT NULL,
    form_type        VARCHAR,
    filing_date      DATE,
    context_ref      VARCHAR NOT NULL,
    instant_date     DATE,
    period_start_date DATE,
    period_end_date  DATE,
    namespace        VARCHAR NOT NULL,
    local_name       VARCHAR NOT NULL,
    xbrl_tag         VARCHAR NOT NULL,
    value_numeric    DECIMAL(30, 6),
    value_text       VARCHAR,
    unit_ref         VARCHAR,
    decimals         INTEGER,
    dimension_member_1 VARCHAR,
    dimension_member_2 VARCHAR,
    extraction_method VARCHAR NOT NULL,
    UNIQUE (accession_number, xbrl_tag, context_ref)
);

CREATE INDEX IF NOT EXISTS idx_facts_cik     ON xbrl_facts (cik);
CREATE INDEX IF NOT EXISTS idx_facts_tag     ON xbrl_facts (xbrl_tag);
CREATE INDEX IF NOT EXISTS idx_facts_period  ON xbrl_facts (period_end_date);
"""


def init_db(path: str) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(path)
    conn.execute("SET memory_limit='2GB'")
    for stmt in _SCHEMA.split(";"):
        s = stmt.strip()
        if s:
            conn.execute(s)
    logger.info("Database ready at %s", path)
    return conn


def upsert_company(conn: duckdb.DuckDBPyConnection, row: dict) -> None:
    conn.execute("""
        INSERT INTO companies (cik, name, ticker, sic, state_of_inc, fiscal_year_end, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT (cik) DO UPDATE SET
            name = excluded.name,
            ticker = COALESCE(excluded.ticker, ticker),
            sic = COALESCE(excluded.sic, sic),
            state_of_inc = COALESCE(excluded.state_of_inc, state_of_inc),
            fiscal_year_end = COALESCE(excluded.fiscal_year_end, fiscal_year_end),
            updated_at = CURRENT_TIMESTAMP
    """, [
        row.get("cik"), row.get("name"), row.get("ticker"),
        row.get("sic"), row.get("state_of_inc"), row.get("fiscal_year_end"),
    ])


def upsert_filing(conn: duckdb.DuckDBPyConnection, row: dict) -> None:
    conn.execute("""
        INSERT INTO filings (
            accession_number, cik, form_type, filing_date,
            period_of_report, is_xbrl, is_inline_xbrl, primary_document
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (accession_number) DO NOTHING
    """, [
        row.get("accession_number"), row.get("cik"), row.get("form_type"),
        row.get("filing_date"), row.get("period_of_report"),
        row.get("is_xbrl", False), row.get("is_inline_xbrl", False),
        row.get("primary_document"),
    ])


def load_facts(conn: duckdb.DuckDBPyConnection, facts: list[XBRLFact]) -> int:
    """Bulk-insert facts, skipping duplicates. Returns count inserted."""
    if not facts:
        return 0

    rows = [
        (
            str(uuid.uuid4()),
            f.accession_number, f.cik, f.form_type, f.filing_date,
            f.context_ref, f.instant_date, f.period_start_date, f.period_end_date,
            f.namespace, f.local_name, f.xbrl_tag,
            f.value_numeric, f.value_text, f.unit_ref, f.decimals,
            f.dimension_member_1, f.dimension_member_2, f.extraction_method,
        )
        for f in facts
    ]

    conn.execute("BEGIN TRANSACTION")
    try:
        conn.executemany("""
            INSERT INTO xbrl_facts (
                fact_id, accession_number, cik, form_type, filing_date,
                context_ref, instant_date, period_start_date, period_end_date,
                namespace, local_name, xbrl_tag,
                value_numeric, value_text, unit_ref, decimals,
                dimension_member_1, dimension_member_2, extraction_method
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (accession_number, xbrl_tag, context_ref) DO NOTHING
        """, rows)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    inserted = conn.execute(
        "SELECT COUNT(*) FROM xbrl_facts WHERE accession_number = ?",
        [facts[0].accession_number]
    ).fetchone()[0]

    logger.info("Loaded %d facts for %s", inserted, facts[0].accession_number)
    return inserted


def query_facts(
    conn: duckdb.DuckDBPyConnection,
    cik: str,
    concept: Optional[str] = None,
    form_type: Optional[str] = None,
    limit: int = 100,
) -> list[dict]:
    """Simple read-back helper for exploration."""
    filters = ["cik = ?"]
    params: list = [cik.zfill(10)]

    if concept:
        filters.append("(local_name ILIKE ? OR xbrl_tag ILIKE ?)")
        params += [f"%{concept}%", f"%{concept}%"]
    if form_type:
        filters.append("form_type = ?")
        params.append(form_type)

    where = " AND ".join(filters)
    rows = conn.execute(f"""
        SELECT xbrl_tag, period_start_date, period_end_date, instant_date,
               value_numeric, unit_ref, form_type, filing_date
        FROM xbrl_facts
        WHERE {where}
        ORDER BY COALESCE(period_end_date, instant_date) DESC
        LIMIT ?
    """, params + [limit]).fetchall()

    cols = ["xbrl_tag", "period_start_date", "period_end_date", "instant_date",
            "value_numeric", "unit_ref", "form_type", "filing_date"]
    return [dict(zip(cols, r)) for r in rows]
