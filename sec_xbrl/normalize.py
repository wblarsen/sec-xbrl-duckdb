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
Normalize raw xbrl_facts → standardized_financials using a ConceptMapping registry.

For each unique (cik, accession_number, period_end_date) in xbrl_facts, we:
  1. Gather all facts for that filing/period.
  2. Apply each ConceptMapping's dimension_rule and precedence.
  3. Write one row to standardized_financials.

The table DDL is generated from the registry, so adding new ConceptMappings
automatically extends the schema. Pass STUB_REGISTRY (or your extended registry)
to all functions here.
"""
import logging
from typing import Optional

import duckdb

from .registry import ConceptMapping, apply_dimension_rule, registry_version_hash

logger = logging.getLogger(__name__)

_FIXED_COLS = """
    cik              VARCHAR NOT NULL,
    accession_number VARCHAR NOT NULL,
    form_type        VARCHAR,
    filing_date      DATE,
    period_end_date  DATE,
    period_start_date DATE,
    registry_version VARCHAR,
    normalized_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
"""


def _ddl(registry: tuple[ConceptMapping, ...]) -> str:
    concept_cols = "\n".join(
        f"    {m.output_column}  DECIMAL(30, 6),"
        for m in registry
    )
    return f"""
CREATE TABLE IF NOT EXISTS standardized_financials (
{_FIXED_COLS},
{concept_cols}
    PRIMARY KEY (cik, accession_number, period_end_date)
);
"""


def create_standardized_table(
    conn: duckdb.DuckDBPyConnection,
    registry: tuple[ConceptMapping, ...],
) -> None:
    """Create standardized_financials with columns derived from the registry."""
    conn.execute(_ddl(registry))
    # Add any new concept columns to an existing table without recreating it
    existing = {
        row[0]
        for row in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'standardized_financials'"
        ).fetchall()
    }
    for m in registry:
        if m.output_column not in existing:
            conn.execute(
                f"ALTER TABLE standardized_financials ADD COLUMN IF NOT EXISTS "
                f"{m.output_column} DECIMAL(30,6)"
            )
            logger.info("Added column %s to standardized_financials", m.output_column)


# ---------------------------------------------------------------------------
# Core normalization
# ---------------------------------------------------------------------------

def normalize_facts(
    conn: duckdb.DuckDBPyConnection,
    registry: tuple[ConceptMapping, ...],
    cik: Optional[str] = None,
    accession_number: Optional[str] = None,
) -> int:
    """
    Normalize xbrl_facts → standardized_financials.

    Args:
        conn:             DuckDB connection (xbrl_facts + standardized_financials must exist)
        registry:         Tuple of ConceptMappings to apply
        cik:              Limit to a single CIK (zero-padded 10-digit)
        accession_number: Limit to a single filing

    Returns:
        Number of rows written to standardized_financials.
    """
    create_standardized_table(conn, registry)

    version = registry_version_hash(registry)

    # Build a WHERE clause for filtering
    filters = []
    params: list = []
    if cik:
        filters.append("cik = ?")
        params.append(cik)
    if accession_number:
        filters.append("accession_number = ?")
        params.append(accession_number)
    where = ("WHERE " + " AND ".join(filters)) if filters else ""

    # Get distinct filing periods (one canonical period_end per accession)
    periods = conn.execute(f"""
        SELECT
            cik,
            accession_number,
            MAX(form_type)  AS form_type,
            MIN(filing_date) AS filing_date,
            MAX(COALESCE(period_end_date, instant_date)) AS period_end_date,
            MIN(period_start_date) AS period_start_date
        FROM xbrl_facts
        {where}
        GROUP BY cik, accession_number
        HAVING MAX(COALESCE(period_end_date, instant_date)) IS NOT NULL
        ORDER BY cik, filing_date
    """, params).fetchall()

    if not periods:
        logger.info("No filing periods found to normalize")
        return 0

    concept_set = {c for m in registry for c in m.xbrl_concepts}
    written = 0

    for cik_val, accn, form_type, filing_date, period_end, period_start in periods:
        # Pull all facts for this filing that are referenced by the registry
        concept_placeholders = ", ".join("?" * len(concept_set))
        fact_rows = conn.execute(f"""
            SELECT local_name, namespace, value_numeric,
                   dimension_member_1, dimension_member_2,
                   COALESCE(period_end_date, instant_date) AS effective_end
            FROM xbrl_facts
            WHERE accession_number = ?
              AND cik = ?
              AND local_name IN ({concept_placeholders})
              AND value_numeric IS NOT NULL
              AND COALESCE(period_end_date, instant_date) = ?
        """, [accn, cik_val] + list(concept_set) + [period_end]).fetchall()

        candidates = [
            {
                "local_name": r[0],
                "namespace": r[1],
                "value_numeric": r[2],
                "dimension_member_1": r[3],
                "dimension_member_2": r[4],
            }
            for r in fact_rows
        ]

        if not candidates:
            # Accept facts from any period for this filing (companyfacts data may
            # spread instant vs duration across different period_end values)
            fact_rows = conn.execute(f"""
                SELECT local_name, namespace, value_numeric,
                       dimension_member_1, dimension_member_2
                FROM xbrl_facts
                WHERE accession_number = ?
                  AND cik = ?
                  AND local_name IN ({concept_placeholders})
                  AND value_numeric IS NOT NULL
            """, [accn, cik_val] + list(concept_set)).fetchall()
            candidates = [
                {
                    "local_name": r[0],
                    "namespace": r[1],
                    "value_numeric": r[2],
                    "dimension_member_1": r[3],
                    "dimension_member_2": r[4],
                }
                for r in fact_rows
            ]

        if not candidates:
            continue

        # Apply each mapping
        row_values: dict[str, Optional[float]] = {}
        for mapping in registry:
            row_values[mapping.output_column] = apply_dimension_rule(mapping, candidates)

        # Build INSERT
        concept_cols = [m.output_column for m in registry]
        all_cols = [
            "cik", "accession_number", "form_type", "filing_date",
            "period_end_date", "period_start_date", "registry_version",
        ] + concept_cols

        all_vals = [
            cik_val, accn, form_type, filing_date,
            period_end, period_start, version,
        ] + [row_values.get(c) for c in concept_cols]

        placeholders = ", ".join("?" * len(all_cols))
        col_list = ", ".join(all_cols)

        conn.execute(f"""
            INSERT OR REPLACE INTO standardized_financials ({col_list})
            VALUES ({placeholders})
        """, all_vals)
        written += 1

    conn.execute("COMMIT") if conn.in_transaction else None
    logger.info("Normalized %d rows → standardized_financials (version=%s)", written, version)
    return written


# ---------------------------------------------------------------------------
# Query helper
# ---------------------------------------------------------------------------

def query_standardized(
    conn: duckdb.DuckDBPyConnection,
    registry: tuple[ConceptMapping, ...],
    cik: str,
    form_type: Optional[str] = None,
    limit: int = 20,
) -> list[dict]:
    """Return rows from standardized_financials for exploration."""
    concept_cols = ", ".join(m.output_column for m in registry)
    filters = ["cik = ?"]
    params: list = [cik]
    if form_type:
        filters.append("form_type = ?")
        params.append(form_type)

    rows = conn.execute(f"""
        SELECT period_end_date, form_type, filing_date, {concept_cols}
        FROM standardized_financials
        WHERE {' AND '.join(filters)}
        ORDER BY period_end_date DESC
        LIMIT ?
    """, params + [limit]).fetchall()

    col_names = ["period_end_date", "form_type", "filing_date"] + [
        m.output_column for m in registry
    ]
    return [dict(zip(col_names, r)) for r in rows]
