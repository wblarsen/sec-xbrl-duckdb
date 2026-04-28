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
XBRL Concept Registry — maps standardized output columns to raw XBRL taxonomy tags.

Design:
  - ConceptMapping is a frozen dataclass: immutable, hashable, diffable.
  - STUB_REGISTRY ships ~5 obvious, unambiguous concepts as working examples.
  - Pass a larger registry (e.g. your extended private registry) to normalize_facts()
    and materialize_to_duckdb() — the registry is a parameter, not a global.
  - registry_version_hash() stamps every standardized_financials row so you can detect
    which rows need re-normalization after a registry change.
"""

import hashlib
import logging
from dataclasses import dataclass
from typing import Optional

import duckdb

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# ConceptMapping dataclass
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ConceptMapping:
    """Maps one standardized output column to its XBRL source concepts."""

    output_column: str
    """Standardized column name produced in standardized_financials (e.g. 'revenue')."""

    xbrl_concepts: tuple[str, ...]
    """US GAAP taxonomy local_name values, in priority order."""

    precedence: str = "coalesce"
    """How to combine when multiple concepts match:
       - 'coalesce': first non-null in listed order (most common)
       - 'max':      largest value across alternatives
       - 'sum':      sum all matching values
    """

    statement: str = "income_statement"
    """Source financial statement: 'income_statement', 'balance_sheet', 'cash_flow', 'cover'."""

    period_type: str = "duration"
    """XBRL period type: 'duration' (income/CF) or 'instant' (balance sheet)."""

    sign_expected: str = "any"
    """Expected sign for plausibility checks: 'positive', 'negative', 'any'."""

    dimension_rule: str = "prefer_null"
    """How to handle dimensional members:
       - 'prefer_null': prefer undimensioned facts (consolidated totals), fall back to any
       - 'mapped_then_null': check a per-ticker share-class mapping first, then prefer_null
       - 'aggregate': SUM across a declared set of dimension members
    """

    scale_check_partner: Optional[str] = None
    """Another output_column whose value this should not exceed (sanity check)."""

    plausibility_upper: Optional[float] = None
    """Per-concept ceiling (absolute value). None falls back to global default (1e14)."""

    plausibility_lower: Optional[float] = None
    """Per-concept floor. None falls back to global default (-1e14)."""


# ---------------------------------------------------------------------------
# Dimension rule engine
# ---------------------------------------------------------------------------

def apply_dimension_rule(
    mapping: ConceptMapping,
    candidates: list[dict],
) -> Optional[float]:
    """
    Apply the mapping's dimension_rule to a list of candidate fact dicts.

    Each dict must have keys: local_name, namespace, value_numeric,
    dimension_member_1, dimension_member_2.

    Returns the winning numeric value, or None if no candidates qualify.
    """
    # Filter to concepts declared in this mapping
    concept_set = set(mapping.xbrl_concepts)
    relevant = [c for c in candidates if c.get("local_name") in concept_set]
    if not relevant:
        return None

    if mapping.dimension_rule == "prefer_null":
        return _prefer_null(mapping, relevant)

    if mapping.dimension_rule == "mapped_then_null":
        # Simplified: fall through to prefer_null when no share-class table is present.
        # Replace this branch in your extended registry with a lookup into
        # ticker_share_class_mapping before falling back.
        return _prefer_null(mapping, relevant)

    if mapping.dimension_rule == "aggregate":
        # Sum across all matching dimension members (must all be in concept_set)
        vals = [c["value_numeric"] for c in relevant if c.get("value_numeric") is not None]
        return sum(vals) if vals else None

    return _prefer_null(mapping, relevant)


def _prefer_null(mapping: ConceptMapping, candidates: list[dict]) -> Optional[float]:
    """Coalesce through xbrl_concepts in declared order, preferring undimensioned facts."""
    # Build lookup: {local_name: [undim_vals, any_vals]}
    by_concept: dict[str, tuple[list, list]] = {}
    for c in candidates:
        name = c.get("local_name", "")
        val = c.get("value_numeric")
        if val is None:
            continue
        undim = c.get("dimension_member_1") is None
        if name not in by_concept:
            by_concept[name] = ([], [])
        (by_concept[name][0] if undim else by_concept[name][1]).append(val)

    if mapping.precedence == "coalesce":
        for concept in mapping.xbrl_concepts:
            if concept not in by_concept:
                continue
            undim_vals, any_vals = by_concept[concept]
            if undim_vals:
                return undim_vals[0]
            if any_vals:
                return any_vals[0]
        return None

    if mapping.precedence == "max":
        all_vals = []
        for undim_vals, any_vals in by_concept.values():
            all_vals.extend(undim_vals or any_vals)
        return max(all_vals) if all_vals else None

    if mapping.precedence == "sum":
        all_vals = []
        for undim_vals, any_vals in by_concept.values():
            all_vals.extend(undim_vals or any_vals)
        return sum(all_vals) if all_vals else None

    return None


# ---------------------------------------------------------------------------
# Stub registry — ~5 obvious, unambiguous concepts (safe to publish)
#
# These cover the most commonly-used financial metrics with well-established
# XBRL tag names. Extend with your own ConceptMappings as needed.
# ---------------------------------------------------------------------------

STUB_REGISTRY: tuple[ConceptMapping, ...] = (

    # ── Income statement ───────────────────────────────────────────────────

    ConceptMapping(
        output_column="revenue",
        xbrl_concepts=(
            "Revenues",
            "RevenueFromContractWithCustomerExcludingAssessedTax",
            "RevenueFromContractWithCustomerIncludingAssessedTax",
            "SalesRevenueNet",
        ),
        precedence="coalesce",
        statement="income_statement",
        period_type="duration",
        sign_expected="positive",
        dimension_rule="prefer_null",
        plausibility_upper=1e12,    # $1T ceiling (6× Walmart quarterly)
    ),

    ConceptMapping(
        output_column="net_income",
        xbrl_concepts=(
            "NetIncomeLoss",
            "ProfitLoss",
        ),
        precedence="coalesce",
        statement="income_statement",
        period_type="duration",
        sign_expected="any",
        dimension_rule="prefer_null",
        plausibility_upper=5e11,    # $500B
        plausibility_lower=-5e11,
    ),

    ConceptMapping(
        output_column="operating_cash_flow",
        xbrl_concepts=("NetCashProvidedByUsedInOperatingActivities",),
        statement="cash_flow",
        period_type="duration",
        sign_expected="any",
        dimension_rule="prefer_null",
    ),

    # ── Balance sheet ──────────────────────────────────────────────────────

    ConceptMapping(
        output_column="total_assets",
        xbrl_concepts=("Assets",),
        statement="balance_sheet",
        period_type="instant",
        sign_expected="positive",
        dimension_rule="prefer_null",
        plausibility_upper=5e12,    # $5T ceiling (above any real total assets)
    ),

    ConceptMapping(
        output_column="cash",
        xbrl_concepts=(
            "CashAndCashEquivalentsAtCarryingValue",
            # Post-ASC 230 (2016): broader concept includes restricted cash
            "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
        ),
        precedence="coalesce",
        statement="balance_sheet",
        period_type="instant",
        sign_expected="positive",
        dimension_rule="prefer_null",
        scale_check_partner="total_assets",
    ),
)

# Validate no duplicate output columns at import time
_seen_cols: set[str] = set()
for _m in STUB_REGISTRY:
    if _m.output_column in _seen_cols:
        raise ValueError(f"Duplicate output_column in STUB_REGISTRY: {_m.output_column}")
    _seen_cols.add(_m.output_column)
del _seen_cols, _m


# ---------------------------------------------------------------------------
# Registry helpers
# ---------------------------------------------------------------------------

def get_all_xbrl_concepts(registry: tuple[ConceptMapping, ...]) -> set[str]:
    """Return every XBRL concept local_name referenced by this registry."""
    return {concept for m in registry for concept in m.xbrl_concepts}


def registry_version_hash(registry: tuple[ConceptMapping, ...]) -> str:
    """8-char SHA-256 fingerprint of the registry. Stamps every standardized row."""
    parts = [
        f"{m.output_column}|{','.join(m.xbrl_concepts)}|"
        f"{m.precedence}|{m.dimension_rule}|{m.period_type}|"
        f"{m.plausibility_upper}|{m.plausibility_lower}"
        for m in registry
    ]
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()[:8]


# ---------------------------------------------------------------------------
# DuckDB materialization
# ---------------------------------------------------------------------------

_META_DDL = """
CREATE TABLE IF NOT EXISTS concept_registry_meta (
    output_column    VARCHAR PRIMARY KEY,
    xbrl_concepts    VARCHAR[],
    precedence       VARCHAR,
    statement        VARCHAR,
    period_type      VARCHAR,
    sign_expected    VARCHAR,
    dimension_rule   VARCHAR,
    scale_check_partner VARCHAR,
    plausibility_upper DOUBLE,
    plausibility_lower DOUBLE
);
"""


def materialize_to_duckdb(
    conn: duckdb.DuckDBPyConnection,
    registry: tuple[ConceptMapping, ...],
) -> int:
    """Write registry metadata to concept_registry_meta for SQL-side introspection."""
    conn.execute(_META_DDL)
    conn.execute("DELETE FROM concept_registry_meta")
    rows = [
        (
            m.output_column,
            list(m.xbrl_concepts),
            m.precedence,
            m.statement,
            m.period_type,
            m.sign_expected,
            m.dimension_rule,
            m.scale_check_partner,
            m.plausibility_upper,
            m.plausibility_lower,
        )
        for m in registry
    ]
    conn.executemany(
        """
        INSERT INTO concept_registry_meta
            (output_column, xbrl_concepts, precedence, statement, period_type,
             sign_expected, dimension_rule, scale_check_partner,
             plausibility_upper, plausibility_lower)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    logger.info("Materialized %d concept mappings to concept_registry_meta", len(rows))
    return len(rows)
