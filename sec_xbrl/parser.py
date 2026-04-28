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
XBRL extraction — three paths:

  1. companyfacts  — EDGAR pre-aggregated JSON (fast, covers all history)
  2. inline_xbrl   — Modern filings (2019+): facts embedded in primary HTML
  3. xml_xbrl      — Legacy filings (2009-2018): separate EX-101.INS document

All three paths produce the same XBRLFact NamedTuple.
"""
import re
import logging
from typing import Optional
from .models import XBRLFact

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Path 1: companyfacts JSON
# ---------------------------------------------------------------------------

def parse_companyfacts(data: dict) -> list[XBRLFact]:
    """
    Parse the /api/xbrl/companyfacts/{CIK}.json response into XBRLFacts.

    Each entry in the JSON already carries fiscal period, dates, and unit.
    We emit one XBRLFact per (concept, period, unit, form) row.
    """
    cik = str(data.get("cik", "")).zfill(10)
    facts: list[XBRLFact] = []

    for namespace, concepts in data.get("facts", {}).items():
        for local_name, concept_data in concepts.items():
            xbrl_tag = f"{namespace}:{local_name}"
            for unit_ref, entries in concept_data.get("units", {}).items():
                for e in entries:
                    accn = e.get("accn", "")
                    end = e.get("end")
                    start = e.get("start")
                    val = e.get("val")
                    fp = e.get("fp", "")
                    fy = e.get("fy")

                    context_ref = f"{fp}_{fy}" if fy else (end or "")

                    facts.append(XBRLFact(
                        accession_number=accn,
                        cik=cik,
                        form_type=e.get("form"),
                        filing_date=e.get("filed"),
                        context_ref=context_ref,
                        instant_date=end if not start else None,
                        period_start_date=start,
                        period_end_date=end if start else None,
                        namespace=namespace,
                        local_name=local_name,
                        xbrl_tag=xbrl_tag,
                        value_numeric=float(val) if val is not None else None,
                        value_text=None,
                        unit_ref=unit_ref,
                        decimals=None,
                        dimension_member_1=None,
                        dimension_member_2=None,
                        extraction_method="companyfacts",
                    ))

    logger.info("companyfacts: %d facts parsed for CIK %s", len(facts), cik)
    return facts


# ---------------------------------------------------------------------------
# Path 2: inline XBRL (iXBRL, 2019+)
# ---------------------------------------------------------------------------

def _extract_contexts(doc: str) -> dict[str, dict]:
    """Parse all xbrli:context elements into {id: {instant_date, period_start_date, period_end_date, dims}}."""
    contexts: dict[str, dict] = {}
    for m in re.finditer(
        r'<xbrli:context[^>]*\sid="([^"]+)"[^>]*>(.*?)</xbrli:context>',
        doc, re.DOTALL | re.IGNORECASE
    ):
        ctx_id, body = m.group(1), m.group(2)
        instant = re.search(r'<xbrli:instant>([^<]+)</xbrli:instant>', body, re.IGNORECASE)
        start = re.search(r'<xbrli:startDate>([^<]+)</xbrli:startDate>', body, re.IGNORECASE)
        end = re.search(r'<xbrli:endDate>([^<]+)</xbrli:endDate>', body, re.IGNORECASE)

        dims: list[str] = []
        for dm in re.finditer(
            r'<xbrldi:explicitMember[^>]*dimension="([^"]+)"[^>]*>([^<]+)</xbrldi:explicitMember>',
            body, re.IGNORECASE
        ):
            axis = dm.group(1).split(":")[-1]
            member = dm.group(2).strip().split(":")[-1]
            dims.append(f"{axis}={member}")

        contexts[ctx_id] = {
            "instant_date": instant.group(1).strip() if instant else None,
            "period_start_date": start.group(1).strip() if start else None,
            "period_end_date": end.group(1).strip() if end else None,
            "dims": dims,
        }
    return contexts


def _parse_attrs(attr_str: str) -> dict[str, str]:
    """Extract key=value pairs from an XML attribute string."""
    return {
        m.group(1).lower(): m.group(2)
        for m in re.finditer(r'([\w:]+)="([^"]*)"', attr_str)
    }


def parse_inline_xbrl(
    html: str,
    accession_number: str,
    cik: str,
    form_type: Optional[str] = None,
    filing_date: Optional[str] = None,
) -> list[XBRLFact]:
    """Extract facts from an iXBRL HTML document (primary 10-K/10-Q file, 2019+)."""
    contexts = _extract_contexts(html)
    facts: list[XBRLFact] = []

    # --- numeric facts ---
    for m in re.finditer(
        r'<ix:nonFraction\s+([^>]+?)>(.*?)</ix:nonFraction>',
        html, re.DOTALL | re.IGNORECASE
    ):
        attrs = _parse_attrs(m.group(1))
        raw = m.group(2).strip().replace(",", "").replace(" ", "")
        # strip nested tags (e.g. <span>)
        raw = re.sub(r'<[^>]+>', '', raw)

        name = attrs.get("name", "")
        if ":" not in name:
            continue
        namespace, local_name = name.split(":", 1)
        ctx_ref = attrs.get("contextref", "")
        ctx = contexts.get(ctx_ref, {})

        try:
            num = float(raw) if raw and raw not in ("-", "") else None
        except ValueError:
            num = None

        # Apply sign attribute from the tag (negated values)
        sign = attrs.get("sign", "")
        if sign == "-" and num is not None:
            num = -num

        # Apply scale (from decimals attribute; positive decimals = decimal places)
        try:
            decimals = int(attrs.get("decimals", "0"))
        except ValueError:
            decimals = None

        dims = ctx.get("dims", [])

        facts.append(XBRLFact(
            accession_number=accession_number,
            cik=cik,
            form_type=form_type,
            filing_date=filing_date,
            context_ref=ctx_ref,
            instant_date=ctx.get("instant_date"),
            period_start_date=ctx.get("period_start_date"),
            period_end_date=ctx.get("period_end_date"),
            namespace=namespace,
            local_name=local_name,
            xbrl_tag=name,
            value_numeric=num,
            value_text=None,
            unit_ref=attrs.get("unitref"),
            decimals=decimals,
            dimension_member_1=dims[0] if len(dims) > 0 else None,
            dimension_member_2=dims[1] if len(dims) > 1 else None,
            extraction_method="inline_xbrl",
        ))

    # --- text / non-numeric facts ---
    for m in re.finditer(
        r'<ix:nonNumeric\s+([^>]+?)>(.*?)</ix:nonNumeric>',
        html, re.DOTALL | re.IGNORECASE
    ):
        attrs = _parse_attrs(m.group(1))
        name = attrs.get("name", "")
        if ":" not in name:
            continue
        namespace, local_name = name.split(":", 1)
        ctx_ref = attrs.get("contextref", "")
        ctx = contexts.get(ctx_ref, {})
        text_val = re.sub(r'<[^>]+>', '', m.group(2)).strip()[:2000]
        dims = ctx.get("dims", [])

        facts.append(XBRLFact(
            accession_number=accession_number,
            cik=cik,
            form_type=form_type,
            filing_date=filing_date,
            context_ref=ctx_ref,
            instant_date=ctx.get("instant_date"),
            period_start_date=ctx.get("period_start_date"),
            period_end_date=ctx.get("period_end_date"),
            namespace=namespace,
            local_name=local_name,
            xbrl_tag=name,
            value_numeric=None,
            value_text=text_val or None,
            unit_ref=None,
            decimals=None,
            dimension_member_1=dims[0] if len(dims) > 0 else None,
            dimension_member_2=dims[1] if len(dims) > 1 else None,
            extraction_method="inline_xbrl",
        ))

    logger.info("inline_xbrl: %d facts for %s", len(facts), accession_number)
    return facts


# ---------------------------------------------------------------------------
# Path 3: legacy XML XBRL (EX-101.INS, 2009-2018)
# ---------------------------------------------------------------------------

def extract_instance_document(txt_content: str) -> Optional[str]:
    """Pull the EX-101.INS XML section out of the complete submission .txt file."""
    m = re.search(
        r'<DOCUMENT>\s*<TYPE>EX-101\.INS.*?<TEXT>\s*(.*?)\s*</TEXT>.*?</DOCUMENT>',
        txt_content, re.DOTALL | re.IGNORECASE
    )
    return m.group(1) if m else None


def parse_xml_xbrl(
    xml: str,
    accession_number: str,
    cik: str,
    form_type: Optional[str] = None,
    filing_date: Optional[str] = None,
) -> list[XBRLFact]:
    """
    Parse a legacy XBRL instance document (EX-101.INS).

    These are standard XBRL XML where facts appear as namespace-prefixed elements:
      <us-gaap:Revenues contextRef="D2018" decimals="-6" unitRef="USD">265595000000</us-gaap:Revenues>
    """
    contexts = _extract_contexts(xml)
    facts: list[XBRLFact] = []

    # Match namespace-qualified elements that carry contextRef (= XBRL facts)
    for m in re.finditer(
        r'<([a-zA-Z][\w-]*):([a-zA-Z][\w]+)\s+([^>]*contextRef="([^"]+)"[^>]*)>\s*([^<]*?)\s*</\1:\2>',
        xml, re.DOTALL
    ):
        namespace = m.group(1)
        local_name = m.group(2)
        attr_str = m.group(3)
        ctx_ref = m.group(4)
        raw = m.group(5).strip()

        # Skip XBRL infrastructure namespaces
        if namespace in ("xbrli", "link", "xlink", "xbrldi", "xbrldt"):
            continue

        attrs = _parse_attrs(attr_str)
        ctx = contexts.get(ctx_ref, {})

        try:
            num = float(raw) if raw else None
        except ValueError:
            num = None

        try:
            decimals = int(attrs.get("decimals", "0")) if attrs.get("decimals") else None
        except ValueError:
            decimals = None

        dims = ctx.get("dims", [])

        facts.append(XBRLFact(
            accession_number=accession_number,
            cik=cik,
            form_type=form_type,
            filing_date=filing_date,
            context_ref=ctx_ref,
            instant_date=ctx.get("instant_date"),
            period_start_date=ctx.get("period_start_date"),
            period_end_date=ctx.get("period_end_date"),
            namespace=namespace,
            local_name=local_name,
            xbrl_tag=f"{namespace}:{local_name}",
            value_numeric=num,
            value_text=raw if num is None and raw else None,
            unit_ref=attrs.get("unitref"),
            decimals=decimals,
            dimension_member_1=dims[0] if len(dims) > 0 else None,
            dimension_member_2=dims[1] if len(dims) > 1 else None,
            extraction_method="xml_xbrl",
        ))

    logger.info("xml_xbrl: %d facts for %s", len(facts), accession_number)
    return facts


# ---------------------------------------------------------------------------
# Format detection helper
# ---------------------------------------------------------------------------

def detect_xbrl_format(primary_doc: Optional[str], txt_content: str) -> str:
    """Return 'inline_xbrl', 'xml_xbrl', or 'none'."""
    if primary_doc and re.search(r'<ix:nonFraction', primary_doc, re.IGNORECASE):
        return "inline_xbrl"
    if extract_instance_document(txt_content):
        return "xml_xbrl"
    return "none"
