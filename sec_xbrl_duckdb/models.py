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
from typing import Optional, NamedTuple


class XBRLFact(NamedTuple):
    accession_number: str
    cik: str
    form_type: Optional[str]
    filing_date: Optional[str]
    context_ref: str
    instant_date: Optional[str]
    period_start_date: Optional[str]
    period_end_date: Optional[str]
    namespace: str
    local_name: str
    xbrl_tag: str
    value_numeric: Optional[float]
    value_text: Optional[str]
    unit_ref: Optional[str]
    decimals: Optional[int]
    dimension_member_1: Optional[str]
    dimension_member_2: Optional[str]
    extraction_method: str
