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
from .models import XBRLFact
from .client import SECClient
from .db import init_db
from .registry import ConceptMapping, STUB_REGISTRY, registry_version_hash, materialize_to_duckdb
from .normalize import normalize_facts, create_standardized_table
from .pipeline import run_companyfacts, run_filing

__all__ = [
    "XBRLFact",
    "SECClient",
    "init_db",
    "ConceptMapping",
    "STUB_REGISTRY",
    "registry_version_hash",
    "materialize_to_duckdb",
    "normalize_facts",
    "create_standardized_table",
    "run_companyfacts",
    "run_filing",
]
