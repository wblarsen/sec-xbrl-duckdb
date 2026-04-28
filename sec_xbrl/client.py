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
SEC EDGAR HTTP client with fair-access rate limiting.

SEC policy: max 10 requests/second with a proper User-Agent.
We target ~8 req/s (0.12s gap) to stay well inside the limit.
"""
import time
import logging
import requests
from typing import Optional

logger = logging.getLogger(__name__)

SEC_BASE = "https://www.sec.gov"
DATA_BASE = "https://data.sec.gov"
_MIN_GAP = 0.12  # seconds between requests (process-wide)
_last_t: float = 0.0


def _throttle() -> None:
    global _last_t
    gap = time.monotonic() - _last_t
    if gap < _MIN_GAP:
        time.sleep(_MIN_GAP - gap)
    _last_t = time.monotonic()


class SECClient:
    def __init__(self, user_agent: str):
        if not user_agent or "@" not in user_agent:
            raise ValueError(
                "SEC_USER_AGENT must include an email, e.g. 'MyApp/1.0 (you@example.com)'"
            )
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": user_agent,
            "Accept-Encoding": "gzip, deflate",
        })

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _get(self, url: str, **kwargs) -> requests.Response:
        _throttle()
        logger.debug("GET %s", url)
        resp = self._session.get(url, timeout=30, **kwargs)
        resp.raise_for_status()
        return resp

    # ------------------------------------------------------------------
    # Company / filing discovery
    # ------------------------------------------------------------------

    def get_submissions(self, cik: str) -> dict:
        """Return the submissions JSON for a CIK (company info + recent filings)."""
        cik_padded = cik.strip().lstrip("0").zfill(10)
        return self._get(f"{DATA_BASE}/submissions/CIK{cik_padded}.json").json()

    def get_company_facts(self, cik: str) -> dict:
        """Return the companyfacts JSON — all XBRL facts ever reported."""
        cik_padded = cik.strip().lstrip("0").zfill(10)
        return self._get(f"{DATA_BASE}/api/xbrl/companyfacts/CIK{cik_padded}.json").json()

    def resolve_ticker(self, ticker: str) -> Optional[str]:
        """Return zero-padded 10-digit CIK for a ticker, or None if not found."""
        data = self._get(f"{SEC_BASE}/files/company_tickers.json").json()
        needle = ticker.upper()
        for entry in data.values():
            if entry.get("ticker", "").upper() == needle:
                return str(entry["cik_str"]).zfill(10)
        return None

    # ------------------------------------------------------------------
    # Document download
    # ------------------------------------------------------------------

    def download_text(self, url: str) -> str:
        return self._get(url).text

    def filing_url(self, cik: str, accession_number: str, filename: str) -> str:
        """Build a direct document URL inside an EDGAR filing."""
        cik_plain = cik.strip().lstrip("0")
        acc_nodash = accession_number.replace("-", "")
        return f"{SEC_BASE}/Archives/edgar/data/{cik_plain}/{acc_nodash}/{filename}"

    def full_submission_url(self, cik: str, accession_number: str) -> str:
        """URL for the complete .txt submission bundle."""
        cik_plain = cik.strip().lstrip("0")
        acc_nodash = accession_number.replace("-", "")
        return f"{SEC_BASE}/Archives/edgar/data/{cik_plain}/{acc_nodash}/{accession_number}.txt"
