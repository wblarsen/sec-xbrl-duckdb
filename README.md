# sec-xbrl-duckdb

A pip-installable Python plugin that fetches SEC EDGAR filings, extracts XBRL financial data, and loads it into a local DuckDB database.

## Features

- **Two ingestion paths**
  - `company` — EDGAR's pre-aggregated [companyfacts API](https://data.sec.gov/api/xbrl/companyfacts/) (one HTTP call, all history)
  - `filing` — downloads and parses actual filing documents (inline iXBRL for 2019+, legacy XML XBRL for 2009–2018)
- **Concept registry** — `ConceptMapping` dataclass maps standardized column names to ordered XBRL taxonomy tags with dimension rules and plausibility bounds
- **Normalization** — applies the registry to raw `xbrl_facts` → `standardized_financials`
- **yfinance validation** — cross-checks normalized output against yfinance's independent pipeline
- **Local DuckDB** — all data lands in a single portable `.duckdb` file

## Installation

```bash
pip install sec-xbrl-duckdb

# With yfinance validation support
pip install "sec-xbrl-duckdb[validate]"
```

## Quick start

```bash
# 1. Configure (copy and edit)
cp .env.example .env
# Set SEC_USER_AGENT="MyApp/1.0 (you@example.com)"

# 2. Load all XBRL facts for a company (fast — one API call)
sec-xbrl company AAPL

# 3. Normalize into standardized_financials
sec-xbrl normalize AAPL

# 4. Validate against yfinance
sec-xbrl validate AAPL

# 5. Query raw facts
sec-xbrl query AAPL --concept Revenues --form 10-K
```

## Commands

| Command | Description |
|---------|-------------|
| `sec-xbrl company TICKER` | Load all XBRL facts via companyfacts API |
| `sec-xbrl filing TICKER [--form 10-K] [--count N]` | Download and parse individual filings |
| `sec-xbrl query TICKER [--concept X] [--form 10-K]` | Query raw xbrl_facts |
| `sec-xbrl normalize TICKER` | Normalize xbrl_facts → standardized_financials |
| `sec-xbrl validate TICKER [--dry-run]` | Cross-check against yfinance |
| `sec-xbrl validate --report [--days 30]` | Print metric-level validation summary |

## Configuration

| Variable | Description |
|----------|-------------|
| `SEC_USER_AGENT` | **Required.** Format: `"AppName/1.0 (you@example.com)"` — SEC blocks requests without this |
| `DB_PATH` | Path to DuckDB file (default: `./sec_xbrl_duckdb.duckdb`) |

## Schema

```
companies            — one row per CIK
filings              — one row per accession number
xbrl_facts           — raw XBRL facts (deduped by accession + tag + context)
standardized_financials — normalized output (one row per filing period)
xbrl_validation_log  — yfinance comparison results
concept_registry_meta — concept mappings materialized for SQL introspection
```

## Extending the registry

The `STUB_REGISTRY` ships 5 example concepts. Add your own:

```python
from sec_xbrl_duckdb import ConceptMapping, STUB_REGISTRY, normalize_facts

MY_REGISTRY = STUB_REGISTRY + (
    ConceptMapping(
        output_column="operating_income",
        xbrl_concepts=("OperatingIncomeLoss",),
        statement="income_statement",
        period_type="duration",
        sign_expected="any",
        dimension_rule="prefer_null",
    ),
    ConceptMapping(
        output_column="capex",
        xbrl_concepts=("PaymentsToAcquirePropertyPlantAndEquipment",),
        statement="cash_flow",
        period_type="duration",
        sign_expected="positive",
        dimension_rule="prefer_null",
    ),
)

normalize_facts(conn, MY_REGISTRY, cik="0000320193")
```

The `normalize_facts` function generates the `standardized_financials` DDL from the registry — adding a new `ConceptMapping` automatically adds the corresponding column.

## XBRL format support

| Format | Era | Detection |
|--------|-----|-----------|
| Inline iXBRL | 2019+ | `<ix:nonFraction>` in primary HTML |
| Legacy XML (EX-101.INS) | 2009–2018 | Extracted from complete submission `.txt` bundle |
| companyfacts JSON | All history | Pre-aggregated by EDGAR |

## SEC fair-access policy

SEC allows up to 10 requests/second with a proper `User-Agent` header. This library targets ~8 req/s (0.12s gap) enforced globally. See [EDGAR access guidelines](https://www.sec.gov/os/accessing-edgar-data).

## License

GPL-3.0-or-later. See [LICENSE](LICENSE).
