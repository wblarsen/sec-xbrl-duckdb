# Contributing

## Dev setup

```bash
git clone https://github.com/wblarsen/sec-xbrl-duckdb
cd sec-xbrl-duckdb
pip install -e ".[validate]"
cp .env.example .env
# edit .env: set SEC_USER_AGENT="MyApp/1.0 (you@example.com)"
```

## Running the smoke tests

The CI suite runs without network access and without a real SEC_USER_AGENT:

```bash
# Same checks CI runs
python -c "from sec_xbrl_duckdb import STUB_REGISTRY, registry_version_hash; print(registry_version_hash(STUB_REGISTRY))"
```

## Adding a concept mapping

Open `sec_xbrl_duckdb/registry.py` and append a `ConceptMapping` to `STUB_REGISTRY`:

```python
ConceptMapping(
    output_column="operating_income",
    xbrl_concepts=("OperatingIncomeLoss",),
    statement="income_statement",
    period_type="duration",
    sign_expected="any",
    dimension_rule="prefer_null",
),
```

Rules:
- `output_column` must be unique across the registry (enforced at import time).
- List `xbrl_concepts` in preference order; `coalesce` picks the first non-null.
- Use `prefer_null` for any concept where dimensional breakdowns (segments, product lines) should lose to the consolidated total.
- Add a `plausibility_upper` when the concept has a natural ceiling (e.g. revenue should never exceed $1T for a single quarter).

## Pull requests

- One logical change per PR.
- For new concept mappings, include the SEC EDGAR taxonomy reference and at least one real ticker where the mapping was verified.
- CI must be green before merge.

## Reporting extraction errors

If a concept maps to a wrong or implausible value for a specific ticker, open a bug report with the ticker, period, and the value you expected vs. what was returned by `sec-xbrl query`.
