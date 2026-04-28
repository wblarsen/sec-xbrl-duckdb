# Security policy

## Scope

This package makes outbound HTTPS requests to `sec.gov` and `data.sec.gov`. It stores data in a local DuckDB file. It does not expose any network services, handle authentication tokens, or store credentials.

## Reporting a vulnerability

Please **do not** open a public GitHub issue for security vulnerabilities.

Email `wyattwoodsen@gmail.com` with:
- A description of the issue and its potential impact
- Steps to reproduce

You should receive a response within 7 days. If the issue is confirmed, a patched release will be cut and a GitHub Security Advisory published.

## Known considerations

- **SEC_USER_AGENT** is stored in `.env`. Do not commit this file (it is in `.gitignore`).
- DuckDB files are not encrypted. Do not store them in publicly accessible locations.
- This package does not validate TLS certificates beyond Python's default `requests` behaviour. Do not disable certificate verification.
