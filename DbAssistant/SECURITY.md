# Security Policy

## Supported versions

| Version | Supported |
|---------|-----------|
| 1.0.x   | Yes       |
| < 1.0   | No        |

## Reporting a vulnerability

**Please do not open a public GitHub issue for security vulnerabilities.**

Email **[dhananjaychaturvedi93@gmail.com](mailto:dhananjaychaturvedi93@gmail.com)** with:

- A description of the issue and impact
- Steps to reproduce
- Affected surfaces (CLI, REST API, Web UI, Desktop UI, etc.)
- Your suggested fix, if any

We aim to acknowledge reports within **5 business days** and will coordinate
disclosure after a fix is available.

## Scope

In scope:

- Credential storage and encryption under `~/.dbassistant/`
- REST API and Web UI authentication (`X-API-Key`, loopback guards)
- SQL execution guards (read-only AI paths, `sql_guard`)
- SSH tunnel and cloud profile handling
- Dependency supply-chain issues in shipped requirement files

Out of scope:

- Misconfiguration by the operator (e.g. binding the API to `0.0.0.0` without
  API keys, exposing the Web UI beyond localhost without `DBTOOL_WEBUI_API_KEY`)
- Compromise of the host OS or database servers themselves
- Third-party AI CLI backends (Claude, Cursor, Codex) and cloud provider APIs

## Secure deployment guidelines

- Run the REST API and Web UI on **loopback** unless you explicitly configure
  API keys and network exposure.
- Generate API keys with `python dbtool.py apikey` and pass them as
  `X-API-Key` headers.
- Keep `config.ini`, `.env`, and `~/.dbassistant/keys/` out of version control
  (see [`.gitignore`](.gitignore)).
- Restrict CORS in production via `config.ini` / `DBTOOL_API_CORS_ORIGINS`.
- Use TLS and firewall rules when exposing any surface beyond a single machine.

## Data handling

- Database passwords and cloud secrets are encrypted at rest (Fernet) in
  `~/.dbassistant/`.
- AI prompts may include schema metadata; enable PII masking in AI settings when
  working with sensitive data.
- The AI assistant blocks mutating SQL on live connections by design; use the
  SQL Editor or Data Migration module for intentional writes.
