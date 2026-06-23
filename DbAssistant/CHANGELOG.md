# Changelog

All notable changes to **DbAssistant** are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versioning aligns
with [`VERSION`](VERSION).

## [1.0.0] - 2026-06-23

### Added

- Initial public release of DbAssistant — multi-engine database management with
  **Desktop UI**, **TUI**, **Web UI**, **CLI**, and **REST API** surfaces.
- **Data Migration** module: schema conversion, data transfer, validation,
  checkpoint/resume, and JSON reports.
- **AI Query Assistant**: NL→SQL via Claude/Cursor/Codex or a local trainable
  model; RAG Manager; local LLM train/eval/harvest.
- **Monitoring** module: DB/OS/cloud metrics, thresholds, alerts, daemon, and
  notifications.
- **App Builder**: scaffold applications from a schema, codebase, or scratch.
- Encrypted-at-rest credentials under `~/.dbassistant/`.
- Read-only SQL guard for AI execution paths.
- Documentation site (Astro + Starlight) with CLI, API, and configuration
  reference.

### Security

- Fernet encryption for connection and cloud profiles.
- REST API authentication via `X-API-Key` and optional KMS keys.
- PII masking toggle for AI prompts and RAG indexing.

[1.0.0]: https://github.com/dhananjay-chaturvedi/dbassistant/releases/tag/v1.0.0
