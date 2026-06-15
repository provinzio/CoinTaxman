# agents.md

## Purpose

This file documents how coding agents and contributors should work in this repository.
It is intended as a practical guide for safe, consistent changes.

## Project structure overview

- Main entrypoint: `src/main.py`
- Core orchestration: `src/book.py`
- Exchange readers: `src/exchanges/`
- Exchange detection/reader factory: `src/exchanges/registry.py`
- Tax logic: `src/taxman.py`
- Price retrieval and persistence: `src/price_data.py`, `src/database.py`

## Exchange integration pattern

When adding or changing an exchange integration:

1. Implement exchange-specific parsing and mapping in `src/exchanges/<exchange>.py`.
2. Keep exchange detection and reader creation in `src/exchanges/registry.py`.
3. Keep `Book` focused on orchestration, not exchange-specific parsing details.
4. For API exchanges, import via `Book.import_api_records()`.
5. Preserve backward-compatible behavior whenever possible.

## Bitget API behavior

- If Bitget API credentials are configured, API import runs automatically.
- Default is to import all record groups.
- Optional filtering is supported via environment variable:
  - `BITGET_API_RECORD_TYPES=spot,future,margin,p2p`
- Unknown record types are ignored and logged as warnings.

## Coding guidelines

- Prefer small, focused changes.
- Avoid broad refactors unless explicitly requested.
- Add clear logs and warnings for skipped/unknown inputs.
- Do not remove existing behavior without a migration path.
- Keep code easy to debug (prefer explicit dispatch over dynamic magic where practical).

## Validation checklist

After code changes, run at least:

1. Static diagnostics for touched files.
2. Relevant tests (or targeted script run if tests are missing).
3. A quick manual sanity run for the changed path.

## Documentation checklist

When behavior changes:

1. Update `README.md` usage or developer notes.
2. Keep examples copy-paste ready for Linux/macOS and Windows when environment variables are involved.
3. If exchange support changed, update the supported exchanges list.
