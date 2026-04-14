# Feature Research: Bitunix + Pionex Integration

## Summary
This file captures the current integration research for adding Bitunix and Pionex support to CoinTaxman.

## Bitunix Findings
- Public API is not directly usable from a simple `requests` client: many endpoints return `403` without browser-like headers.
- Documentation is hosted under `https://www.bitunix.com/api-docs/spots/en_us/`.
- The docs are built with MkDocs and include private authenticated endpoint references.
- Identified API endpoint path patterns from the docs page HTML search, although the raw docs HTML did not expose every endpoint cleanly.

### Confirmed Bitunix API concepts
- `Public Interface`: market and configuration endpoints require no auth.
- `Private Interface`: order and account endpoints require signed API requests with API key.
- The docs mention canonical request signing and the need to apply for an API key before using private APIs.

### Relevant Bitunix endpoint patterns discovered
- Order-related endpoints under `/api/spot/v1/order/...`
- Potential tax/history-related endpoints are likely available under similar `/api/spot/v1/...` private routes.

## Pionex Findings
- Pionex public price endpoints are accessible and were already added to `src/price_data.py`.
- Private/tax API endpoint discovery remains pending.
- Pionex is Cloudflare-protected on the website, so automated scraping of web-only docs is unreliable.

## Current Codebase Status
- `src/price_data.py` has new exchange price provider methods for both Bitunix and Pionex.
- Bitget tax import is already implemented in `src/book.py`.
- Bitunix/Pionex tax endpoint integration is not yet added.
- Configuration support for Bitunix/Pionex API credentials has not been introduced.

## Next Steps
1. Find the exact authenticated endpoint and signing rules for Bitunix private APIs.
2. Locate Pionex private transaction/history API endpoints and auth requirements.
3. Add config options for Bitunix and Pionex API credentials in `src/config.py` and `config.ini`.
4. Implement new import methods in `src/book.py` for Bitunix and Pionex.
5. Wire new exchange imports into `src/main.py` if credentials are present.

## Notes
- Bitunix documentation page for signing appears broken or inaccessible via direct `/api-docs/sign/` request.
- The system may require additional browser-like headers or JS-enabled browsing to access certain docs assets.
- The current feature branch should focus on authenticated API flow once endpoints are confirmed.
