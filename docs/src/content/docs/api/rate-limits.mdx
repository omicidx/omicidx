---
title: Rate limits
description: Per-IP request rate limits and how to handle 429 responses.
---

import Wip from "../../../components/Wip.astro";

## Default limit

The API enforces a default rate limit of **1000 requests per minute per IP**. Reasonable interactive use stays well below this. Bulk pipelines should respect the response headers.

## Headers

Every response includes:

- `X-RateLimit-Limit` — your current limit (requests per window).
- `X-RateLimit-Remaining` — how many requests you have left in the current window.
- `X-RateLimit-Reset` — UNIX timestamp at which the window resets.

## Hitting the limit

When you exceed the limit, the API returns `429 Too Many Requests` with a `Retry-After` header (in seconds). Wait for that interval and retry.

```http
HTTP/1.1 429 Too Many Requests
Retry-After: 30
```

## Bulk pipelines

If you're scraping the entire index, prefer the [downloadable DuckDB snapshot](/overview/architecture/) (`omicidx.duckdb` on R2) over hitting the API for every record. It's faster, doesn't consume rate budget, and is the same data.

<Wip reason="Authenticated higher-rate tiers and per-route limits are under consideration." />
