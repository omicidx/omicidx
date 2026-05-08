---
title: API overview
description: Base URL, authentication, response shape, and the rest of what you need to know before making a request.
---

import Wip from "../../../components/Wip.astro";

## Base URL

```
https://api-omicidx.cancerdatasci.org
```

## Authentication

None required. The API is open and read-only. Authenticated higher-rate tiers may be added later.

## Response envelope

Every collection endpoint returns a consistent envelope:

```json
{
  "data": [...],
  "meta": {
    "count": 50,
    "total": 1234567
  },
  "links": {
    "self": "...",
    "next": "..."
  },
  "relationships": { ... }
}
```

- `data` — the page of results.
- `meta.count` — number of items in `data`; `meta.total` — total matching the query (where computable).
- `links.next` — present when more pages exist; absent on the last page. See [Pagination](/api/pagination/).
- `relationships` — links to related resources (e.g., a study's samples).

Single-resource endpoints return the resource object directly without the envelope.

## Endpoints

The full list of endpoints, parameters, and response schemas is in the [API reference](/api/reference/), auto-generated from the live OpenAPI spec.

## Error responses

Standard HTTP status codes:

- `200` — success.
- `400` — invalid request (malformed cursor, unknown field).
- `404` — resource not found.
- `429` — rate limited; see [Rate limits](/api/rate-limits/).
- `503` — temporary backend issue; retry with backoff.

Error responses include a JSON body with `detail` and, where applicable, the offending field.

<Wip reason="More client-specific examples land once the Python and R clients exist." />
