---
title: Pagination
description: Cursor-based, base64url-encoded keyset pagination — stable, fast, and offset-free.
---

OmicIDX uses **keyset cursor pagination**. No `offset` parameter; no `page=N` parameter; no degraded performance deep into a result set.

## How it works

Every paginated request returns a `links.next` URL when more results exist:

```json
{
  "data": [...],
  "meta": {
    "count": 25,
    "cursor": {
      "next": "eyJpZCI6MTIzNDV9",
      "prev": null
    }
  },
  "links": {
    "self": "https://api-omicidx.cancerdatasci.org/v1/biosample?limit=25",
    "next": "https://api-omicidx.cancerdatasci.org/v1/biosample?cursor=eyJpZCI6MTIzNDV9&limit=25",
    "prev": null
  }
}
```

To get the next page, fetch `links.next`. When you receive a response with no `next` link, you've reached the end.

## Why cursors, not offsets

- **Stable across writes.** A new row inserted between page fetches doesn't shift your cursor; offset-based pagination would skip or duplicate rows in the same situation.
- **Cheap at depth.** Page 10,000 costs the same as page 1 — the cursor is an indexed lookup, not a scan past 500,000 rows.
- **Self-describing.** The cursor encodes the keyset position; clients don't need to track state.

## What's in the cursor

The `cursor` parameter is a base64url-encoded JSON object containing the sort keys of the last row of the previous page. It's opaque to clients — don't construct or modify it. Decoding for inspection is harmless; decoding to mint your own is not supported and may break across releases.

## Limits

- `limit` parameter: default `25`, max `500`.
- Sort order is fixed per endpoint; passing arbitrary sort fields isn't supported (would invalidate the cursor encoding).

## Example

```bash
# First page
curl 'https://api-omicidx.cancerdatasci.org/v1/biosample?limit=100'

# Follow the next link from the response
curl 'https://api-omicidx.cancerdatasci.org/v1/biosample?cursor=...&limit=100'

# Stop when no `next` link is returned
```
