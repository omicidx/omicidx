# OmicIDX Data Worker

Cloudflare Worker that serves OmicIDX data files from R2 with usage analytics.

## Features

- Serves Parquet, NDJSON, and other files from the `omicidx` R2 bucket
- HTTP range request support (required for DuckDB remote Parquet queries)
- Directory listing with breadcrumb navigation (HTML for browsers, JSON for programmatic access)
- Usage analytics via Workers Analytics Engine (geolocation, bytes transferred, user-agent)
- CORS enabled for cross-origin access

## Prerequisites

- Node.js (>= 18)
- A Cloudflare account with the `omicidx` R2 bucket
- [Wrangler CLI](https://developers.cloudflare.com/workers/wrangler/) (installed as a dev dependency)

## Setup

```bash
cd worker
npm install
npx wrangler login
```

Create an Analytics Engine dataset named `omicidx_usage` in the
[Cloudflare dashboard](https://dash.cloudflare.com/) under
Account > Analytics > Analytics Engine.

## Development

```bash
npm run dev          # or: just worker-dev
```

This runs `wrangler dev --remote`, connecting to the real R2 bucket.

## Deployment

```bash
npm run deploy       # or: just worker-deploy
```

The worker will be available at `omicidx-data.<your-subdomain>.workers.dev`.
To use a custom domain (e.g., `data.omicidx.org`), configure a route in
`wrangler.toml` or add a Custom Domain in the Cloudflare dashboard.

## Analytics

Query usage data via the Analytics Engine SQL API:

```bash
curl "https://api.cloudflare.com/client/v4/accounts/<ACCOUNT_ID>/analytics_engine/sql" \
  -H "Authorization: Bearer <API_TOKEN>" \
  -d "SELECT
        blob5 AS country,
        SUM(_sample_interval) AS requests,
        SUM(_sample_interval * double2) / 1073741824 AS gb_transferred
      FROM omicidx_usage
      WHERE timestamp > NOW() - INTERVAL '30' DAY
      GROUP BY country
      ORDER BY requests DESC"
```

### Analytics schema

| Column    | Field            | Description                      |
|-----------|------------------|----------------------------------|
| `blob1`   | object_key       | File path requested              |
| `blob2`   | user_agent       | Client user-agent                |
| `blob3`   | hashed_ip        | SHA-256 prefix (privacy)         |
| `blob4`   | method           | HTTP method (GET/HEAD)           |
| `blob5`   | country          | ISO 3166-1 alpha-2 country code  |
| `blob6`   | city             | City name                        |
| `blob7`   | region           | Region/state                     |
| `blob8`   | continent        | Continent code                   |
| `double1` | status_code      | HTTP status code                 |
| `double2` | bytes_transferred| Actual bytes sent (range-aware)  |
| `double3` | latitude         | Client latitude                  |
| `double4` | longitude        | Client longitude                 |
