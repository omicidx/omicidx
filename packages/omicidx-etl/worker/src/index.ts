import { logUsage } from "./analytics";
import { listDirectory } from "./listing";

export interface Env {
  DATA_BUCKET: R2Bucket;
  USAGE: AnalyticsEngineDataset;
}

/** Map file extensions to content types. */
function contentType(key: string): string {
  if (key.endsWith(".parquet")) return "application/vnd.apache.parquet";
  if (key.endsWith(".json") || key.endsWith(".ndjson"))
    return "application/json";
  if (key.endsWith(".gz")) return "application/gzip";
  if (key.endsWith(".csv")) return "text/csv";
  return "application/octet-stream";
}

/** Parse an HTTP Range header into an R2Range. */
function parseRange(
  header: string,
  totalSize: number,
): R2Range | undefined {
  const match = header.match(/^bytes=(\d+)-(\d*)$/);
  if (!match) return undefined;
  const start = parseInt(match[1], 10);
  const end = match[2] ? parseInt(match[2], 10) : totalSize - 1;
  return { offset: start, length: end - start + 1 };
}

const CORS_HEADERS = {
  "access-control-allow-origin": "*",
  "access-control-allow-methods": "GET, HEAD, OPTIONS",
  "access-control-allow-headers": "*",
  "access-control-expose-headers":
    "Content-Length, Content-Range, Accept-Ranges, ETag",
};

export default {
  async fetch(
    request: Request,
    env: Env,
    ctx: ExecutionContext,
  ): Promise<Response> {
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }

    if (request.method !== "GET" && request.method !== "HEAD") {
      return new Response("Method not allowed", { status: 405 });
    }

    const url = new URL(request.url);
    const key = decodeURIComponent(url.pathname.slice(1)); // strip leading /

    // Directory listing
    if (key === "" || key.endsWith("/")) {
      const acceptsHtml =
        request.headers.get("accept")?.includes("text/html") ?? false;
      const response = await listDirectory(env.DATA_BUCKET, key, acceptsHtml);
      ctx.waitUntil(logUsage(env, request, key || "/", response.status, 0));
      return response;
    }

    // Check if this is a range request — we need the object size first
    // to parse the range, so do a head-style get first if range is present.
    const rangeHeader = request.headers.get("range");

    // Build R2GetOptions
    const options: R2GetOptions = { onlyIf: request.headers };

    if (rangeHeader) {
      // We need total size to resolve open-ended ranges. Use a HEAD first
      // only if necessary (open-ended range like "bytes=8192-").
      // For simplicity, always get the object head to know total size.
      const head = await env.DATA_BUCKET.head(key);
      if (head === null) {
        ctx.waitUntil(logUsage(env, request, key, 404, 0));
        return new Response("Not found", {
          status: 404,
          headers: CORS_HEADERS,
        });
      }
      const range = parseRange(rangeHeader, head.size);
      if (range) {
        options.range = range;
      }
    }

    const object = await env.DATA_BUCKET.get(key, options);

    if (object === null) {
      ctx.waitUntil(logUsage(env, request, key, 404, 0));
      return new Response("Not found", {
        status: 404,
        headers: CORS_HEADERS,
      });
    }

    // R2 may return R2Object (no body, conditional 304) when onlyIf fails
    if (!("body" in object)) {
      const obj = object as R2Object;
      ctx.waitUntil(logUsage(env, request, key, 304, 0));
      return new Response(null, {
        status: 304,
        headers: {
          etag: obj.httpEtag,
          ...CORS_HEADERS,
        },
      });
    }

    const isRangeResponse = rangeHeader && options.range;
    const bodySize = (object.range && "length" in object.range)
      ? object.range.length
      : object.size;
    const status = isRangeResponse ? 206 : 200;

    const headers = new Headers({
      "content-type": contentType(key),
      etag: object.httpEtag,
      "accept-ranges": "bytes",
      "cache-control": "public, max-age=86400",
      ...CORS_HEADERS,
    });

    if (bodySize !== undefined) {
      headers.set("content-length", bodySize.toString());
    }

    if (isRangeResponse && options.range) {
      const r = options.range as { offset: number; length: number };
      headers.set(
        "content-range",
        `bytes ${r.offset}-${r.offset + r.length - 1}/${object.size}`,
      );
    }

    ctx.waitUntil(logUsage(env, request, key, status, bodySize ?? 0));

    return new Response(request.method === "HEAD" ? null : object.body, {
      status,
      headers,
    });
  },
} satisfies ExportedHandler<Env>;
