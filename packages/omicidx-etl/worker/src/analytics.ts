import type { Env } from "./index";

/**
 * Hash the first portion of a client IP for privacy-preserving analytics.
 * Returns the first 8 hex characters of the SHA-256 hash.
 */
async function hashIp(ip: string): Promise<string> {
  const data = new TextEncoder().encode(ip);
  const hash = await crypto.subtle.digest("SHA-256", data);
  const bytes = new Uint8Array(hash);
  return Array.from(bytes.slice(0, 4))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

/**
 * Log a usage data point to the Analytics Engine dataset.
 * Designed to be called via ctx.waitUntil() so it never blocks the response.
 *
 * Schema:
 *   blob1   = object key (path)
 *   blob2   = user-agent
 *   blob3   = hashed client IP
 *   blob4   = HTTP method
 *   blob5   = country (ISO 3166-1 alpha-2)
 *   blob6   = city
 *   blob7   = region
 *   blob8   = continent
 *   double1 = status code
 *   double2 = bytes transferred
 *   double3 = latitude
 *   double4 = longitude
 */
export async function logUsage(
  env: Env,
  request: Request,
  objectKey: string,
  statusCode: number,
  bytesTransferred: number,
): Promise<void> {
  const ip = request.headers.get("cf-connecting-ip") ?? "unknown";
  const hashedIp = await hashIp(ip);
  const userAgent = request.headers.get("user-agent") ?? "";

  const cf = (request as RequestInit & { cf?: IncomingRequestCfProperties }).cf;
  const country = cf?.country ?? "";
  const city = cf?.city ?? "";
  const region = cf?.region ?? "";
  const continent = cf?.continent ?? "";
  const latitude = parseFloat(cf?.latitude ?? "0") || 0;
  const longitude = parseFloat(cf?.longitude ?? "0") || 0;

  env.USAGE.writeDataPoint({
    indexes: [objectKey],
    blobs: [objectKey, userAgent, hashedIp, request.method, country, city, region, continent],
    doubles: [statusCode, bytesTransferred, latitude, longitude],
  });
}
