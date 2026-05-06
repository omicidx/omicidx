/**
 * Generate a directory listing for a given R2 prefix.
 * Returns HTML if the client accepts it, otherwise JSON.
 */
export async function listDirectory(
  bucket: R2Bucket,
  prefix: string,
  acceptsHtml: boolean,
): Promise<Response> {
  const directories: string[] = [];
  const files: { key: string; size: number; lastModified: string }[] = [];

  let cursor: string | undefined;
  do {
    const listed = await bucket.list({ prefix, delimiter: "/", cursor });
    for (const obj of listed.objects) {
      files.push({
        key: obj.key,
        size: obj.size,
        lastModified: obj.uploaded.toISOString(),
      });
    }
    for (const dp of listed.delimitedPrefixes) {
      directories.push(dp);
    }
    cursor = listed.truncated ? listed.cursor : undefined;
  } while (cursor);

  if (acceptsHtml) {
    return htmlListing(prefix, directories, files);
  }

  return Response.json({ prefix, directories, files }, {
    headers: { "access-control-allow-origin": "*" },
  });
}

function htmlListing(
  prefix: string,
  directories: string[],
  files: { key: string; size: number; lastModified: string }[],
): Response {
  const title = prefix ? `/${prefix}` : "OmicIDX Data";
  const isRoot = !prefix;

  const breadcrumbs = buildBreadcrumbs(prefix);

  const parentRow = prefix
    ? `<tr><td><a href="/${prefix.replace(/[^/]+\/$/, "")}">../</a></td><td></td><td></td></tr>`
    : "";

  const dirRows = directories
    .map((d) => {
      const name = d.replace(prefix, "");
      return `<tr><td><a href="/${d}">${name}</a></td><td>&mdash;</td><td>&mdash;</td></tr>`;
    })
    .join("\n");

  const fileRows = files
    .map((f) => {
      const name = f.key.replace(prefix, "");
      return `<tr><td><a href="/${f.key}">${name}</a></td><td>${formatBytes(f.size)}</td><td>${f.lastModified.slice(0, 10)}</td></tr>`;
    })
    .join("\n");

  const hero = isRoot
    ? `<header>
        <h1>OmicIDX Data</h1>
        <p>Public genomics metadata from NCBI, available as analysis-ready Parquet and NDJSON files.
           Query directly with <a href="https://duckdb.org/">DuckDB</a>, <a href="https://pola.rs/">Polars</a>,
           or <a href="https://arrow.apache.org/docs/python/">PyArrow</a>.</p>
        <p><strong>Example:</strong></p>
        <pre><code>SELECT * FROM read_parquet('https://data.omicidx.org/sra/parquet/study.parquet') LIMIT 10;</code></pre>
      </header>`
    : `<header><h1>${title}</h1></header>`;

  const html = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>${isRoot ? "OmicIDX Data" : `OmicIDX \u2014 ${title}`}</title>
  <link rel="stylesheet" href="https://cdn.simplecss.org/simple.min.css">
  <style>
    nav { font-size: 0.9rem; }
    table { width: 100%; }
    td:nth-child(2), td:nth-child(3) { text-align: right; white-space: nowrap; width: 1%; }
    pre code { font-size: 0.85rem; }
    footer { font-size: 0.85rem; }
  </style>
</head>
<body>
  <nav>${breadcrumbs}</nav>
  <main>
    ${hero}
    <table>
      <thead>
        <tr><th>Name</th><th>Size</th><th>Modified</th></tr>
      </thead>
      <tbody>
        ${parentRow}
        ${dirRows}
        ${fileRows}
      </tbody>
    </table>
  </main>
  <footer>
    <p><a href="https://github.com/omicidx">OmicIDX</a> &mdash; Open genomics metadata infrastructure</p>
  </footer>
</body>
</html>`;

  return new Response(html, {
    headers: {
      "content-type": "text/html; charset=utf-8",
      "access-control-allow-origin": "*",
    },
  });
}

function buildBreadcrumbs(prefix: string): string {
  const parts = prefix.split("/").filter(Boolean);
  let path = "";
  const crumbs = [`<a href="/">Home</a>`];
  for (const part of parts) {
    path += part + "/";
    crumbs.push(`<a href="/${path}">${part}</a>`);
  }
  return crumbs.join(" / ");
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  const val = bytes / Math.pow(1024, i);
  return `${val.toFixed(1)} ${units[i]}`;
}
