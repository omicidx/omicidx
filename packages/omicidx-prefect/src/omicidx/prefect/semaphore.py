"""Semaphore-file partition tracking.

Replaces Dagster's built-in partition state with marker files written to
storage. For each (namespace, key) pair we maintain a JSON file at
`{PUBLISH_ROOT}/_semaphores/{namespace}/{key}.json` containing the
completion timestamp and any caller-supplied metadata.

Flows check `exists()` before running work for a partition and call
`mark_done()` once the partition output is durably written. Idempotent
re-runs become "skip if semaphore exists"; backfills become "clear the
semaphores you want to redo, then re-run".

The semaphore namespace mirrors the path layout (e.g., `sra/study`,
`geo/gse`, `pubmed`, `ebi_biosample`) so they're easy to enumerate.
"""

import json
from datetime import UTC, datetime

import orjson
from omicidx.prefect.config import get_upath


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


class SemaphoreStore:
    """File-backed completion markers for a single partition namespace."""

    def __init__(self, namespace: str) -> None:
        cleaned = namespace.strip("/")
        if not cleaned:
            raise ValueError("Namespace cannot be empty")
        self.namespace = cleaned

    def _path(self, key: str):
        if not key or "/" in key:
            raise ValueError(f"Invalid partition key {key!r} (no slashes allowed)")
        return get_upath("_semaphores", *self.namespace.split("/"), f"{key}.json")

    def exists(self, key: str) -> bool:
        return self._path(key).exists()

    def mark_done(self, key: str, metadata: dict | None = None) -> None:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "namespace": self.namespace,
            "key": key,
            "completed_at": _now_iso(),
            "metadata": metadata or {},
        }
        with path.open("wb") as f:
            f.write(orjson.dumps(payload, option=orjson.OPT_INDENT_2))

    def read(self, key: str) -> dict | None:
        path = self._path(key)
        if not path.exists():
            return None
        with path.open("rb") as f:
            return json.loads(f.read())

    def clear(self, key: str) -> bool:
        path = self._path(key)
        if path.exists():
            path.unlink()
            return True
        return False

    def list_keys(self) -> list[str]:
        """Return the partition keys that have been marked done."""
        root = get_upath("_semaphores", *self.namespace.split("/"))
        if not root.exists():
            return []
        return sorted(p.stem for p in root.iterdir() if p.suffix == ".json")

    def clear_all(self) -> int:
        """Remove every semaphore in this namespace. Returns count removed."""
        keys = self.list_keys()
        for k in keys:
            self.clear(k)
        return len(keys)
