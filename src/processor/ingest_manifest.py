# src/processor/ingest_manifest.py
from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional


# -------------------------
# Low-level fingerprint
# -------------------------
@dataclass
class FileFP:
    path: str
    size: int
    mtime: float
    sha1: str


def fingerprint(path: str | Path, max_bytes: int = 1024 * 1024) -> FileFP:
    """
    Fingerprint for change-detection.
    - size + mtime
    - sha1 of first max_bytes (fast enough, good signal)
    """
    p = Path(path)
    st = p.stat()
    h = hashlib.sha1()
    with p.open("rb") as f:
        h.update(f.read(max_bytes))
    return FileFP(
        path=str(p),
        size=int(st.st_size),
        mtime=float(st.st_mtime),
        sha1=h.hexdigest(),
    )


# ---- backward-compatible alias ----
def file_fingerprint(path: str | Path, max_bytes: int = 1024 * 1024) -> FileFP:
    return fingerprint(path, max_bytes=max_bytes)


# -------------------------
# Manifest storage helpers
# -------------------------
def load_manifest(manifest_path: str | Path) -> Dict[str, dict]:
    p = Path(manifest_path)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_manifest(manifest_path: str | Path, manifest: Dict[str, dict]) -> None:
    p = Path(manifest_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")


def is_new_or_changed(fp: FileFP, manifest: Dict[str, dict]) -> bool:
    key = os.path.normpath(fp.path)
    old = manifest.get(key)
    if old is None:
        return True
    return not (
        int(old.get("size", -1)) == fp.size
        and float(old.get("mtime", -1)) == fp.mtime
        and str(old.get("sha1", "")) == fp.sha1
    )


def append_manifest(fp: FileFP, manifest: Dict[str, dict]) -> None:
    key = os.path.normpath(fp.path)
    manifest[key] = {
        "path": fp.path,
        "size": fp.size,
        "mtime": fp.mtime,
        "sha1": fp.sha1,
    }


# -------------------------
# Compatibility class (so old imports don't break)
# -------------------------
class IngestManifest:
    """
    Compatibility wrapper.
    Some older code expects:
      - IngestManifest(manifest_path)
      - .load(), .save()
      - .is_new_or_changed(fp)
      - .append(fp)
      - .data (dict)
    """

    def __init__(self, manifest_path: str | Path):
        self.manifest_path = Path(manifest_path)
        self.data: Dict[str, dict] = {}

    def load(self) -> "IngestManifest":
        self.data = load_manifest(self.manifest_path)
        return self

    def save(self) -> "IngestManifest":
        save_manifest(self.manifest_path, self.data)
        return self

    def is_new_or_changed(self, fp: FileFP) -> bool:
        return is_new_or_changed(fp, self.data)

    def append(self, fp: FileFP) -> None:
        append_manifest(fp, self.data)

    def changed_files(self, fps: List[FileFP]) -> List[FileFP]:
        return [fp for fp in fps if self.is_new_or_changed(fp)]