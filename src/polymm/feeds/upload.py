"""Hourly upload of closed hours to the bucket, and the laptop's fetch back (D-042).

   data/<layer>/<table>/dt=D/hour=H/<files>   ──►  s3://<bucket>/<layer>/<table>/dt=D/hour=H/<files>

Why the same path on both sides: `pl.scan_parquet("s3://bucket/feeds/tables/x/dt=*/…")` and
the local scan are the same code with a different root (DATA_PLAN §6).
Why a manifest per hour (`.uploaded.json`): the upload is resumable and auditable; a file is
listed only after the bucket confirmed its size, with the local sha256 beside it. Prune
(feeds layer only) deletes an hour only when every file in it is in the manifest.
Why boto3 and not rsync/rclone: one client, in the repo, also used by `fetch`; multipart
and retries come with it.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .spec import read_env_file

log = logging.getLogger(__name__)

MANIFEST = ".uploaded.json"
DEFAULT_LAYERS = ("raw", "events", "spot", "reference", "backfill", "feeds")
CLOSE_GRACE_S = 300  # an hour is closed this long after it ended (last flush + rotation)


def s3_client(env_path: str = "~/.config/polymm/s3.env"):
    import boto3
    from botocore.config import Config

    env = read_env_file(env_path)
    missing = [k for k in ("S3_ENDPOINT", "S3_REGION", "S3_BUCKET", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY") if k not in env]
    if missing:
        raise SystemExit(f"{env_path}: missing {missing}")
    c = boto3.client("s3", endpoint_url=env["S3_ENDPOINT"], region_name=env["S3_REGION"],
                     aws_access_key_id=env["AWS_ACCESS_KEY_ID"], aws_secret_access_key=env["AWS_SECRET_ACCESS_KEY"],
                     config=Config(retries={"max_attempts": 5, "mode": "standard"}, max_pool_connections=8))
    return c, env["S3_BUCKET"]


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def hour_dirs(root: Path, layers=DEFAULT_LAYERS):
    """Every partition directory holding files: …/dt=D/hour=H, or …/dt=D when there is no hour level."""
    for layer in layers:
        base = root / layer
        if not base.exists():
            continue
        for dt in sorted(base.glob("*/dt=*")) + sorted(base.glob("*/*/dt=*")):
            hours = sorted(dt.glob("hour=*"))
            for d in hours or [dt]:
                if any(p.is_file() and not p.name.startswith(".") for p in d.iterdir()):
                    yield d


def partition_end(d: Path) -> datetime | None:
    """When the partition stopped receiving writes: the end of its hour (or day)."""
    try:
        day = next(p for p in d.parts if p.startswith("dt=")).split("=")[1]
        t = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        if d.name.startswith("hour="):
            return t + timedelta(hours=int(d.name.split("=")[1]) + 1)
        return t + timedelta(days=1)
    except (StopIteration, ValueError):
        return None


def is_closed(d: Path, now: datetime | None = None) -> bool:
    end = partition_end(d)
    now = now or datetime.now(timezone.utc)
    if end is None or end + timedelta(seconds=CLOSE_GRACE_S) > now:
        return False
    return not any(p.suffix == ".tmp" for p in d.iterdir())


def load_manifest(d: Path) -> dict:
    m = d / MANIFEST
    try:
        return json.loads(m.read_text()) if m.exists() else {}
    except json.JSONDecodeError:
        return {}


def upload_partition(client, bucket: str, root: Path, d: Path, dry: bool = False) -> tuple[int, int]:
    """Upload the files of one partition that the manifest does not list yet. Returns (files, bytes)."""
    man = load_manifest(d)
    files = sorted(p for p in d.iterdir() if p.is_file() and not p.name.startswith(".") and p.suffix != ".tmp")
    n = b = 0
    changed = False
    for p in files:
        size = p.stat().st_size
        rec = man.get(p.name)
        if rec and rec.get("size") == size:
            continue
        key = str(p.relative_to(root))
        if dry:
            log.info("would upload %s (%d B)", key, size)
            n += 1
            b += size
            continue
        digest = sha256(p)
        client.upload_file(str(p), bucket, key)
        head = client.head_object(Bucket=bucket, Key=key)
        if head["ContentLength"] != size:
            raise RuntimeError(f"size mismatch after upload: {key} local {size} remote {head['ContentLength']}")
        man[p.name] = {"size": size, "sha256": digest, "etag": head.get("ETag", "").strip('"'),
                       "uploaded_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
        changed = True
        n += 1
        b += size
    if changed:
        tmp = d / (MANIFEST + ".tmp")
        tmp.write_text(json.dumps(man, indent=0, sort_keys=True))
        os.replace(tmp, d / MANIFEST)
        client.upload_file(str(d / MANIFEST), bucket, str((d / MANIFEST).relative_to(root)))
    return n, b


def run_upload(root: Path, env_path: str, layers=DEFAULT_LAYERS, prune_days: int | None = None, prune_layers=("feeds",),
               dry: bool = False) -> dict:
    client, bucket = s3_client(env_path)
    t0 = time.perf_counter()
    stats = {"partitions": 0, "files": 0, "bytes": 0, "pruned": 0, "errors": 0}
    for d in hour_dirs(root, layers):
        if not is_closed(d):
            continue
        try:
            n, b = upload_partition(client, bucket, root, d, dry)
        except Exception:
            stats["errors"] += 1
            log.exception("upload of %s failed", d)
            continue
        stats["partitions"] += 1
        stats["files"] += n
        stats["bytes"] += b
        if n:
            log.info("uploaded %s: %d files, %.1f MB", d.relative_to(root), n, b / 1e6)
    if prune_days is not None and not dry:
        stats["pruned"] = prune(root, prune_days, prune_layers)
    stats["seconds"] = round(time.perf_counter() - t0, 1)
    log.info("upload done: %s", stats)
    return stats


def prune(root: Path, keep_days: int, layers=("feeds",)) -> int:
    """Delete local partitions older than `keep_days` whose every file is in the manifest.
    Why only verified ones: the manifest is written after the bucket confirmed each file."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=keep_days)
    n = 0
    for d in list(hour_dirs(root, layers)):
        end = partition_end(d)
        if end is None or end > cutoff:
            continue
        man = load_manifest(d)
        files = [p for p in d.iterdir() if p.is_file() and not p.name.startswith(".")]
        if files and all(man.get(p.name, {}).get("size") == p.stat().st_size for p in files):
            shutil.rmtree(d)
            n += 1
            parent = d.parent
            if parent.name.startswith("dt=") and not any(parent.iterdir()):
                parent.rmdir()
    if n:
        log.info("pruned %d uploaded partitions older than %d days", n, keep_days)
    return n


def run_fetch(root: Path, env_path: str, prefixes: list[str], dry: bool = False) -> dict:
    """Download every object under the prefixes that is missing locally (or differs in size)."""
    client, bucket = s3_client(env_path)
    stats = {"files": 0, "bytes": 0}
    pag = client.get_paginator("list_objects_v2")
    for prefix in prefixes:
        for page in pag.paginate(Bucket=bucket, Prefix=prefix):
            for o in page.get("Contents", []):
                key, size = o["Key"], o["Size"]
                dst = root / key
                if dst.exists() and dst.stat().st_size == size:
                    continue
                if dry:
                    log.info("would fetch %s (%d B)", key, size)
                else:
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    tmp = dst.with_name(dst.name + ".part")
                    client.download_file(bucket, key, str(tmp))
                    os.replace(tmp, dst)
                stats["files"] += 1
                stats["bytes"] += size
    log.info("fetch done: %s", stats)
    return stats
