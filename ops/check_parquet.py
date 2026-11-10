"""Find damaged Parquet files in a data store: exit 1 and list them if any.

    python -I ops/check_parquet.py data_vps [data_vps ...]

A complete Parquet file starts and ends with the 4 magic bytes PAR1. A truncated copy
(an interrupted transfer, a full disk) fails that test, and any reader that opens it
crashes the whole run. Checking 8 bytes per file is fast enough for every file, every
morning, before anything reads the data. Files under .rsync-partial/ (unfinished
transfers kept aside by rsync) and *.tmp files (still being written) are skipped.
"""
from __future__ import annotations

import sys
from pathlib import Path


def bad_files(root: Path) -> list[tuple[Path, int]]:
    bad = []
    for p in root.rglob("*.parquet"):
        if ".rsync-partial" in p.parts:
            continue
        try:
            with open(p, "rb") as f:
                head = f.read(4)
                f.seek(0, 2)
                n = f.tell()
                if n < 12:
                    bad.append((p, n))
                    continue
                f.seek(-4, 2)
                tail = f.read(4)
        except OSError:
            bad.append((p, -1))
            continue
        if head != b"PAR1" or tail != b"PAR1":
            bad.append((p, n))
    return bad


def main(argv: list[str]) -> int:
    roots = [Path(a) for a in argv] or [Path("data_vps")]
    bad = [b for r in roots if r.exists() for b in bad_files(r)]
    for p, n in bad:
        print(f"damaged: {p} ({n} bytes)")
    print(f"parquet check: {len(bad)} damaged file(s) under {', '.join(map(str, roots))}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
