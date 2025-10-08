#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Filter large MIMIC-style CSV/CSV.GZ tables by SUBJECT_ID and/or HADM_ID
using an ID baseline file, writing header + matched rows to a new folder.

Key features
------------
- OR logic: keep any row where SUBJECT_ID OR HADM_ID matches the baseline.
- Works with .csv and .csv.gz (compression='infer').
- Chunked reading for very large files (default 1,000,000 rows per chunk).
- Always writes the header (even if no rows match), per requirement.
- Case-insensitive column matching (SUBJECT_ID/HADM_ID).

Default paths (can be overridden via CLI):
- Baseline IDs: /content/patient_ids_SUBJECT_HADM.csv
- Input files:
    /content/ADMISSIONS.csv
    /content/DIAGNOSES_ICD.csv
    /content/ICUSTAYS.csv
    /content/LABEVENTS.csv
    /content/PATIENTS.csv
    /content/TRANSFERS.csv
    /content/CHARTEVENTS.csv
- Output folder: /content/filtered_by_ids

Usage
-----
# Basic (use defaults)
python filter_by_ids.py

# Custom IDs file and output folder
python filter_by_ids.py \
  --ids /path/to/patient_ids_SUBJECT_HADM.csv \
  --out-dir /path/to/out

# Custom input files (space-separated)
python filter_by_ids.py \
  --inputs /data/A.csv /data/B.csv.gz /data/C.csv \
  --ids /data/patient_ids.csv \
  --out-dir /data/filtered \
  --chunksize 500000 \
  --log-every 5
"""

from __future__ import annotations

import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, Iterable, List, Set, Tuple

import pandas as pd


DEFAULT_IDS = "/content/patient_ids_SUBJECT_HADM.csv"
DEFAULT_INPUTS = [
    "/content/ADMISSIONS.csv",
    "/content/DIAGNOSES_ICD.csv",
    "/content/ICUSTAYS.csv",
    "/content/LABEVENTS.csv",
    "/content/PATIENTS.csv",
    "/content/TRANSFERS.csv",
    "/content/CHARTEVENTS.csv",
]
DEFAULT_OUT_DIR = "/content/filtered_by_ids"


def load_id_sets(ids_path: Path) -> Tuple[Set[str], Set[str]]:
    """Load SUBJECT_ID and HADM_ID sets from the baseline CSV."""
    if not ids_path.exists():
        raise FileNotFoundError(f"Baseline IDs file not found: {ids_path}")

    ids = pd.read_csv(ids_path, dtype=str, on_bad_lines="skip", low_memory=False)
    ids.columns = [c.upper() for c in ids.columns]

    if "SUBJECT_ID" not in ids.columns and "HADM_ID" not in ids.columns:
        raise ValueError("Baseline file must contain SUBJECT_ID and/or HADM_ID columns.")

    sub_ids: Set[str] = set()
    hadm_ids: Set[str] = set()

    if "SUBJECT_ID" in ids.columns:
        sub_ids = set(ids["SUBJECT_ID"].dropna().astype(str).str.strip())
    if "HADM_ID" in ids.columns:
        hadm_ids = set(ids["HADM_ID"].dropna().astype(str).str.strip())

    print(f"[INFO] Loaded baseline IDs: SUBJECT_ID={len(sub_ids):,}, HADM_ID={len(hadm_ids):,}")
    return sub_ids, hadm_ids


def norm_colmap(columns: Iterable[str]) -> Dict[str, str]:
    """Build a mapping {UPPER: original_name} without changing original names."""
    return {c.upper(): c for c in columns}


def target_out_path(in_path: Path, out_dir: Path) -> Path:
    """Compute output filename (strip '.gz', ensure '.csv') inside out_dir."""
    name = in_path.name
    if name.endswith(".gz"):
        name = name[:-3]  # drop .gz
    if not name.lower().endswith(".csv"):
        name += ".csv"
    return out_dir / name


def process_one_file(
    in_path: Path,
    out_dir: Path,
    sub_ids: Set[str],
    hadm_ids: Set[str],
    chunksize: int = 1_000_000,
    log_every: int = 10,
) -> None:
    """Filter a single CSV/CSV.GZ file by SUBJECT_ID/HADM_ID (OR logic)."""
    if not in_path.exists():
        print(f"[SKIP] File not found: {in_path}")
        return

    out_path = target_out_path(in_path, out_dir)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    kept_rows = 0
    header_written = False
    start = datetime.now()

    print(f"\n[START] Filtering: {in_path}")
    reader = pd.read_csv(
        in_path,
        dtype=str,
        chunksize=chunksize,
        compression="infer",
        on_bad_lines="skip",
        low_memory=False,
    )

    for i, chunk in enumerate(reader, 1):
        total_rows += len(chunk)
        if total_rows == 0:
            continue

        colmap = norm_colmap(chunk.columns)

        # Always write header (even if no matches in any chunk)
        if not header_written:
            chunk.head(0).to_csv(out_path, index=False, mode="w", header=True)
            header_written = True

        # If neither ID column exists, there is nothing to match; continue.
        if "SUBJECT_ID" not in colmap and "HADM_ID" not in colmap:
            if i == 1:
                print("  [WARN] Neither SUBJECT_ID nor HADM_ID found in columns; only header written.")
            continue

        mask = pd.Series(False, index=chunk.index)

        if "SUBJECT_ID" in colmap:
            s = chunk[colmap["SUBJECT_ID"]].astype(str).str.strip().isin(sub_ids)
            mask = mask | s
        if "HADM_ID" in colmap:
            h = chunk[colmap["HADM_ID"]].astype(str).str.strip().isin(hadm_ids)
            mask = mask | h

        matched = chunk[mask]
        if not matched.empty:
            kept_rows += len(matched)
            matched.to_csv(out_path, index=False, mode="a", header=False)

        if (i % log_every) == 0:
            rate = (kept_rows / max(total_rows, 1)) * 100.0
            print(f"  - Progress: chunk {i}, read {total_rows:,} rows, kept {kept_rows:,} rows ({rate:.2f}%)")

    dur = (datetime.now() - start).total_seconds()
    print(f"[DONE] {in_path.name} -> {out_path.name} | read {total_rows:,}, kept {kept_rows:,}, took {dur:.1f}s")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Filter CSV/CSV.GZ tables by SUBJECT_ID/HADM_ID (OR logic) using a baseline IDs CSV."
    )
    p.add_argument(
        "--ids",
        type=Path,
        default=Path(DEFAULT_IDS),
        help=f"Path to baseline IDs CSV (default: {DEFAULT_IDS})",
    )
    p.add_argument(
        "--inputs",
        type=Path,
        nargs="*",
        default=[Path(x) for x in DEFAULT_INPUTS],
        help="Input CSV/CSV.GZ files to filter (space-separated).",
    )
    p.add_argument(
        "--out-dir",
        type=Path,
        default=Path(DEFAULT_OUT_DIR),
        help=f"Output folder for filtered CSVs (default: {DEFAULT_OUT_DIR})",
    )
    p.add_argument(
        "--chunksize",
        type=int,
        default=1_000_000,
        help="Number of rows per chunk when reading large files (default: 1,000,000).",
    )
    p.add_argument(
        "--log-every",
        type=int,
        default=10,
        help="Log progress every N chunks (default: 10).",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    sub_ids, hadm_ids = load_id_sets(args.ids)

    for fpath in args.inputs:
        process_one_file(
            in_path=fpath,
            out_dir=args.out_dir,
            sub_ids=sub_ids,
            hadm_ids=hadm_ids,
            chunksize=args.chunksize,
            log_every=args.log_every,
        )

    print(f"\n[ALL DONE] Output directory: {args.out_dir.resolve()}")


if __name__ == "__main__":
    main()
