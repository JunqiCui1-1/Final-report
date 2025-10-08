# -*- coding: utf-8 -*-
"""
Filter multiple CSVs by SUBJECT_ID / HADM_ID (OR match) against a baseline CSV.
- Baseline: /content/CAD_CABG_loose_intersection.csv
- Inputs (any of .csv or .csv.gz are ok; compression='infer'):
    /content/admissions.csv
    /content/diagnoses_icd.csv
    /content/icustays.csv
    /content/labevents.csv
    /content/patients.csv
    /content/chartevents.csv
- Keep: header + matched rows
- Output dir: /content/filtered_by_ids (created if not exists)
- Chunked reading for large files (default 1,000,000 rows)
"""

from pathlib import Path
import pandas as pd
import re
import sys
from typing import Tuple, Set, Optional

# ---------------------- Config ----------------------
BASELINE = Path("/content/CAD_CABG_loose_intersection.csv")

INPUTS = [
    Path("/content/admissions.csv"),
    Path("/content/diagnoses_icd.csv"),
    Path("/content/icustays.csv"),
    Path("/content/labevents.csv"),
    Path("/content/patients.csv"),
    Path("/content/chartevents.csv"),
]

OUT_DIR = Path("/content/filtered_by_ids")
OUT_DIR.mkdir(parents=True, exist_ok=True)

CHUNKSIZE = 1_000_000  # rows per chunk; adjust if memory is tight
CSV_KW = dict(compression="infer", dtype=str)  # robust types


# ---------------------- Helpers ----------------------
def _norm_name(col: str) -> str:
    """Normalize column name: uppercase & remove non-alnum."""
    return re.sub(r"[^A-Z0-9]", "", col.upper())


def find_id_columns(columns) -> Tuple[Optional[str], Optional[str]]:
    """
    Find SUBJECT_ID and HADM_ID columns in a list of column names,
    regardless of case/underscore variations.
    """
    subj_col = None
    hadm_col = None
    for c in columns:
        nc = _norm_name(c)
        if nc == "SUBJECTID":
            subj_col = c
        elif nc == "HADMID":
            hadm_col = c
    return subj_col, hadm_col


def to_id_set(series) -> Set[str]:
    """Convert a pandas Series to a clean set of string IDs."""
    if series is None:
        return set()
    s = (
        series.dropna()
        .astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)  # clean float-cast artifacts like '123.0'
    )
    s = s[~s.str.lower().isin(["", "nan", "none", "null"])]
    return set(s.tolist())


def load_baseline_ids(baseline_path: Path) -> Tuple[Set[str], Set[str]]:
    """Load SUBJECT_ID/HADM_ID sets from the baseline CSV."""
    if not baseline_path.exists():
        print(f"[ERROR] Baseline not found: {baseline_path}", file=sys.stderr)
        sys.exit(1)

    # Read only needed columns if possible; fall back to full read
    df = pd.read_csv(baseline_path, **CSV_KW)
    subj_col, hadm_col = find_id_columns(df.columns)

    if subj_col is None and hadm_col is None:
        print("[ERROR] Baseline file has no SUBJECT_ID or HADM_ID columns.", file=sys.stderr)
        sys.exit(1)

    subj_ids = to_id_set(df[subj_col]) if subj_col else set()
    hadm_ids = to_id_set(df[hadm_col]) if hadm_col else set()

    print(f"[BASELINE] SUBJECT_IDs: {len(subj_ids):,} | HADM_IDs: {len(hadm_ids):,}")
    return subj_ids, hadm_ids


def filter_one_file(inp: Path, out_dir: Path, subj_ids: Set[str], hadm_ids: Set[str], chunksize: int = CHUNKSIZE):
    """Filter a single CSV by OR match on SUBJECT_ID/HADM_ID; write header+matched rows."""
    if not inp.exists():
        print(f"[WARN] Input not found, skip: {inp}")
        return

    out_path = out_dir / inp.name  # keep same file name under new folder
    wrote_header = False
    matched_rows = 0
    total_rows = 0

    # Peek header to find id columns first (cheap single-row read)
    head = pd.read_csv(inp, nrows=0, **CSV_KW)
    subj_col, hadm_col = find_id_columns(head.columns)

    if subj_col is None and hadm_col is None:
        # No id columns: still write header only (as required)
        head.to_csv(out_path, index=False)
        print(f"[INFO] {inp.name}: no SUBJECT_ID/HADM_ID columns. Wrote header only.")
        return

    # Stream by chunks
    for chunk in pd.read_csv(inp, chunksize=chunksize, **CSV_KW):
        total_rows += len(chunk)

        # Build boolean mask (OR condition)
        mask = pd.Series(False, index=chunk.index)
        if subj_col in chunk.columns:
            mask |= chunk[subj_col].astype(str).str.strip().isin(subj_ids)
        if hadm_col in chunk.columns:
            mask |= chunk[hadm_col].astype(str).str.strip().isin(hadm_ids)

        sub = chunk.loc[mask]
        if len(sub) > 0:
            sub.to_csv(out_path, index=False, mode="a", header=not wrote_header)
            wrote_header = True
            matched_rows += len(sub)

    # Ensure header exists even if no matches
    if not wrote_header:
        head.to_csv(out_path, index=False)

    print(f"[DONE] {inp.name}: matched {matched_rows:,} / {total_rows:,} rows -> {out_path}")


# ---------------------- Main ----------------------
if __name__ == "__main__":
    subj_ids, hadm_ids = load_baseline_ids(BASELINE)

    print(f"[RUN] Output dir: {OUT_DIR.resolve()}")
    for f in INPUTS:
        filter_one_file(f, OUT_DIR, subj_ids, hadm_ids, chunksize=CHUNKSIZE)

    print("[ALL DONE]")
