#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Extract CAD from diagnosis.csv and CABG from treatment.csv.

Outputs:
  - CAD_from_diagnosis.csv
      Columns: ICD_CODE, DESCRIPTION
  - CABG_from_treatment.csv
      Columns: ICD_CODE, DESCRIPTION  (if a code column is present)
      or      DESCRIPTION             (if no code column is present)

Matching rules:
  CAD  (diagnosis):
    - ICD-10-CM: I25.*
    - ICD-9-CM : 414.*
  CABG (treatment):
    - ICD-10-PCS: length=7 and startswith '021'
    - ICD-9-CM procedures: 36.1x  (no-dot form '361*')
    - CPT: 33510–33523 (venous), 33533–33536 (arterial)
"""

from __future__ import annotations
import argparse
import logging
import re
from pathlib import Path
from typing import Optional, Tuple, List
import pandas as pd


# -------------------------- utils --------------------------

def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s"
    )

def read_csv_any(path: Path) -> pd.DataFrame:
    """Read CSV with common encodings; return empty DataFrame if file missing."""
    if not path.exists():
        logging.warning("File not found: %s", path)
        return pd.DataFrame()
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return pd.read_csv(path, dtype=str, encoding=enc).fillna("")
        except Exception:
            continue
    # last attempt without encoding hint
    return pd.read_csv(path, dtype=str).fillna("")

def pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """Pick the first existing column (case-insensitive) from candidates."""
    cols_lc = {c.lower(): c for c in df.columns}
    for k in candidates:
        if k.lower() in cols_lc:
            return cols_lc[k.lower()]
    return None

def nodot_upper(s: Optional[str]) -> str:
    """Normalize code by removing dots/spaces and uppercasing."""
    return re.sub(r"[.\s]", "", (s or "")).upper()

def has_code_column(df: pd.DataFrame) -> Tuple[Optional[str], bool]:
    """
    Detect a usable code column in an event table (diagnosis/treatment).
    Returns: (column_name or None, True if column is likely code-like)
    """
    candidates = [
        "icd_code", "icd10_code", "icd9_code", "code", "icd",
        "proc_code", "procedure_code", "operation_code", "op_code",
        "cpt", "cpt_code", "dx", "dx1"
    ]
    col = pick_col(df, candidates)
    return (col, col is not None)

# ----------------------- CAD rules (diagnosis) -----------------------

CAD_TEXT_RE = re.compile(
    r"(coronary artery disease|coronary atherosclerosis|chronic ischemic heart disease|ischemic heart disease)",
    flags=re.I
)

def is_cad_code(icd_code: str) -> bool:
    """CAD by code: ICD-10-CM I25.*, ICD-9-CM 414.*"""
    c = (icd_code or "").strip().upper()
    c_n = nodot_upper(c)
    return c.startswith("I25") or c_n.startswith("414")

def is_cad_text(desc: str) -> bool:
    return bool(CAD_TEXT_RE.search((desc or "").strip()))

# ---------------------- CABG rules (treatment) ----------------------

CABG_TEXT_RE = re.compile(
    r"(coronary artery bypass(\s*graft(ing)?)?|aorto[\-\s]?coronary\s+bypass|cabg)",
    flags=re.I
)

def is_cabg_icd10pcs(code: str) -> bool:
    """ICD-10-PCS CABG: exactly 7 alphanumeric chars starting with '021'."""
    c = (code or "").strip().upper()
    return len(c) == 7 and c.isalnum() and c.startswith("021")

def is_cabg_icd9proc(code: str) -> bool:
    """ICD-9-CM procedure CABG: 36.1x (no-dot form '361*')."""
    return nodot_upper(code).startswith("361")

def is_cabg_cpt(code: str) -> bool:
    """CABG CPT ranges: 33510–33523 (venous), 33533–33536 (arterial)."""
    c = (code or "").strip().upper()
    if not c.isdigit():
        return False
    v = int(c)
    return (33510 <= v <= 33523) or (33533 <= v <= 33536)

def is_cabg_by_code(code: str) -> bool:
    return is_cabg_icd10pcs(code) or is_cabg_icd9proc(code) or is_cabg_cpt(code)

def is_cabg_text(desc: str) -> bool:
    return bool(CABG_TEXT_RE.search((desc or "").strip()))

# ---------------------- extraction pipelines ----------------------

def extract_cad_from_diagnosis(diagnosis_csv: Path) -> pd.DataFrame:
    df = read_csv_any(diagnosis_csv)
    if df.empty:
        logging.warning("Diagnosis file is empty or missing.")
        return pd.DataFrame(columns=["ICD_CODE", "DESCRIPTION"])

    code_col = pick_col(df, [
        "icd_code", "icd10_code", "icd9_code", "code", "icd", "dx", "dx1"
    ])
    desc_col = pick_col(df, [
        "long_title", "longtitle", "title", "description", "desc",
        "diagnosis_description", "dx_name", "short_title", "shorttitle"
    ])

    # Build working view
    w = pd.DataFrame()
    if code_col:
        w["ICD_CODE"] = df[code_col].astype(str).str.strip().str.upper()
        w["NORM_CODE"] = w["ICD_CODE"].map(nodot_upper)
    if desc_col:
        w["DESCRIPTION"] = df[desc_col].astype(str).str.strip()

    # Match by code or text
    mask_code = w["ICD_CODE"].map(is_cad_code) if "ICD_CODE" in w else pd.Series(False, index=w.index)
    mask_text = w["DESCRIPTION"].map(is_cad_text) if "DESCRIPTION" in w else pd.Series(False, index=w.index)
    mask = mask_code | mask_text
    out_cols = [c for c in ["ICD_CODE", "DESCRIPTION"] if c in w.columns]

    out = w.loc[mask, out_cols].drop_duplicates()
    # Ensure both columns exist in output
    if "ICD_CODE" not in out.columns:
        out["ICD_CODE"] = ""
    if "DESCRIPTION" not in out.columns:
        out["DESCRIPTION"] = ""
    return out[["ICD_CODE", "DESCRIPTION"]].sort_values(["ICD_CODE", "DESCRIPTION"])


def extract_cabg_from_treatment(treatment_csv: Path) -> pd.DataFrame:
    df = read_csv_any(treatment_csv)
    if df.empty:
        logging.warning("Treatment file is empty or missing.")
        return pd.DataFrame(columns=["DESCRIPTION"])  # fall back to single-column

    code_col = pick_col(df, [
        "icd_code", "icd10pcs", "icd10_code", "icd9_code", "proc_code",
        "procedure_code", "operation_code", "op_code", "code", "cpt", "cpt_code"
    ])
    desc_col = pick_col(df, [
        "procedure", "procedure_name", "proc_description", "operation",
        "op_name", "title", "description", "desc", "long_title", "short_title"
    ])

    w = pd.DataFrame()
    if code_col:
        w["ICD_CODE"] = df[code_col].astype(str).str.strip().str.upper()
    if desc_col:
        w["DESCRIPTION"] = df[desc_col].astype(str).str.strip()

    # Match by code or text
    mask_code = w["ICD_CODE"].map(is_cabg_by_code) if "ICD_CODE" in w else pd.Series(False, index=df.index)
    mask_text = w["DESCRIPTION"].map(is_cabg_text) if "DESCRIPTION" in w else pd.Series(False, index=df.index)
    mask = mask_code | mask_text

    # If we have a code column, return two columns; else return only DESCRIPTION
    if "ICD_CODE" in w:
        out = w.loc[mask, [c for c in ["ICD_CODE", "DESCRIPTION"] if c in w.columns]].drop_duplicates()
        if "DESCRIPTION" not in out.columns:  # if description missing in source, create empty col
            out["DESCRIPTION"] = ""
        return out.sort_values(["ICD_CODE", "DESCRIPTION"])
    else:
        out = w.loc[mask, [c for c in ["DESCRIPTION"] if c in w.columns]].drop_duplicates()
        if out.empty and desc_col is None:
            # no description column exists; create an empty one to satisfy the spec
            out = pd.DataFrame({"DESCRIPTION": []})
        return out.sort_values(["DESCRIPTION"])

# -------------------------- CLI --------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Extract CAD (diagnosis) and CABG (treatment) from event CSVs.")
    p.add_argument("--diagnosis", type=Path, default=Path("diagnosis.csv"),
                   help="Path to diagnosis CSV. Default: diagnosis.csv")
    p.add_argument("--treatment", type=Path, default=Path("treatment.csv"),
                   help="Path to treatment CSV. Default: treatment.csv")
    p.add_argument("--outdir", type=Path, default=Path("."),
                   help="Output directory. Default: current directory")
    p.add_argument("--loglevel", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                   help="Logging level. Default: INFO")
    return p.parse_args()

def main() -> None:
    args = parse_args()
    setup_logging(args.loglevel)

    logging.info("Extracting CAD from %s", args.diagnosis)
    cad = extract_cad_from_diagnosis(args.diagnosis)
    args.outdir.mkdir(parents=True, exist_ok=True)
    cad_path = args.outdir / "CAD_from_diagnosis.csv"
    cad.to_csv(cad_path, index=False, encoding="utf-8")
    logging.info("CAD rows: %d -> %s", len(cad), cad_path)

    logging.info("Extracting CABG from %s", args.treatment)
    cabg = extract_cabg_from_treatment(args.treatment)
    cabg_path = args.outdir / "CABG_from_treatment.csv"
    cabg.to_csv(cabg_path, index=False, encoding="utf-8")
    logging.info("CABG rows: %d -> %s", len(cabg), cabg_path)

if __name__ == "__main__":
    main()
