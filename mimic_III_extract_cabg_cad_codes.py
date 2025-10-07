#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Extract ICD codes for:
  - Diagnosis: Coronary artery disease (CAD)
  - Procedure: Coronary artery bypass grafting (CABG)

Inputs (CSV):
  - D_ICD_DIAGNOSES.csv
  - D_ICD_PROCEDURES.csv

Outputs (CSV in --outdir):
  - cad_diagnosis_icd.csv      (ICD_CODE, DESCRIPTION)
  - cabg_procedure_icd.csv     (ICD_CODE, DESCRIPTION)

Example:
  python extract_icd_cad_cabg.py \
      --diagnoses /mnt/data/D_ICD_DIAGNOSES.csv \
      --procedures /mnt/data/D_ICD_PROCEDURES.csv \
      --outdir ./icd_extracts
"""

import argparse, os, re
from pathlib import Path
import pandas as pd

# -------------------- IO helpers --------------------
def read_csv_flex(path: str) -> pd.DataFrame:
    if not path or not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")
    for enc in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            return pd.read_csv(path, dtype=str, encoding=enc, low_memory=False)
        except Exception:
            continue
    # last try without encoding hint
    return pd.read_csv(path, dtype=str, low_memory=False)

def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    return df

def pick_desc_col(df: pd.DataFrame) -> str:
    for c in ("long_title", "short_title", "title", "icd_title", "label", "description", "text"):
        if c in df.columns:
            return c
    raise KeyError(f"No description/title column found. Available: {list(df.columns)}")

def pick_code_col(df: pd.DataFrame) -> str:
    for c in ("icd_code", "code", "icd10_code", "icd9_code"):
        if c in df.columns:
            return c
    raise KeyError(f"No ICD code column found. Available: {list(df.columns)}")

# -------------------- Patterns (no capturing groups) --------------------
CAD_PATTERNS = [
    r"\bcoronary artery disease\b",
    r"\bcoronary atheroscl",                              # coronary atherosclerosis
    r"\batherosclerotic heart disease\b",                 # ASHD
    r"\bische?mic heart disease\b.*coronary",
    r"\bcoronary arter(?:ioscl|y)\b",
    r"\bchronic ischemic heart disease\b.*coronary",
    r"\batherosclerosis of (?:native )?coronary artery\b",
    r"\bCAD\b",
]

CABG_PATTERNS = [
    r"\baorto.?coronary.*bypass\b",
    r"\bcoronary artery bypass\b",
    r"\bbypass.*coronary artery\b",
    r"\bCABG\b",
    r"\bcoronary revascularization\b.*bypass",
]

CAD_REGEX = [re.compile(p, re.I) for p in CAD_PATTERNS]
CABG_REGEX = [re.compile(p, re.I) for p in CABG_PATTERNS]

def any_regex_match(series: pd.Series, compiled_patterns) -> pd.Series:
    s = series.fillna("")
    mask = pd.Series(False, index=s.index)
    for rx in compiled_patterns:
        mask = mask | s.str.contains(rx, na=False)
    return mask

# -------------------- Core logic --------------------
def extract_cad(diag_csv: str) -> pd.DataFrame:
    d = read_csv_flex(diag_csv)
    d = normalize_cols(d)
    code_col = pick_code_col(d)
    desc_col = pick_desc_col(d)
    m = any_regex_match(d[desc_col], CAD_REGEX)
    out = (d.loc[m, [code_col, desc_col]]
             .dropna()
             .drop_duplicates()
             .rename(columns={code_col: "ICD_CODE", desc_col: "DESCRIPTION"}))
    return out[["ICD_CODE", "DESCRIPTION"]].sort_values("ICD_CODE")

def extract_cabg(proc_csv: str) -> pd.DataFrame:
    p = read_csv_flex(proc_csv)
    p = normalize_cols(p)
    code_col = pick_code_col(p)
    desc_col = pick_desc_col(p)
    m = any_regex_match(p[desc_col], CABG_REGEX)
    out = (p.loc[m, [code_col, desc_col]]
             .dropna()
             .drop_duplicates()
             .rename(columns={code_col: "ICD_CODE", desc_col: "DESCRIPTION"}))
    return out[["ICD_CODE", "DESCRIPTION"]].sort_values("ICD_CODE")

def main():
    parser = argparse.ArgumentParser(description="Extract CAD (diagnoses) and CABG (procedures) ICD codes.")
    parser.add_argument("--diagnoses", default="D_ICD_DIAGNOSES.csv", help="Path to D_ICD_DIAGNOSES.csv")
    parser.add_argument("--procedures", default="D_ICD_PROCEDURES.csv", help="Path to D_ICD_PROCEDURES.csv")
    parser.add_argument("--outdir", default="./icd_extracts", help="Output directory")
    args, _ = parser.parse_known_args()  # ignore stray args from notebooks

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    cad = extract_cad(args.diagnoses)
    cabg = extract_cabg(args.procedures)

    cad_out = outdir / "cad_diagnosis_icd.csv"
    cabg_out = outdir / "cabg_procedure_icd.csv"
    cad.to_csv(cad_out, index=False)
    cabg.to_csv(cabg_out, index=False)

    print(f"[OK] CAD (diagnoses) rows: {len(cad)} -> {cad_out}")
    print(f"[OK] CABG (procedures) rows: {len(cabg)} -> {cabg_out}")

if __name__ == "__main__":
    # Do not call sys.exit to avoid SystemExit in notebooks
    main()
