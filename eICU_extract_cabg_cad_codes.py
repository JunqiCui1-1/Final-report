#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Extract CAD (from diagnosis.csv) and CABG (from treatment.csv) with strict outputs:

- CAD_from_diagnosis.csv:    exactly 2 columns -> ICD_CODE, DESCRIPTION
- CABG_from_treatment.csv:
    * if any valid ICD/CPT present -> exactly 2 columns (only valid-code rows)
    * if no valid ICD/CPT present  -> exactly 1 column (DESCRIPTION only)

Patterns:
- CAD codes: ICD-10-CM I25.*, ICD-9-CM 414.*
- CABG codes: ICD-10-PCS 021***** (7 chars), ICD-9-Proc 36.1x (361* nodot), CPT 33510–33523, 33533–33536
"""

from __future__ import annotations
import re
from pathlib import Path
from typing import Optional, List
import pandas as pd

def read_csv_any(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return pd.read_csv(path, dtype=str, encoding=enc).fillna("")
        except Exception:
            pass
    return pd.read_csv(path, dtype=str).fillna("")

def pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols_lc = {c.lower(): c for c in df.columns}
    for k in candidates:
        if k.lower() in cols_lc:
            return cols_lc[k.lower()]
    return None

def nodot_upper(s: Optional[str]) -> str:
    return re.sub(r"[.\s]", "", (s or "")).upper()

def concat_text_fallback(df: pd.DataFrame, max_cols: int = 8) -> pd.Series:
    obj_cols = [c for c in df.columns if df[c].dtype == object]
    obj_cols = obj_cols[:max_cols]
    if not obj_cols:
        return pd.Series([""] * len(df))
    s = df[obj_cols[0]].astype(str)
    for c in obj_cols[1:]:
        s = s.str.cat(" " + df[c].astype(str), sep=" ", na_rep="")
    return s.str.strip()


# ------------------------- CAD ---------------------------

CAD_TEXT_RE = re.compile(
    r"(coronary artery disease|coronary atherosclerosis|chronic ischemic heart disease|"
    r"ischemic heart disease|coronary heart disease|CAD\b|IHD\b)",
    re.I
)

def is_cad_code(code: str) -> bool:
    c = (code or "").strip().upper()
    c_n = nodot_upper(c)
    return c.startswith("I25") or c_n.startswith("414")

def extract_cad_from_diagnosis(diagnosis_csv: Path) -> pd.DataFrame:
    df = read_csv_any(diagnosis_csv)
    if df.empty:
        return pd.DataFrame(columns=["ICD_CODE", "DESCRIPTION"])

    code_col = pick_col(df, ["icd_code","icd10_code","icd9_code","code","icd","dx","dx1"])
    desc_col = pick_col(df, ["description","long_title","title","desc","diagnosis_description","dx_name","short_title","shorttitle"])

    w = pd.DataFrame(index=df.index)
    if code_col:
        w["ICD_CODE"] = df[code_col].astype(str).str.strip().str.upper()
    else:
        w["ICD_CODE"] = ""  # ensure column exists

    if desc_col:
        w["DESCRIPTION"] = df[desc_col].astype(str).str.strip()
    else:
        w["DESCRIPTION"] = concat_text_fallback(df)

    mask_code = w["ICD_CODE"].map(is_cad_code) if code_col else pd.Series(False, index=w.index)
    mask_text = w["DESCRIPTION"].str.contains(CAD_TEXT_RE, na=False)
    mask = mask_code | mask_text

    out = w.loc[mask, ["ICD_CODE", "DESCRIPTION"]].drop_duplicates()
    # keep only CAD-valid codes; if not valid for a row, blank it (still 2 columns)
    out.loc[~out["ICD_CODE"].map(is_cad_code), "ICD_CODE"] = ""
    out = out.sort_values(["ICD_CODE", "DESCRIPTION"])
    return out[["ICD_CODE", "DESCRIPTION"]]


# ------------------------- CABG --------------------------

CABG_TEXT_RE = re.compile(
    r"(coronary artery bypass(\s*graft(ing)?)?|aorto[\-\s]?coronary\s+bypass|CABG\b)",
    re.I
)

def is_cabg_icd10pcs(code: str) -> bool:
    c = (code or "").strip().upper()
    return len(c) == 7 and c.isalnum() and c.startswith("021")

def is_cabg_icd9proc(code: str) -> bool:
    return nodot_upper(code).startswith("361")  # 36.1x

def is_cabg_cpt(code: str) -> bool:
    c = (code or "").strip()
    if not c.isdigit():
        return False
    v = int(c)
    return (33510 <= v <= 33523) or (33533 <= v <= 33536)

def is_cabg_code(code: str) -> bool:
    return is_cabg_icd10pcs(code) or is_cabg_icd9proc(code) or is_cabg_cpt(code)

def extract_cabg_from_treatment(treatment_csv: Path) -> pd.DataFrame:
    df = read_csv_any(treatment_csv)
    if df.empty:
        return pd.DataFrame(columns=["DESCRIPTION"])

    code_col = pick_col(df, [
        "icd_code","icd10pcs","icd10_code","icd9_code","proc_code","procedure_code",
        "operation_code","op_code","code","cpt","cpt_code"
    ])
    desc_col = pick_col(df, [
        "procedure","procedure_name","proc_description","operation","op_name",
        "title","description","desc","long_title","short_title","name","label"
    ])

    w = pd.DataFrame(index=df.index)
    if code_col:
        w["ICD_CODE"] = df[code_col].astype(str).str.strip().str.upper()
    if desc_col:
        w["DESCRIPTION"] = df[desc_col].astype(str).str.strip()
    else:
        w["DESCRIPTION"] = concat_text_fallback(df)

    mask_text = w["DESCRIPTION"].str.contains(CABG_TEXT_RE, na=False)

    if "ICD_CODE" in w:
        valid_code_mask = w["ICD_CODE"].map(is_cabg_code)
        any_valid = bool(valid_code_mask.any())

        if any_valid:
            # strict two-column output; include only rows with valid codes
            out = w.loc[valid_code_mask, ["ICD_CODE", "DESCRIPTION"]].drop_duplicates()
            out = out.sort_values(["ICD_CODE", "DESCRIPTION"])
            return out[["ICD_CODE", "DESCRIPTION"]]
        else:
            # no valid ICD/CPT at all -> description-only per requirement
            out = w.loc[mask_text, ["DESCRIPTION"]].drop_duplicates()
            out = out.sort_values(["DESCRIPTION"])
            return out[["DESCRIPTION"]]
    else:
        # no code column -> description-only per requirement
        out = w.loc[mask_text, ["DESCRIPTION"]].drop_duplicates()
        out = out.sort_values(["DESCRIPTION"])
        return out[["DESCRIPTION"]]


# ------------------------- run ---------------------------

if __name__ == "__main__":
    # notebook/CLI safe entry (no argparse; configure paths here)
    DIAGNOSIS = Path("diagnosis.csv")   # adjust if needed
    TREATMENT = Path("treatment.csv")   # adjust if needed
    OUTDIR    = Path(".")

    cad = extract_cad_from_diagnosis(DIAGNOSIS)
    cabg = extract_cabg_from_treatment(TREATMENT)

    OUTDIR.mkdir(parents=True, exist_ok=True)
    cad.to_csv(OUTDIR / "CAD_from_diagnosis.csv", index=False, encoding="utf-8")
    cabg.to_csv(OUTDIR / "CABG_from_treatment.csv", index=False, encoding="utf-8")

    print(f"[CAD] rows:  {len(cad)} -> {OUTDIR / 'CAD_from_diagnosis.csv'}")
    print(f"[CABG] rows: {len(cabg)} -> {OUTDIR / 'CABG_from_treatment.csv'}")
