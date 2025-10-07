#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Intersect patientunitstayid across CAD (diagnosis side) and CABG (treatment side).

Inputs:
  diagnosis.csv                 # has patientunitstayid, diagnosisstring, icd9code
  treatment.csv                 # has patientunitstayid, treatmentstring
  CAD_from_diagnosis.csv        # DESCRIPTION only; parse 2nd integer as patientunitstayid
  CABG_from_treatment.csv       # DESCRIPTION only; parse 2nd integer as patientunitstayid

Output:
  patientunitstayid_intersection.csv   # one column: patientunitstayid (sorted unique)

Run:
  python extract_patientunitstayid_intersection.py \
    --diagnosis diagnosis.csv \
    --treatment treatment.csv \
    --cad_list CAD_from_diagnosis.csv \
    --cabg_list CABG_from_treatment.csv \
    --out patientunitstayid_intersection.csv
"""

import argparse, re
from pathlib import Path
import pandas as pd

def read_csv_safe(p: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig","utf-8","gb18030","latin1"):
        try:
            return pd.read_csv(p, dtype=str, encoding=enc)
        except Exception:
            pass
    return pd.read_csv(p, dtype=str)

def exists(p: str) -> bool:
    try:
        return Path(p).exists()
    except Exception:
        return False

def col_present(df: pd.DataFrame, names: list[str]) -> str:
    for n in names:
        if n in df.columns:
            return n
    return ""

def parse_ids_from_description(df: pd.DataFrame, desc_col="DESCRIPTION") -> pd.Series:
    """
    DESCRIPTION looks like: "<some_id>  <patientunitstayid>  <offset>  ...".
    Extract the 2nd integer token as patientunitstayid.
    """
    if desc_col not in df.columns:
        return pd.Series([], dtype=str)
    s = df[desc_col].dropna().astype(str)
    out = []
    for line in s:
        # First try split by 2+ spaces (as observed)
        parts = re.split(r"\s{2,}", line.strip())
        if len(parts) < 2:
            # Fallback: any whitespace, then capture integers
            ints = re.findall(r"\b\d+\b", line)
            if len(ints) >= 2:
                out.append(ints[1])
            continue
        # parts[1] should be patientunitstayid
        if re.fullmatch(r"\d+", parts[1]):
            out.append(parts[1])
        else:
            # Fallback: integer scan
            ints = re.findall(r"\b\d+\b", line)
            if len(ints) >= 2:
                out.append(ints[1])
    return pd.Series(out, dtype=str).dropna().drop_duplicates()

def match_diag_ids_by_text(diagnosis: pd.DataFrame) -> pd.Series:
    pid_col = col_present(diagnosis, ["patientunitstayid","PATIENTUNITSTAYID"])
    txt_col = col_present(diagnosis, ["diagnosisstring","DIAGNOSISSTRING"])
    if not pid_col or not txt_col:
        return pd.Series([], dtype=str)
    text = diagnosis[txt_col].astype(str)
    # CAD-related keywords
    cad_kw = [
        r"coronary artery disease",
        r"\bCAD\b",
        r"ischemic heart disease",
        r"\bIHD\b",
        r"\bASHD\b",
        r"coronary disease"
    ]
    pat = re.compile(r"(?:%s)" % "|".join(cad_kw), flags=re.IGNORECASE)
    m = text.str.contains(pat, na=False)
    return diagnosis.loc[m, pid_col].dropna().astype(str).drop_duplicates()

def match_treat_ids_by_text(treatment: pd.DataFrame) -> pd.Series:
    pid_col = col_present(treatment, ["patientunitstayid","PATIENTUNITSTAYID"])
    txt_col = col_present(treatment, ["treatmentstring","TREATMENTSTRING"])
    if not pid_col or not txt_col:
        return pd.Series([], dtype=str)
    text = treatment[txt_col].astype(str)
    # CABG-related keywords
    cabg_kw = [
        r"\bCABG\b",
        r"coronary artery bypass graft",
        r"coronary artery bypass"
    ]
    pat = re.compile(r"(?:%s)" % "|".join(cabg_kw), flags=re.IGNORECASE)
    m = text.str.contains(pat, na=False)
    return treatment.loc[m, pid_col].dropna().astype(str).drop_duplicates()

# ---------- core ----------
def run(diagnosis_path, treatment_path, cad_list_path, cabg_list_path, out_path):
    # ---- build CAD side IDs ----
    cad_ids_parts = []

    if exists(diagnosis_path):
        diag = read_csv_safe(Path(diagnosis_path))
        cad_ids_parts.append(match_diag_ids_by_text(diag))

    if exists(cad_list_path):
        cad_list = read_csv_safe(Path(cad_list_path))
        cad_ids_parts.append(parse_ids_from_description(cad_list, "DESCRIPTION"))

    CAD_ids = (pd.concat(cad_ids_parts, ignore_index=True).drop_duplicates()
               if cad_ids_parts else pd.Series([], dtype=str))

    # ---- build CABG side IDs ----
    cabg_ids_parts = []

    if exists(treatment_path):
        treat = read_csv_safe(Path(treatment_path))
        cabg_ids_parts.append(match_treat_ids_by_text(treat))

    if exists(cabg_list_path):
        cabg_list = read_csv_safe(Path(cabg_list_path))
        cabg_ids_parts.append(parse_ids_from_description(cabg_list, "DESCRIPTION"))

    CABG_ids = (pd.concat(cabg_ids_parts, ignore_index=True).drop_duplicates()
                if cabg_ids_parts else pd.Series([], dtype=str))

    # ---- INTERSECTION ----
    inter = sorted(set(CAD_ids.astype(str)) & set(CABG_ids.astype(str)))
    out_df = pd.DataFrame({"patientunitstayid": inter})

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out_path, index=False, encoding="utf-8-sig")

    print(f"CAD side unique IDs:  {len(CAD_ids)}")
    print(f"CABG side unique IDs: {len(CABG_ids)}")
    print(f"INTERSECTION size:    {len(out_df)} -> {out_path}")

def main():
    ap = argparse.ArgumentParser(description="Intersect patientunitstayid across CAD (diagnosis) and CABG (treatment).")
    ap.add_argument("--diagnosis", default="diagnosis.csv")
    ap.add_argument("--treatment", default="treatment.csv")
    ap.add_argument("--cad_list", default="CAD_from_diagnosis.csv")
    ap.add_argument("--cabg_list", default="CABG_from_treatment.csv")
    ap.add_argument("--out", default="patientunitstayid_intersection.csv")
    args, _ = ap.parse_known_args()
    run(args.diagnosis, args.treatment, args.cad_list, args.cabg_list, args.out)

if __name__ == "__main__":
    main()
