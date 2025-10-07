#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Loose intersection (SUBJECT_ID OR HADM_ID overlap) between:
- CAD matches in DIAGNOSES_ICD.csv (by diagnosis codes)
- CABG matches in PROCEDURES_ICD.csv (by procedure codes)

Inputs (CSV):
  DIAGNOSES_ICD.csv
  PROCEDURES_ICD.csv
  cad_diagnosis_icd.csv     # CAD code list (supports '*' suffix wildcard)
  cabg_procedure_icd.csv    # CABG code list (supports '*' suffix wildcard)

Output:
  patient_ids_SUBJECT_HADM.csv    # two columns: SUBJECT_ID,HADM_ID

Notes:
- Accepts MIMIC-III/IV column variants (SUBJECT_ID/subject_id, HADM_ID/hadm_id, ICD9_CODE/ICD10_CODE/icd_code).
- ICD codes are normalized to UPPERCASE and '.' removed; code lists support trailing '*' for prefix match.
- Uses parse_known_args() to tolerate Jupyter/Colab's extra args.
"""

import argparse, re
from pathlib import Path
import pandas as pd

# ---------- helpers ----------
def read_csv_safe(p: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig","utf-8","latin1"):
        try:
            return pd.read_csv(p, dtype=str, encoding=enc)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(p, dtype=str)

def pick_first(df: pd.DataFrame, candidates) -> str:
    for c in candidates:
        if c in df.columns:
            return c
    return ""

def normalize_core_cols(df: pd.DataFrame, kind: str) -> pd.DataFrame:
    subj = pick_first(df, ["SUBJECT_ID","subject_id"])
    hadm = pick_first(df, ["HADM_ID","hadm_id"])
    code = pick_first(df, ["ICD_CODE","icd_code","ICD9_CODE","ICD10_CODE","icd9_code","icd10_code","ICD","code","Code"])
    missing = []
    if not subj: missing.append("SUBJECT_ID/subject_id")
    if not hadm: missing.append("HADM_ID/hadm_id")
    if not code: missing.append("ICD_CODE/icd_code/ICD9_CODE/ICD10_CODE")
    if missing:
        raise KeyError(f"{kind} missing {missing}. Available: {df.columns.tolist()}")
    out = df[[subj, hadm, code]].copy()
    out.columns = ["SUBJECT_ID","HADM_ID","ICD_CODE"]
    return out

def find_code_col(df: pd.DataFrame) -> str:
    for c in ["ICD_CODE","icd_code","ICD9_CODE","ICD10_CODE","code","Code","ICD","icd","icd9","icd10"]:
        if c in df.columns:
            return c
    # heuristic fallback
    for c in df.columns:
        s = df[c].astype(str)
        if s.str.match(r"^[A-Za-z0-9][A-Za-z0-9\.\*]*$").fillna(False).mean() > 0.5:
            return c
    raise ValueError(f"Cannot find ICD code column in list. Columns: {df.columns.tolist()}")

def norm_code(x: str) -> str:
    x = ("" if x is None else str(x)).strip().upper()
    return x.replace(".", "")

def build_matcher(code_series: pd.Series):
    codes = [norm_code(c) for c in code_series.dropna()]
    exact, prefixes = set(), []
    for c in codes:
        if not c:
            continue
        if c.endswith("*"):
            prefixes.append(c[:-1])
        else:
            exact.add(c)
    regex = None
    if prefixes:
        pats = [re.escape(p) for p in sorted(set(prefixes)) if p]
        if pats:
            regex = re.compile(r"^(?:%s)" % "|".join(pats))
    def _match(series: pd.Series) -> pd.Series:
        sn = series.astype(str).map(norm_code)
        m = sn.isin(exact)
        if regex is not None:
            m = m | sn.str.match(regex).fillna(False)
        return m
    return _match

# ---------- core ----------
def run(diag_p, proc_p, cad_p, cabg_p, out_p):
    # load & normalize tables
    diag_raw = read_csv_safe(Path(diag_p))
    proc_raw = read_csv_safe(Path(proc_p))
    diag = normalize_core_cols(diag_raw, "DIAGNOSES_ICD.csv")
    proc = normalize_core_cols(proc_raw, "PROCEDURES_ICD.csv")

    # load code lists + matchers
    cad_list  = read_csv_safe(Path(cad_p))
    cabg_list = read_csv_safe(Path(cabg_p))
    cad_col  = find_code_col(cad_list)
    cabg_col = find_code_col(cabg_list)
    cad_matcher  = build_matcher(cad_list[cad_col])
    cabg_matcher = build_matcher(cabg_list[cabg_col])

    # filter matches
    diag_match = diag[cad_matcher(diag["ICD_CODE"])][["SUBJECT_ID","HADM_ID"]].dropna().drop_duplicates()
    proc_match = proc[cabg_matcher(proc["ICD_CODE"])][["SUBJECT_ID","HADM_ID"]].dropna().drop_duplicates()

    # normalize id dtype
    for df in (diag_match, proc_match):
        df["SUBJECT_ID"] = df["SUBJECT_ID"].astype(str)
        df["HADM_ID"]    = df["HADM_ID"].astype(str)

    # ---- LOOSE INTERSECTION ----
    # keep rows from either side if SUBJECT_ID OR HADM_ID appears in both sides
    subj_inter = set(diag_match["SUBJECT_ID"]) & set(proc_match["SUBJECT_ID"])
    hadm_inter = set(diag_match["HADM_ID"])    & set(proc_match["HADM_ID"])

    both = pd.concat([diag_match, proc_match], ignore_index=True).drop_duplicates()
    loose = both[both["SUBJECT_ID"].isin(subj_inter) | both["HADM_ID"].isin(hadm_inter)]
    loose = loose[["SUBJECT_ID","HADM_ID"]].drop_duplicates().reset_index(drop=True)

    # save
    loose.to_csv(out_p, index=False, encoding="utf-8-sig")
    print(f"Loose intersection (SUBJECT_ID OR HADM_ID): {len(loose)} pairs -> {out_p}")

def main():
    ap = argparse.ArgumentParser(description="Loose intersection on SUBJECT_ID OR HADM_ID (CAD vs CABG matches).")
    ap.add_argument("--diagnoses", default="DIAGNOSES_ICD.csv")
    ap.add_argument("--procedures", default="PROCEDURES_ICD.csv")
    ap.add_argument("--cad_codes", default="cad_diagnosis_icd.csv")
    ap.add_argument("--cabg_codes", default="cabg_procedure_icd.csv")
    ap.add_argument("--out", default="patient_ids_SUBJECT_HADM.csv")
    args, _ = ap.parse_known_args()

    run(args.diagnoses, args.procedures, args.cad_codes, args.cabg_codes, args.out)

if __name__ == "__main__":
    main()
