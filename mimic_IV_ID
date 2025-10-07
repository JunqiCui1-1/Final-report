#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Loose intersection between CAD diagnoses matches and CABG procedures matches:
Count a pair if EITHER subject_id matches across sets OR hadm_id matches across sets.

Inputs:
  diagnoses_icd.csv  -> columns: subject_id, hadm_id, icd_code
  procedures_icd.csv -> columns: subject_id, hadm_id, icd_code
  cad_icd.csv        -> CAD code list; code column auto-detected (supports '*' suffix wildcards)
  cabg_icd.csv       -> CABG code list; code column auto-detected (supports '*' suffix wildcards)

Outputs:
  outdir/CAD_matches.csv                      # diagnosis matches (unique pairs)
  outdir/CABG_matches.csv                     # procedure matches (unique pairs)
  outdir/CAD_CABG_loose_intersection.csv      # loose intersection on subject_id OR hadm_id (two columns)
  outdir/summary.txt
"""

import argparse, re
from pathlib import Path
import pandas as pd

def read_csv_safe(p: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "latin1"):
        try:
            return pd.read_csv(p, dtype=str, encoding=enc)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(p, dtype=str)

def ensure_cols(df: pd.DataFrame, need, fname: str):
    miss = [c for c in need if c not in df.columns]
    if miss:
        raise KeyError(f"{fname} missing {miss}. Available: {df.columns.tolist()}")
    return df

def find_code_col(df: pd.DataFrame) -> str:
    candidates = [
        "icd_code","ICD_CODE","ICD-CODE","code","Code","ICD",
        "ICD9_CODE","ICD10_CODE","icd","icd9","icd10"
    ]
    for c in candidates:
        if c in df.columns:
            return c
    # heuristic
    for c in df.columns:
        s = df[c].astype(str)
        if s.str.match(r"^[A-Za-z0-9][A-Za-z0-9\.\*]*$").fillna(False).mean() > 0.5:
            return c
    raise ValueError(f"Cannot find ICD code column in {df.columns.tolist()}")

# ---------- Code normalization & matcher ----------
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
    def _match(s: pd.Series) -> pd.Series:
        sn = s.astype(str).map(norm_code)
        m = sn.isin(exact)
        if regex is not None:
            m = m | sn.str.match(regex).fillna(False)
        return m
    return _match

# ---------- Core ----------
def run(diagnoses_path, procedures_path, cad_codes_path, cabg_codes_path, outdir):
    outdir = Path(outdir); outdir.mkdir(parents=True, exist_ok=True)

    diag = read_csv_safe(Path(diagnoses_path))
    proc = read_csv_safe(Path(procedures_path))
    cad_list = read_csv_safe(Path(cad_codes_path))
    cabg_list = read_csv_safe(Path(cabg_codes_path))

    ensure_cols(diag, ["subject_id","hadm_id","icd_code"], "diagnoses_icd.csv")
    ensure_cols(proc, ["subject_id","hadm_id","icd_code"], "procedures_icd.csv")

    cad_col  = find_code_col(cad_list)
    cabg_col = find_code_col(cabg_list)

    cad_matcher  = build_matcher(cad_list[cad_col])
    cabg_matcher = build_matcher(cabg_list[cabg_col])

    # Filter to unique pairs
    diag_match = diag[cad_matcher(diag["icd_code"])][["subject_id","hadm_id"]].dropna().drop_duplicates()
    proc_match = proc[cabg_matcher(proc["icd_code"])][["subject_id","hadm_id"]].dropna().drop_duplicates()

    # Normalize id dtype to str
    for df in (diag_match, proc_match):
        df["subject_id"] = df["subject_id"].astype(str)
        df["hadm_id"]    = df["hadm_id"].astype(str)

    # Build intersection sets on subject_id and hadm_id separately
    subj_inter = set(diag_match["subject_id"]) & set(proc_match["subject_id"])
    hadm_inter = set(diag_match["hadm_id"])    & set(proc_match["hadm_id"])

    # Keep any row (from either side) that matches by subject OR by hadm
    both = pd.concat(
        [diag_match.assign(source="diagnosis"), proc_match.assign(source="procedure")],
        ignore_index=True
    ).drop_duplicates()

    keep_mask = both["subject_id"].isin(subj_inter) | both["hadm_id"].isin(hadm_inter)
    loose = both.loc[keep_mask, ["subject_id","hadm_id"]].drop_duplicates().reset_index(drop=True)

    # Save
    cad_out   = outdir / "CAD_matches.csv"
    cabg_out  = outdir / "CABG_matches.csv"
    loose_out = outdir / "CAD_CABG_loose_intersection.csv"

    diag_match.to_csv(cad_out, index=False, encoding="utf-8-sig")
    proc_match.to_csv(cabg_out, index=False, encoding="utf-8-sig")
    loose.to_csv(loose_out, index=False, encoding="utf-8-sig")

    summary = (
        f"CAD (diagnosis) unique pairs: {len(diag_match)}\n"
        f"CABG (procedure) unique pairs: {len(proc_match)}\n"
        f"Loose intersection (subject_id OR hadm_id): {len(loose)}\n"
        f"Saved:\n  {cad_out}\n  {cabg_out}\n  {loose_out}\n"
    )
    (outdir / "summary.txt").write_text(summary, encoding="utf-8")
    print(summary)

def main():
    ap = argparse.ArgumentParser(description="Loose intersection on subject_id OR hadm_id between CAD and CABG matches.")
    ap.add_argument("--diagnoses", default="diagnoses_icd.csv")
    ap.add_argument("--procedures", default="procedures_icd.csv")
    ap.add_argument("--cad_codes", default="cad_icd.csv")
    ap.add_argument("--cabg_codes", default="cabg_icd.csv")
    ap.add_argument("--outdir", default="icd_outputs")
    args, _ = ap.parse_known_args()  # tolerate Jupyter's -f

    run(args.diagnoses, args.procedures, args.cad_codes, args.cabg_codes, args.outdir)

if __name__ == "__main__":
    main()
