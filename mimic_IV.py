# -*- coding: utf-8 -*-
"""
Build a subject-level dataset restricted to each subject's *first* ICU admission.

Steps
-----
1) From /content/icustays_mimic_IV.csv, pick the first ICU stay per subject (earliest 'intime').
2) Merge the following files by subject_id, keeping only subjects from step 1:
   - /content/CAD_CABG_loose_intersection_comorbidities_hadm_or_subject.csv
   - /content/admissions_subject_hadm_death.csv
   - /content/icustays_mimic_IV.csv                  (used only to restrict subjects)
   - /content/patients_subject_sex_age.csv
   - /content/WEIGHT_filtered.csv
3) Drop all hadm_id columns (any case, including suffixed variants).
4) Recode Sex: M -> 1, F -> 0 (nullable Int64).
5) Save to /content/subject_level_merged_firstICU.csv
"""

import pandas as pd
import numpy as np
from functools import reduce
from pathlib import Path

# ------------------------- I/O paths -------------------------
ICU_PATH   = Path("/content/icustays_mimic_IV.csv")
COMORB_PATH = Path("/content/CAD_CABG_loose_intersection_comorbidities_hadm_or_subject.csv")
ADM_PATH    = Path("/content/admissions_subject_hadm_death.csv")
PAT_PATH    = Path("/content/patients_subject_sex_age.csv")
WT_PATH     = Path("/content/WEIGHT_filtered.csv")
OUT_PATH    = Path("/content/subject_level_merged_firstICU.csv")

# ------------------------- Helpers -------------------------
def normalize_id_series(s: pd.Series) -> pd.Series:
    """Normalize ID as string; trim whitespace and trailing '.0'."""
    return (
        s.astype("string")
         .str.strip()
         .str.replace(r"\.0$", "", regex=True)
    )

def drop_hadm_id_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Drop any columns that look like hadm_id (case-insensitive, including suffixed variants)."""
    to_drop = [c for c in df.columns if c.lower() == "hadm_id" or c.lower().endswith("hadm_id")]
    if to_drop:
        df = df.drop(columns=to_drop)
    return df

def read_with_subject(path: Path) -> pd.DataFrame:
    """Read CSV; ensure subject_id exists and normalized; drop hadm_id-like columns."""
    df = pd.read_csv(path)
    # normalize subject_id
    if "subject_id" not in df.columns:
        # try common alternatives (fail with clear error if not found)
        candidates = [c for c in df.columns if c.lower() == "subject_id"]
        if not candidates:
            raise KeyError(f"{path} is missing required column 'subject_id'. Columns found: {list(df.columns)}")
        df.rename(columns={candidates[0]: "subject_id"}, inplace=True)
    df["subject_id"] = normalize_id_series(df["subject_id"])
    # drop hadm_id variants
    df = drop_hadm_id_cols(df)
    return df

def first_icu_per_subject(icu_path: Path) -> pd.DataFrame:
    """
    From icustays, keep the earliest 'intime' per subject.
    Returns a DataFrame with unique subject_id (and keeps 'intime' as the first ICU intime).
    """
    icu = pd.read_csv(icu_path)
    # normalize subject_id
    if "subject_id" not in icu.columns:
        candidates = [c for c in icu.columns if c.lower() == "subject_id"]
        if not candidates:
            raise KeyError(f"{icu_path} is missing 'subject_id'. Columns: {list(icu.columns)}")
        icu.rename(columns={candidates[0]: "subject_id"}, inplace=True)
    icu["subject_id"] = normalize_id_series(icu["subject_id"])

    # pick an 'intime' column (allow a few common variants)
    intime_col = None
    for cand in ["intime", "icu_intime", "in_time", "icu_intime_dt"]:
        if cand in icu.columns:
            intime_col = cand
            break
    if intime_col is None:
        raise KeyError(f"{icu_path} is missing an ICU admission time column (expected one of: 'intime', 'icu_intime').")

    icu[intime_col] = pd.to_datetime(icu[intime_col], errors="coerce")
    # sort by subject then intime asc; NaT go last to avoid being picked as earliest
    icu = icu.sort_values(["subject_id", intime_col], ascending=[True, True], na_position="last")
    # keep first ICU stay per subject
    first_icu = icu.drop_duplicates(subset=["subject_id"], keep="first").copy()
    # slim to subject_id + earliest intime (optional)
    first_icu = first_icu[["subject_id", intime_col]].rename(columns={intime_col: "first_icu_intime"})
    return first_icu

def is_binary_01(s: pd.Series) -> bool:
    """True if numeric and non-null unique values are subset of {0,1}."""
    if not pd.api.types.is_numeric_dtype(s):
        return False
    vals = pd.unique(s.dropna())
    if len(vals) == 0:
        return False
    try:
        return set(pd.Series(vals, dtype="float").unique()).issubset({0.0, 1.0})
    except Exception:
        return False

def dedupe_by_subject(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reduce to one row per subject_id:
      - For binary 0/1 columns -> max (any 1 => 1)
      - For others -> first non-null
    """
    if df.empty:
        return df
    def _agg_group(g: pd.DataFrame) -> pd.Series:
        out = {}
        for c in g.columns:
            if c == "subject_id":
                continue
            s = g[c]
            if is_binary_01(s):
                out[c] = s.max()
            else:
                out[c] = s.dropna().iloc[0] if s.notna().any() else np.nan
        return pd.Series(out)
    rows = []
    for sid, grp in df.groupby("subject_id", dropna=False):
        row = _agg_group(grp)
        row["subject_id"] = sid
        rows.append(row)
    return pd.DataFrame(rows)

def recode_and_collapse_sex(df: pd.DataFrame) -> pd.DataFrame:
    """
    Recode Sex to 1/0 and collapse multiple Sex-like columns into a single 'Sex'.
    - M/m -> 1
    - F/f -> 0
    - Otherwise NA
    """
    sex_like = [c for c in df.columns if c.lower().startswith("sex")]
    if not sex_like:
        return df
    # normalize each Sex-like col
    for c in sex_like:
        if not pd.api.types.is_numeric_dtype(df[c]):
            s = df[c].astype("string").str.strip().str.lower()
            df[c] = s.map({"m": 1, "male": 1, "f": 0, "female": 0})
        df[c] = df[c].astype("Int64")

    # collapse into a single 'Sex' via row-wise max (any 1 => 1; 0 if only 0/NA)
    sex_df = pd.concat([df[c] for c in sex_like], axis=1)
    df["Sex"] = sex_df.max(axis=1, skipna=True).astype("Int64")

    # drop redundant Sex-like columns except the final 'Sex'
    to_drop = [c for c in sex_like if c != "Sex"]
    if to_drop:
        df.drop(columns=to_drop, inplace=True)
    return df

# ------------------------- Main -------------------------
if __name__ == "__main__":
    # 1) Subjects with first ICU stay
    first_icu = first_icu_per_subject(ICU_PATH)  # columns: subject_id, first_icu_intime
    subjects = set(first_icu["subject_id"])

    # 2) Load other files, filter to subjects from first ICU, drop hadm_id cols, and dedupe per subject
    comorb = read_with_subject(COMORB_PATH)
    comorb = comorb[comorb["subject_id"].isin(subjects)]
    comorb = dedupe_by_subject(comorb)

    adm = read_with_subject(ADM_PATH)
    adm = adm[adm["subject_id"].isin(subjects)]
    adm = dedupe_by_subject(adm)

    pat = read_with_subject(PAT_PATH)
    pat = pat[pat["subject_id"].isin(subjects)]
    pat = dedupe_by_subject(pat)

    wt = read_with_subject(WT_PATH)
    wt = wt[wt["subject_id"].isin(subjects)]
    wt = dedupe_by_subject(wt)

    # 3) Merge (left-join) onto the subject list from first ICU
    dfs_to_merge = [first_icu, comorb, adm, pat, wt]
    merged = reduce(lambda l, r: pd.merge(l, r, on="subject_id", how="left"), dfs_to_merge)

    # 4) Drop any hadm_id columns that may have slipped in
    merged = drop_hadm_id_cols(merged)

    # 5) Recode/collapse Sex (M->1, F->0)
    merged = recode_and_collapse_sex(merged)

    # 6) Put ID first; save
    cols = ["subject_id"] + [c for c in merged.columns if c != "subject_id"]
    merged = merged[cols]
    merged.to_csv(OUT_PATH, index=False)

    print(f"Subjects in first ICU cohort: {len(subjects)}")
    print(f"Final rows: {len(merged)}")
    print(f"Saved to: {OUT_PATH}")
