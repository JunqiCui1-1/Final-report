# -*- coding: utf-8 -*-
"""
Build a subject-level MIMIC-III dataset restricted to each subject's *first* ICU admission.

Inputs
------
/content/ICUSTAYS_mimic_III.csv
/content/ITEMID226512_filtered.csv
/content/PATIENTS_mimic_III.csv
/content/mimic_III_death.csv
/content/patient_ids_SUBJECT_HADM_comorbidities_or.csv

Output
------
/content/subject_level_firstICU_MIMICIII.csv

Rules
-----
1) From ICUSTAYS_mimic_III.csv keep the earliest ICU stay per SUBJECT_ID (min INTIME).
2) Merge all sources by SUBJECT_ID, restricted to the cohort from step 1.
3) Drop any HADM_ID-like columns (case-insensitive, including merge-suffixed variants).
4) Recode sex/gender to 1/0: M->1, F->0 (nullable Int64), collapsed into a single 'sex' column.
5) When multiple rows exist per SUBJECT_ID within a file:
   - Binary 0/1 columns -> take max (any 1 => 1)
   - Other columns -> take first non-null value
"""

import pandas as pd
import numpy as np
from functools import reduce
from pathlib import Path

# ------------------------- I/O paths -------------------------
ICU_PATH   = Path("/content/ICUSTAYS_mimic_III.csv")
ITEM_PATH  = Path("/content/ITEMID226512_filtered.csv")
PAT_PATH   = Path("/content/PATIENTS_mimic_III.csv")
DEATH_PATH = Path("/content/mimic_III_death.csv")
COMORB_PATH= Path("/content/patient_ids_SUBJECT_HADM_comorbidities_or.csv")
OUT_PATH   = Path("/content/subject_level_firstICU_MIMICIII.csv")

# ------------------------- Helpers -------------------------
def normalize_id(s: pd.Series) -> pd.Series:
    """Normalize SUBJECT_ID as string; trim whitespace and trailing '.0'."""
    return (
        s.astype("string")
         .str.strip()
         .str.replace(r"\.0$", "", regex=True)
    )

def drop_hadm_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Drop any columns whose name contains 'hadm_id' (case-insensitive)."""
    to_drop = [c for c in df.columns if "hadm_id" in c.lower()]
    return df.drop(columns=to_drop) if to_drop else df

def read_with_subject(path: Path) -> pd.DataFrame:
    """Read CSV; ensure SUBJECT_ID exists (case-insensitive), normalize, and drop HADM_ID-like cols."""
    df = pd.read_csv(path)
    # unify SUBJECT_ID column name
    if "SUBJECT_ID" not in df.columns:
        candidates = [c for c in df.columns if c.lower() == "subject_id"]
        if not candidates:
            raise KeyError(f"{path} is missing 'SUBJECT_ID'. Columns: {list(df.columns)}")
        df = df.rename(columns={candidates[0]: "SUBJECT_ID"})
    df["SUBJECT_ID"] = normalize_id(df["SUBJECT_ID"])
    df = drop_hadm_cols(df)
    return df

def first_icu_per_subject(icu_path: Path) -> pd.DataFrame:
    """Keep earliest ICU stay per SUBJECT_ID based on INTIME."""
    icu = pd.read_csv(icu_path)
    # SUBJECT_ID
    if "SUBJECT_ID" not in icu.columns:
        candidates = [c for c in icu.columns if c.lower() == "subject_id"]
        if not candidates:
            raise KeyError(f"{icu_path} is missing 'SUBJECT_ID'. Columns: {list(icu.columns)}")
        icu = icu.rename(columns={candidates[0]: "SUBJECT_ID"})
    icu["SUBJECT_ID"] = normalize_id(icu["SUBJECT_ID"])
    # INTIME
    intime_col = None
    for cand in ["INTIME", "ICU_INTIME", "IN_TIME", "ADMITTIME"]:
        if cand in icu.columns:
            intime_col = cand
            break
    if intime_col is None:
        raise KeyError(f"{icu_path} is missing an ICU admission time column (e.g., 'INTIME').")
    icu[intime_col] = pd.to_datetime(icu[intime_col], errors="coerce")
    # sort then keep first occurrence per subject
    icu = icu.sort_values(["SUBJECT_ID", intime_col], ascending=[True, True], na_position="last")
    first_icu = icu.drop_duplicates(subset=["SUBJECT_ID"], keep="first").copy()
    # keep only SUBJECT_ID and the earliest intime
    first_icu = first_icu[["SUBJECT_ID", intime_col]].rename(columns={intime_col: "FIRST_ICU_INTIME"})
    return first_icu

def is_binary_01(s: pd.Series) -> bool:
    """True if series is numeric and non-null uniques are subset of {0,1}."""
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
    Reduce to one row per SUBJECT_ID:
      - Binary 0/1 columns -> max (any 1 => 1)
      - Others -> first non-null
    """
    if df.empty:
        return df
    def _agg_group(g: pd.DataFrame) -> pd.Series:
        out = {}
        for c in g.columns:
            if c == "SUBJECT_ID":
                continue
            s = g[c]
            if is_binary_01(s):
                out[c] = s.max()
            else:
                out[c] = s.dropna().iloc[0] if s.notna().any() else np.nan
        return pd.Series(out)
    rows = []
    for sid, grp in df.groupby("SUBJECT_ID", dropna=False):
        row = _agg_group(grp)
        row["SUBJECT_ID"] = sid
        rows.append(row)
    return pd.DataFrame(rows)

def recode_and_collapse_sex(df: pd.DataFrame) -> pd.DataFrame:
    """
    Recode any sex/gender columns into a single 'sex' (nullable Int64):
      - M/m/male -> 1
      - F/f/female -> 0
      - else -> NA
    """
    # collect columns that look like SEX or GENDER (including merge suffixes)
    candidates = [c for c in df.columns if any(k in c.lower() for k in ["sex", "gender"])]
    if not candidates:
        return df
    # normalize each candidate
    for c in candidates:
        if not pd.api.types.is_numeric_dtype(df[c]):
            s = df[c].astype("string").str.strip().str.lower()
            df[c] = s.map({"m": 1, "male": 1, "f": 0, "female": 0})
        df[c] = df[c].astype("Int64")
    # collapse to single 'sex' (row-wise max => any 1 becomes 1; 0 if only 0/NA)
    sex_df = pd.concat([df[c] for c in candidates], axis=1)
    df["sex"] = sex_df.max(axis=1, skipna=True).astype("Int64")
    # drop redundant originals except final 'sex'
    to_drop = [c for c in candidates if c != "sex"]
    if to_drop:
        df.drop(columns=to_drop, inplace=True)
    return df

# ------------------------- Main -------------------------
if __name__ == "__main__":
    # 1) First ICU stay per subject (and subject cohort)
    first_icu = first_icu_per_subject(ICU_PATH)  # SUBJECT_ID, FIRST_ICU_INTIME
    cohort = set(first_icu["SUBJECT_ID"])

    # 2) Load other sources, restrict to cohort, drop HADM_ID, and dedupe per SUBJECT_ID
    item  = read_with_subject(ITEM_PATH)
    item  = item[item["SUBJECT_ID"].isin(cohort)]
    item  = dedupe_by_subject(item)

    pat   = read_with_subject(PAT_PATH)
    pat   = pat[pat["SUBJECT_ID"].isin(cohort)]
    pat   = dedupe_by_subject(pat)

    death = read_with_subject(DEATH_PATH)
    death = death[death["SUBJECT_ID"].isin(cohort)]
    death = dedupe_by_subject(death)

    comorb = read_with_subject(COMORB_PATH)
    comorb = comorb[comorb["SUBJECT_ID"].isin(cohort)]
    comorb = dedupe_by_subject(comorb)

    # 3) Merge onto the first-ICU subject list (left joins keep cohort only)
    dfs_to_merge = [first_icu, item, pat, death, comorb]
    merged = reduce(lambda l, r: pd.merge(l, r, on="SUBJECT_ID", how="left"), dfs_to_merge)

    # 4) Drop any HADM_ID columns that may remain after merges
    merged = drop_hadm_cols(merged)

    # 5) Recode/collapse sex -> 1/0 into a single 'sex' column
    merged = recode_and_collapse_sex(merged)

    # 6) Order columns, save
    merged = merged[["SUBJECT_ID"] + [c for c in merged.columns if c != "SUBJECT_ID"]]
    merged.to_csv(OUT_PATH, index=False)

    print(f"Subjects in first-ICU cohort: {len(cohort)}")
    print(f"Final rows: {len(merged)}")
    print(f"Saved to: {OUT_PATH}")
