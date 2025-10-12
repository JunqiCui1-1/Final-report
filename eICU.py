# -*- coding: utf-8 -*-
"""
Merge four CSVs by `patientunitstayid`, recode Sex (Male→1, Female→0), and
deduplicate by ID with sensible aggregation rules.

Inputs
------
/content/patient_eICU.csv
/content/patientunitstayid_Death.csv
/content/patientunitstayid_Sex_Age_Weight.csv
/content/patientunitstayid_comorbidities.csv

Output
------
/content/patientunitstayid_merged.csv

Rules
-----
- Join key: patientunitstayid (outer join to keep all IDs).
- Sex recode: Male/male/M -> 1; Female/female/F -> 0. Keep NA if unknown.
- If multiple rows per ID:
    * For binary 0/1 columns: take max (any 1 => 1).
    * For non-binary columns: take first non-null value.
"""

import pandas as pd
import numpy as np
from functools import reduce
from pathlib import Path

# ------------------------- I/O paths -------------------------
P1 = Path("/content/patient_eICU.csv")
P2 = Path("/content/patientunitstayid_Death.csv")
P3 = Path("/content/patientunitstayid_Sex_Age_Weight.csv")
P4 = Path("/content/patientunitstayid_comorbidities.csv")
OUT = Path("/content/patientunitstayid_merged.csv")


# ------------------------- Helpers -------------------------
def _normalize_id(s: pd.Series) -> pd.Series:
    """Normalize `patientunitstayid` as string, stripping whitespace and trailing '.0'."""
    return (
        s.astype("string")
         .str.strip()
         .str.replace(r"\.0$", "", regex=True)
    )


def read_with_id(path: Path) -> pd.DataFrame:
    """Read a CSV and ensure it contains a normalized `patientunitstayid` column."""
    df = pd.read_csv(path, dtype={"patientunitstayid": "string"})
    if "patientunitstayid" not in df.columns:
        raise KeyError(f"{path} is missing required column: 'patientunitstayid'")
    df["patientunitstayid"] = _normalize_id(df["patientunitstayid"])
    return df


def recode_sex_columns_inplace(df: pd.DataFrame) -> None:
    """
    Recode any column named (case-insensitive) 'sex':
      - Male/male/M -> 1
      - Female/female/F -> 0
      - Preserve NA for unknown/other values
    If the column is already numeric, cast to nullable integer.
    """
    sex_like = [c for c in df.columns if c.lower() == "sex"]
    for c in sex_like:
        if pd.api.types.is_numeric_dtype(df[c]):
            df[c] = df[c].astype("Int64")
            continue
        s = df[c].astype("string").str.strip().str.lower()
        df[c] = (
            s.map({"male": 1, "m": 1, "female": 0, "f": 0})
             .astype("Int64")
        )


def is_binary_01(s: pd.Series) -> bool:
    """Return True if series is numeric and its non-null unique values are a subset of {0, 1}."""
    if not pd.api.types.is_numeric_dtype(s):
        return False
    vals = pd.unique(s.dropna())
    if len(vals) == 0:
        return False
    try:
        return set(pd.Series(vals, dtype="float").unique()).issubset({0.0, 1.0})
    except Exception:
        return False


def aggregate_group(g: pd.DataFrame) -> pd.Series:
    """
    Aggregate rows within a single patientunitstayid:
      - Binary 0/1 columns -> max
      - Others -> first non-null
    """
    out = {}
    for col in g.columns:
        if col == "patientunitstayid":
            continue
        s = g[col]
        if is_binary_01(s):
            out[col] = s.max()
        else:
            out[col] = s.dropna().iloc[0] if s.notna().any() else np.nan
    return pd.Series(out)


# ------------------------- Main -------------------------
if __name__ == "__main__":
    # 1) Read all CSVs and recode Sex (if present in each)
    dfs = [read_with_id(p) for p in (P1, P2, P3, P4)]
    for d in dfs:
        recode_sex_columns_inplace(d)

    # 2) Outer-join on patientunitstayid
    merged = reduce(lambda l, r: pd.merge(l, r, on="patientunitstayid", how="outer"), dfs)

    # 3) Collapse multiple Sex-like columns into a single 'Sex' column if needed
    sex_like_cols = [c for c in merged.columns if c.lower().startswith("sex")]
    if len(sex_like_cols) > 1:
        # Re-ensure all Sex-like columns are 0/1/NA
        for c in sex_like_cols:
            if not pd.api.types.is_numeric_dtype(merged[c]):
                s = merged[c].astype("string").str.strip().str.lower()
                merged[c] = s.map({"male": 1, "m": 1, "female": 0, "f": 0})
            merged[c] = merged[c].astype("Int64")

        # Row-wise max across Sex-like columns -> final 'Sex'
        merged["Sex"] = pd.concat([merged[c] for c in sex_like_cols], axis=1).max(axis=1, skipna=True).astype("Int64")

        # Drop redundant Sex-like columns, keep only 'Sex'
        merged.drop(columns=[c for c in sex_like_cols if c != "Sex"], inplace=True)

    # 4) Deduplicate by patientunitstayid with custom aggregation
    rows = []
    for pid, grp in merged.groupby("patientunitstayid", dropna=False):
        row = aggregate_group(grp)
        row["patientunitstayid"] = pid
        rows.append(row)
    result = pd.DataFrame(rows)

    # 5) Ensure final Sex dtype
    if "Sex" in result.columns:
        result["Sex"] = result["Sex"].astype("Int64")

    # 6) Save
    result = result[["patientunitstayid"] + [c for c in result.columns if c != "patientunitstayid"]]  # put ID first
    result.to_csv(OUT, index=False)
    print(f"Done. Saved to: {OUT}")
