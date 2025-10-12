# -*- coding: utf-8 -*-
"""
MIMIC-IV labevents → four long tables (no collapsing)
Each CSV has 4 columns:
  subject_id, <Analyte>, <Analyte>_valueuom, <Analyte>_charttime
Analytes (ITEMID): Creatinine(50912), Sodium(50983), Potassium(50971), Hemoglobin(51222)
Units kept/normalized and ranges cleaned as agreed. All rows kept (no per-subject earliest).
"""

import pandas as pd
import numpy as np

# -------- Paths --------
PATH_IN = "/content/labevents.csv"

OUT_CRE = "/content/creatinine_long_4cols.csv"
OUT_NA  = "/content/sodium_long_4cols.csv"
OUT_K   = "/content/potassium_long_4cols.csv"
OUT_HGB = "/content/hemoglobin_long_4cols.csv"

# -------- Fixed params --------
ITEMIDS = {
    "Creatinine": 50912,   # mg/dL
    "Sodium":     50983,   # mmol/L or mEq/L
    "Potassium":  50971,   # mmol/L or mEq/L
    "Hemoglobin": 51222,   # g/dL
}

UNIT_ALLOW = {
    "Creatinine": {"mg/dl"},
    "Sodium":     {"mmol/l", "meq/l"},   # treated 1:1; output "mmol/L"
    "Potassium":  {"mmol/l", "meq/l"},   # treated 1:1; output "mmol/L"
    "Hemoglobin": {"g/dl"},
}

RANGE = {
    "Creatinine": (0.1, 20.0),
    "Sodium":     (110.0, 170.0),
    "Potassium":  (1.5, 8.0),
    "Hemoglobin": (3.0, 22.0),
}

OUT_UNIT = {
    "Creatinine": "mg/dL",
    "Sodium":     "mmol/L",
    "Potassium":  "mmol/L",
    "Hemoglobin": "g/dL",
}

# -------- Load --------
df = pd.read_csv(PATH_IN, low_memory=False)
df.columns = [c.lower() for c in df.columns]

def pick(df, name):
    for c in df.columns:
        if c.lower() == name.lower():
            return c
    raise KeyError(f"Missing required column: {name}")

col_subj     = pick(df, "subject_id")
col_itemid   = pick(df, "itemid")
col_value    = pick(df, "valuenum")
col_valueuom = pick(df, "valueuom")
col_time     = pick(df, "charttime")

# Basic typing / normalization
df[col_value]    = pd.to_numeric(df[col_value], errors="coerce")
df[col_time]     = pd.to_datetime(df[col_time], errors="coerce")
df[col_valueuom] = df[col_valueuom].astype(str).str.strip().str.lower()

def extract_long(analyte: str, out_csv_path: str):
    """Filter by itemid and allowed units, clean by fixed ranges, keep ALL rows."""
    iid = ITEMIDS[analyte]
    allowed = {u.lower() for u in UNIT_ALLOW[analyte]}
    lo, hi = RANGE[analyte]
    out_unit_canonical = OUT_UNIT[analyte]

    d = df.loc[df[col_itemid] == iid, [col_subj, col_time, col_value, col_valueuom]].copy()

    # Keep only allowed units
    d = d[d[col_valueuom].isin(allowed)]

    # Normalize Na/K unit label to mmol/L (values unchanged, mEq/L==mmol/L)
    if analyte in ("Sodium", "Potassium"):
        d.loc[:, col_valueuom] = "mmol/l"

    # Non-positive to NA where appropriate
    if analyte in ("Creatinine", "Hemoglobin", "Potassium"):
        d.loc[d[col_value] <= 0, col_value] = np.nan

    # Fixed range cleaning
    d.loc[(d[col_value] < lo) | (d[col_value] > hi), col_value] = np.nan

    # Drop rows lacking value or time
    d = d.dropna(subset=[col_value, col_time])

    # Sort for readability; DO NOT collapse — keep all rows
    d = d.sort_values([col_subj, col_time])

    # Rename columns for output and set canonical unit label
    out_val  = analyte
    out_unit = f"{analyte}_valueuom"
    out_time = f"{analyte}_charttime"

    d = d.rename(columns={
        col_subj:     "subject_id",
        col_value:    out_val,
        col_valueuom: out_unit,
        col_time:     out_time
    })
    d[out_unit] = out_unit_canonical

    d = d[["subject_id", out_val, out_unit, out_time]]
    d.to_csv(out_csv_path, index=False)
    print(f"{analyte}: saved {out_csv_path}, shape={d.shape}")

# ---- Run for each analyte ----
extract_long("Creatinine", OUT_CRE)
extract_long("Sodium",     OUT_NA)
extract_long("Potassium",  OUT_K)
extract_long("Hemoglobin", OUT_HGB)
