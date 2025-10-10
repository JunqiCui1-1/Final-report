"""
Purpose
-------
Extract four lab analytes (Creatinine, Sodium, Potassium, Hemoglobin) from a single
labevents table and produce a wide, subject-level table with exactly 13 columns:
    subject_id,
    Creatinine, Creatinine_valueuom, Creatinine_charttime,
    Sodium,     Sodium_valueuom,     Sodium_charttime,
    Potassium,  Potassium_valueuom,  Potassium_charttime,
    Hemoglobin, Hemoglobin_valueuom, Hemoglobin_charttime

Rules
-----
1) Use one ItemID per analyte (the most frequent blood item in your data):
      - Creatinine: itemid=50912  (mg/dL)
      - Sodium:     itemid=50983  (mmol/L or mEq/L, 1:1 numerically)
      - Potassium:  itemid=50971  (mmol/L or mEq/L, 1:1 numerically)
      - Hemoglobin: itemid=51222  (g/dL)
2) Keep only allowed units:
      - Creatinine: mg/dL
      - Sodium:     mmol/L or mEq/L (treated equivalently; output normalized to "mmol/L")
      - Potassium:  mmol/L or mEq/L (treated equivalently; output normalized to "mmol/L")
      - Hemoglobin: g/dL
3) Clean to NA using fixed valid ranges:
      - Creatinine (mg/dL): 0.1–20.0
      - Sodium (mmol/L):    110–170
      - Potassium (mmol/L): 1.5–8.0
      - Hemoglobin (g/dL):  3–22
   Additionally, for Creatinine/Hemoglobin/Potassium, values ≤0 are set to NA.
4) For each analyte, select the earliest valid measurement per subject_id.
5) Save the 13-column wide table to CSV at the end.

Input
-----
/content/filtered_by_ids_20251008_090531/labevents.csv
(required columns: subject_id, itemid, valuenum, valueuom, charttime; any case is accepted)

Output
------
/content/labs_cleaned_4vars_13cols.csv
"""

import pandas as pd
import numpy as np

# Paths
PATH_IN  = "/content/filtered_by_ids_20251008_090531/labevents.csv"
PATH_OUT = "/content/labs_cleaned_4vars_13cols.csv"

# Fixed ItemIDs (most frequent blood items from your dataset)
ITEMIDS = {
    "Creatinine": 50912,   # mg/dL
    "Sodium":     50983,   # mmol/L or mEq/L
    "Potassium":  50971,   # mmol/L or mEq/L
    "Hemoglobin": 51222,   # g/dL
}

# Allowed units (lowercase for matching)
UNIT_ALLOW = {
    "Creatinine": {"mg/dl"},
    "Sodium":     {"mmol/l", "meq/l"},
    "Potassium":  {"mmol/l", "meq/l"},
    "Hemoglobin": {"g/dl"},
}

# Cleaning ranges
RANGE = {
    "Creatinine": (0.1, 20.0),
    "Sodium":     (110.0, 170.0),
    "Potassium":  (1.5, 8.0),
    "Hemoglobin": (3.0, 22.0),
}

# Friendly output unit labels (normalized)
OUT_UNIT = {
    "Creatinine": "mg/dL",
    "Sodium":     "mmol/L",
    "Potassium":  "mmol/L",
    "Hemoglobin": "g/dL",
}

# Load input
df = pd.read_csv(PATH_IN, low_memory=False)

# Normalize column names to lowercase for robust access
df.columns = [c.lower() for c in df.columns]

def pick(df, name):
    for c in df.columns:
        if c.lower() == name.lower():
            return c
    return None

col_subj     = pick(df, "subject_id")
col_itemid   = pick(df, "itemid")
col_value    = pick(df, "valuenum")
col_valueuom = pick(df, "valueuom")
col_time     = pick(df, "charttime")

# Basic type handling
df[col_value] = pd.to_numeric(df[col_value], errors="coerce")
df[col_time]  = pd.to_datetime(df[col_time], errors="coerce")
df[col_valueuom] = df[col_valueuom].astype(str).str.strip().str.lower()

def extract_one(df, analyte, out_value_col, out_unit_col, out_time_col):
    """Filter to one itemid, allowed units, clean to NA by fixed ranges,
    and keep the earliest valid measurement per subject_id."""
    iid = ITEMIDS[analyte]
    allowed = UNIT_ALLOW[analyte]
    lo, hi = RANGE[analyte]

    d = df.loc[df[col_itemid] == iid, [col_subj, col_time, col_value, col_valueuom]].copy()
    d = d[d[col_valueuom].isin(allowed)]

    # Normalize equivalent units for Na/K to mmol/L (display)
    if analyte in ("Sodium", "Potassium"):
        d.loc[d[col_valueuom].isin({"mmol/l", "meq/l"}), col_valueuom] = "mmol/l"

    # Additional non-positive to NA where appropriate
    if analyte in ("Creatinine", "Hemoglobin", "Potassium"):
        d.loc[d[col_value] <= 0, col_value] = np.nan

    # Range clean to NA
    d.loc[(d[col_value] < lo) | (d[col_value] > hi), col_value] = np.nan

    # Keep rows with both value and time
    d = d.dropna(subset=[col_value, col_time])

    # Earliest per subject_id
    d = d.sort_values([col_subj, col_time]).groupby(col_subj, as_index=False).first()

    # Rename for output
    d = d.rename(columns={
        col_value: out_value_col,
        col_valueuom: out_unit_col,
        col_time: out_time_col
    })

    # Normalize unit label case for output
    d[out_unit_col] = OUT_UNIT[analyte]

    return d[[col_subj, out_value_col, out_unit_col, out_time_col]]

# Build four analyte tables
cr  = extract_one(df, "Creatinine", "Creatinine", "Creatinine_valueuom", "Creatinine_charttime")
na_ = extract_one(df, "Sodium",     "Sodium",     "Sodium_valueuom",     "Sodium_charttime")
k_  = extract_one(df, "Potassium",  "Potassium",  "Potassium_valueuom",  "Potassium_charttime")
hgb = extract_one(df, "Hemoglobin", "Hemoglobin", "Hemoglobin_valueuom", "Hemoglobin_charttime")

# Merge to 13-column wide table
out = (
    cr.merge(na_,  how="outer", on=col_subj)
      .merge(k_,   how="outer", on=col_subj)
      .merge(hgb,  how="outer", on=col_subj)
)

# Reorder columns explicitly
cols = [
    col_subj,
    "Creatinine", "Creatinine_valueuom", "Creatinine_charttime",
    "Sodium",     "Sodium_valueuom",     "Sodium_charttime",
    "Potassium",  "Potassium_valueuom",  "Potassium_charttime",
    "Hemoglobin", "Hemoglobin_valueuom", "Hemoglobin_charttime",
]
out = out.reindex(columns=cols)

# Save to CSV
out.to_csv(PATH_OUT, index=False)

# Optional console feedback
print(f"Saved: {PATH_OUT}")
print(f"Shape: {out.shape}")
print(out.head(10))
