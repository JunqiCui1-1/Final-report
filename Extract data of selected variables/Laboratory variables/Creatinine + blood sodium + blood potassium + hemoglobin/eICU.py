"""
Purpose
-------
Extract four analytes (Creatinine, Sodium, Potassium, Hemoglobin) from an eICU-style
lab table and return a wide, patient-level table with exactly 13 columns:
    patientunitstayid,
    Creatinine, Creatinine_valueuom, Creatinine_charttime,
    Sodium,     Sodium_valueuom,     Sodium_charttime,
    Potassium,  Potassium_valueuom,  Potassium_charttime,
    Hemoglobin, Hemoglobin_valueuom, Hemoglobin_charttime

Selection and cleaning rules (same fixed ranges as before)
----------------------------------------------------------
1) Identify rows by labname matching the analyte (case-insensitive; excludes "urine").
2) Normalize/convert units so the final unit per analyte is:
      - Creatinine: mg/dL
      - Sodium:     mmol/L  (mEq/L treated 1:1 as mmol/L)
      - Potassium:  mmol/L  (mEq/L treated 1:1 as mmol/L)
      - Hemoglobin: g/dL
   Supported conversions:
      - Creatinine:  µmol/L → mg/dL  (divide by 88.4),  mg/L → mg/dL (divide by 10)
      - Sodium:      mg/dL → mmol/L  (× 10 / 22.989 ≈ × 0.434)
      - Potassium:   mg/dL → mmol/L  (× 10 / 39.098 ≈ × 0.2569)
      - Hemoglobin:  g/L → g/dL (divide by 10), mmol/L → g/dL (× 6.45)
3) Clean to NA using fixed valid ranges:
      - Creatinine (mg/dL): 0.1–20.0
      - Sodium (mmol/L):    110–170
      - Potassium (mmol/L): 1.5–8.0
      - Hemoglobin (g/dL):  3–22
   Additionally set values ≤ 0 to NA for Creatinine, Potassium, Hemoglobin.
4) For each analyte, keep the earliest valid measurement per patientunitstayid
   using labresultoffset (minutes) as time.
5) Save the final 13-column CSV.

Input
-----
/content/filtered_by_patientunitstayid_20251009_013746/lab.csv
(Required columns, any case: patientunitstayid, labname, labresult, labresultoffset,
 and a unit column such as labmeasurenamesystem / labmeasurename / labunit / units / valueuom)

Output
------
/content/lab_cleaned_4vars_13cols.csv
"""

import pandas as pd
import numpy as np
import math

PATH_IN  = "/content/filtered_by_patientunitstayid_20251009_013746/lab.csv"
PATH_OUT = "/content/lab_cleaned_4vars_13cols2.csv"

# Load
df = pd.read_csv(PATH_IN, low_memory=False)
df.columns = [c.lower() for c in df.columns]

def pick(df, *candidates):
    for name in candidates:
        for c in df.columns:
            if c.lower() == name.lower():
                return c
    raise KeyError(f"Missing required column. Tried: {', '.join(candidates)}")

col_pid   = pick(df, "patientunitstayid")
col_name  = pick(df, "labname")
col_val   = pick(df, "labresult")
col_time  = pick(df, "labresultoffset")
# Try common unit column names in eICU exports
col_unit  = pick(df, "labmeasurenamesystem", "labmeasurename", "labunit", "labresultunit",
                 "labresultunits", "units", "unit", "valueuom")

# Basic normalization
df[col_val]  = pd.to_numeric(df[col_val], errors="coerce")
df[col_time] = pd.to_numeric(df[col_time], errors="coerce")  # offset in minutes
df[col_unit] = df[col_unit].astype(str).str.strip().str.lower()
df[col_name] = df[col_name].astype(str).str.strip().str.lower()

# Target definitions
TARGETS = {
    "Creatinine": {
        "patterns": ["creatin"],  # match "creatinine" variants
        "exclude":  ["urine"],
        "out_unit": "mg/dL",
        "range": (0.1, 20.0)
    },
    "Sodium": {
        "patterns": ["sodium", r"\bna\b", "na+"],
        "exclude":  ["urine"],
        "out_unit": "mmol/L",
        "range": (110.0, 170.0)
    },
    "Potassium": {
        "patterns": ["potassium", r"\bk\b", "k+"],
        "exclude":  ["urine"],
        "out_unit": "mmol/L",
        "range": (1.5, 8.0)
    },
    "Hemoglobin": {
        "patterns": ["hemoglobin", r"\bhgb\b", r"\bhb\b"],
        "exclude":  ["urine"],
        "out_unit": "g/dL",
        "range": (3.0, 22.0)
    },
}

def name_mask(s: pd.Series, includes, excludes):
    m = False
    for kw in includes:
        m = m | s.str.contains(kw, case=False, regex=True, na=False)
    for ex in excludes:
        m = m & ~s.str.contains(ex, case=False, regex=True, na=False)
    return m

def convert_units(analyte: str, val: pd.Series, unit: pd.Series):
    u = unit.copy()
    x = val.copy()

    # Normalize unicode micro symbol and variants
    u = (u.str.replace("µ", "u", regex=False)
           .str.replace("μ", "u", regex=False))

    # Standardize common spellings
    u = (u.str.replace("milliequivalents/l", "meq/l", regex=False)
           .str.replace("milliequivalent/l", "meq/l", regex=False)
           .str.replace("millimoles/l", "mmol/l", regex=False)
           .str.replace("millimole/l", "mmol/l", regex=False)
           .str.replace("gram/l", "g/l", regex=False)
           .str.replace("grams/l", "g/l", regex=False)
           .str.replace("gram/dl", "g/dl", regex=False)
           .str.replace("grams/dl", "g/dl", regex=False))

    if analyte == "Creatinine":
        # Desired: mg/dL
        # Supported inputs: mg/dl (ok), mg/l, umol/l
        mask_mgdl = u.eq("mg/dl")
        mask_mgl  = u.eq("mg/l")
        mask_umol = u.eq("umol/l")

        # mg/L -> mg/dL (divide by 10)
        x.loc[mask_mgl] = x.loc[mask_mgl] / 10.0
        # umol/L -> mg/dL (divide by 88.4)
        x.loc[mask_umol] = x.loc[mask_umol] / 88.4

        # After conversion, set unit to mg/dL where we accepted/converted
        accepted = mask_mgdl | mask_mgl | mask_umol
        u.loc[accepted] = "mg/dl"
        return x, u

    if analyte in ("Sodium", "Potassium"):
        # Desired: mmol/L (treat mEq/L 1:1)
        mask_mmol = u.eq("mmol/l")
        mask_meq  = u.eq("meq/l")
        mask_mgdl = u.eq("mg/dl")

        if analyte == "Sodium":
            # mg/dL -> mmol/L: × 10 / 22.989
            x.loc[mask_mgdl] = x.loc[mask_mgdl] * (10.0 / 22.989)
        else:
            # Potassium mg/dL -> mmol/L: × 10 / 39.098
            x.loc[mask_mgdl] = x.loc[mask_mgdl] * (10.0 / 39.098)

        accepted = mask_mmol | mask_meq | mask_mgdl
        u.loc[accepted] = "mmol/l"
        return x, u

    if analyte == "Hemoglobin":
        # Desired: g/dL
        mask_gdl  = u.eq("g/dl")
        mask_gl   = u.eq("g/l")
        mask_mmol = u.eq("mmol/l")

        # g/L -> g/dL (divide by 10)
        x.loc[mask_gl] = x.loc[mask_gl] / 10.0
        # mmol/L -> g/dL (× 6.45)
        x.loc[mask_mmol] = x.loc[mask_mmol] * 6.45

        accepted = mask_gdl | mask_gl | mask_mmol
        u.loc[accepted] = "g/dl"
        return x, u

    return x, u

def extract_one(analyte: str, out_val: str, out_unit: str, out_time: str):
    inc = TARGETS[analyte]["patterns"]
    exc = TARGETS[analyte]["exclude"]
    lo, hi = TARGETS[analyte]["range"]
    desired_unit = TARGETS[analyte]["out_unit"]

    d = df.loc[name_mask(df[col_name], inc, exc), [col_pid, col_time, col_val, col_unit]].copy()

    # Convert/normalize units to desired unit
    d[col_val], d[col_unit] = convert_units(analyte, d[col_val], d[col_unit])

    # Only keep rows now in desired unit
    keep_unit = desired_unit.lower()
    d = d[d[col_unit].eq(keep_unit)]

    # Clean non-positive where appropriate
    if analyte in ("Creatinine", "Potassium", "Hemoglobin"):
        d.loc[d[col_val] <= 0, col_val] = np.nan

    # Fixed-range clean to NA
    d.loc[(d[col_val] < lo) | (d[col_val] > hi), col_val] = np.nan

    # Drop rows lacking both value and time
    d = d.dropna(subset=[col_val, col_time])

    # Earliest by offset per patient
    d = d.sort_values([col_pid, col_time]).groupby(col_pid, as_index=False).first()

    # Rename output columns; set pretty unit label case
    d = d.rename(columns={
        col_val: out_val,
        col_unit: out_unit,
        col_time: out_time
    })
    d[out_unit] = desired_unit  # canonical case

    return d[[col_pid, out_val, out_unit, out_time]]

# Build four analyte tables
cr  = extract_one("Creatinine", "Creatinine", "Creatinine_valueuom", "Creatinine_charttime")
na_ = extract_one("Sodium",     "Sodium",     "Sodium_valueuom",     "Sodium_charttime")
k_  = extract_one("Potassium",  "Potassium",  "Potassium_valueuom",  "Potassium_charttime")
hgb = extract_one("Hemoglobin", "Hemoglobin", "Hemoglobin_valueuom", "Hemoglobin_charttime")

# Merge to 13-column wide table
out = (
    cr.merge(na_,  how="outer", on=col_pid)
      .merge(k_,   how="outer", on=col_pid)
      .merge(hgb,  how="outer", on=col_pid)
)

# Order columns explicitly and save
cols = [
    col_pid,
    "Creatinine", "Creatinine_valueuom", "Creatinine_charttime",
    "Sodium",     "Sodium_valueuom",     "Sodium_charttime",
    "Potassium",  "Potassium_valueuom",  "Potassium_charttime",
    "Hemoglobin", "Hemoglobin_valueuom", "Hemoglobin_charttime",
]
out = out.reindex(columns=cols)
out.to_csv(PATH_OUT, index=False)

print(f"Saved: {PATH_OUT}")
print(f"Shape: {out.shape}")
print(out.head(10))
