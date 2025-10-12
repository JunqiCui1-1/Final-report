# -*- coding: utf-8 -*-
"""
Purpose
-------
From /content/lab.csv (eICU-style), extract four analytes (Creatinine, Sodium,
Potassium, Hemoglobin). For each analyte, output a LONG table that keeps *all*
valid measurements (no per-patient collapsing), with exactly 4 columns:
    patientunitstayid, <Analyte>, <Analyte>_valueuom, <Analyte>_charttime

Unit normalization and cleaning follow the same fixed rules as before.

Input
-----
/content/lab.csv
Required columns (any case): patientunitstayid, labname, labresult, labresultoffset,
and one unit column among: labmeasurenamesystem / labmeasurename / labunit /
labresultunit / labresultunits / units / unit / valueuom

Outputs (4 files)
-----------------
/content/creatinine_long_4cols.csv
/content/sodium_long_4cols.csv
/content/potassium_long_4cols.csv
/content/hemoglobin_long_4cols.csv
"""

import pandas as pd
import numpy as np

PATH_IN = "/content/lab.csv"

OUT_CRE = "/content/creatinine_long_4cols.csv"
OUT_NA  = "/content/sodium_long_4cols.csv"
OUT_K   = "/content/potassium_long_4cols.csv"
OUT_HGB = "/content/hemoglobin_long_4cols.csv"

# -------- Load --------
df = pd.read_csv(PATH_IN, low_memory=False)
df.columns = [c.lower() for c in df.columns]

def pick(df, *candidates):
    for name in candidates:
        for c in df.columns:
            if c.lower() == name.lower():
                return c
    raise KeyError(f"Missing required column. Tried: {', '.join(candidates)}")

col_pid  = pick(df, "patientunitstayid")
col_name = pick(df, "labname")
col_val  = pick(df, "labresult")
col_time = pick(df, "labresultoffset")
col_unit = pick(df, "labmeasurenamesystem", "labmeasurename", "labunit",
                "labresultunit", "labresultunits", "units", "unit", "valueuom")

# Basic normalization
df[col_val]  = pd.to_numeric(df[col_val], errors="coerce")
df[col_time] = pd.to_numeric(df[col_time], errors="coerce")  # offset in minutes
df[col_unit] = df[col_unit].astype(str).str.strip().str.lower()
df[col_name] = df[col_name].astype(str).str.strip().str.lower()

# -------- Target definitions --------
TARGETS = {
    "Creatinine": {
        "patterns": ["creatin"],  # match "creatinine" variants
        "exclude":  ["urine"],
        "out_unit": "mg/dl",
        "range": (0.1, 20.0)
    },
    "Sodium": {
        "patterns": ["sodium", r"\bna\b", "na+"],
        "exclude":  ["urine"],
        "out_unit": "mmol/l",
        "range": (110.0, 170.0)
    },
    "Potassium": {
        "patterns": ["potassium", r"\bk\b", "k+"],
        "exclude":  ["urine"],
        "out_unit": "mmol/l",
        "range": (1.5, 8.0)
    },
    "Hemoglobin": {
        "patterns": ["hemoglobin", r"\bhgb\b", r"\bhb\b"],
        "exclude":  ["urine"],
        "out_unit": "g/dl",
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

    # Normalize micro symbol variants
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
        mask_mgdl = u.eq("mg/dl")
        mask_mgl  = u.eq("mg/l")
        mask_umol = u.eq("umol/l")

        x.loc[mask_mgl]  = x.loc[mask_mgl]  / 10.0       # mg/L -> mg/dL
        x.loc[mask_umol] = x.loc[mask_umol] / 88.4       # umol/L -> mg/dL

        accepted = mask_mgdl | mask_mgl | mask_umol
        u.loc[accepted] = "mg/dl"
        return x, u

    if analyte in ("Sodium", "Potassium"):
        # Desired: mmol/L (treat mEq/L 1:1)
        mask_mmol = u.eq("mmol/l")
        mask_meq  = u.eq("meq/l")
        mask_mgdl = u.eq("mg/dl")

        if analyte == "Sodium":
            x.loc[mask_mgdl] = x.loc[mask_mgdl] * (10.0 / 22.989)
        else:  # Potassium
            x.loc[mask_mgdl] = x.loc[mask_mgdl] * (10.0 / 39.098)

        accepted = mask_mmol | mask_meq | mask_mgdl
        u.loc[accepted] = "mmol/l"
        return x, u

    if analyte == "Hemoglobin":
        # Desired: g/dL
        mask_gdl  = u.eq("g/dl")
        mask_gl   = u.eq("g/l")
        mask_mmol = u.eq("mmol/l")

        x.loc[mask_gl]   = x.loc[mask_gl]   / 10.0  # g/L -> g/dL
        x.loc[mask_mmol] = x.loc[mask_mmol] * 6.45  # mmol/L -> g/dL

        accepted = mask_gdl | mask_gl | mask_mmol
        u.loc[accepted] = "g/dl"
        return x, u

    return x, u

def extract_long(analyte: str, out_csv_path: str):
    inc = TARGETS[analyte]["patterns"]
    exc = TARGETS[analyte]["exclude"]
    lo, hi = TARGETS[analyte]["range"]
    desired_unit = TARGETS[analyte]["out_unit"]

    d = df.loc[name_mask(df[col_name], inc, exc), [col_pid, col_time, col_val, col_unit]].copy()

    # Unit conversion / normalization
    d[col_val], d[col_unit] = convert_units(analyte, d[col_val], d[col_unit])

    # Keep only rows now in desired unit (lowercase for comparison)
    d = d[d[col_unit].eq(desired_unit)]

    # Clean non-positive where appropriate
    if analyte in ("Creatinine", "Potassium", "Hemoglobin"):
        d.loc[d[col_val] <= 0, col_val] = np.nan

    # Range clean to NA
    d.loc[(d[col_val] < lo) | (d[col_val] > hi), col_val] = np.nan

    # Drop rows missing value or time
    d = d.dropna(subset=[col_val, col_time])

    # Sort (patient, time); DO NOT collapse — keep all rows
    d = d.sort_values([col_pid, col_time])

    # Rename to analyte-specific columns and set canonical unit label
    out_val  = analyte
    out_unit = f"{analyte}_valueuom"
    out_time = f"{analyte}_charttime"
    d = d.rename(columns={col_val: out_val, col_unit: out_unit, col_time: out_time})
    d[out_unit] = desired_unit  # canonical case

    d = d[[col_pid, out_val, out_unit, out_time]]
    d.to_csv(out_csv_path, index=False)

    print(f"{analyte}: saved {out_csv_path}, shape={d.shape}")

# ---- Run for each analyte (outputs 4 files) ----
extract_long("Creatinine", OUT_CRE)
extract_long("Sodium",     OUT_NA)
extract_long("Potassium",  OUT_K)
extract_long("Hemoglobin", OUT_HGB)
