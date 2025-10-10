#!/usr/bin/env python3
"""
Extract Albumin from eICU-style lab.csv by labname and clean out-of-range values.

- Input : /content/filtered_by_patientunitstayid_20251009_013746/lab.csv
- Output: /content/filtered_by_patientunitstayid_20251009_013746/albumin_from_lab_clean.csv
- Keep/rename columns:
    patientunitstayid,
    labresult           -> Albumin,
    labmeasurenamesystem-> valueuom,
    labresultoffset     -> Albumin_charttime
- Cleaning rule (assumed g/dL): Albumin < 2.0 or > 5.0  => set to NA

Notes:
- labname filtering uses a conservative pattern:
  include rows whose labname contains the whole word "albumin"
  and exclude common non-serum variants like "microalbumin", ratios, or slash-combined tests.
  Adjust the filters below if your lab naming is different.
"""

from pathlib import Path
import pandas as pd
import re

SRC = Path("/content/filtered_by_patientunitstayid_20251009_013746/lab.csv")
OUT = SRC.with_name("albumin_from_lab_clean.csv")
LOW, HIGH = 2.0, 5.0  # g/dL

usecols = [
    "patientunitstayid", "labname", "labresult",
    "labmeasurenamesystem", "labresultoffset"
]
df = pd.read_csv(SRC, usecols=usecols)

# --- Filter to Albumin by labname ---
# include: labname contains whole word "albumin" (case-insensitive)
# exclude: "microalbumin", "ratio", "/", "globulin", "creatinine" (common non-serum contexts)
inc = df["labname"].astype(str).str.contains(r"\balbumin\b", case=False, na=False)
exc = df["labname"].astype(str).str.contains(r"micro|ratio|/|globulin|creatinine", case=False, na=False)
alb = df.loc[inc & ~exc].copy()

# --- Rename columns as requested ---
alb = alb.rename(columns={
    "labresult": "Albumin",
    "labmeasurenamesystem": "valueuom",
    "labresultoffset": "Albumin_charttime"
})[["patientunitstayid", "Albumin", "valueuom", "Albumin_charttime"]].reset_index(drop=True)

# --- Parse numeric Albumin from text like "<2.5" or " 3.6 " ---
alb["Albumin"] = (
    alb["Albumin"].astype(str)
       .str.strip()
       .str.replace(r"^[<>]=?\s*", "", regex=True)  # drop leading <, >, <=, >=
       .pipe(pd.to_numeric, errors="coerce")
)

# --- Clean out-of-range values: <2 or >5 g/dL -> NA ---
mask_out = (alb["Albumin"] < LOW) | (alb["Albumin"] > HIGH)
alb.loc[mask_out, "Albumin"] = pd.NA

# --- Save ---
alb.to_csv(OUT, index=False)

# Optional: quick console summary
print(f"Saved: {OUT}")
print(f"Rows: {len(alb)} | Unique patients: {alb['patientunitstayid'].nunique()}")
print("valueuom top values:\n", alb["valueuom"].value_counts(dropna=False).head())
