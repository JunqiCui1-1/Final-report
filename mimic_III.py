#!/usr/bin/env python3
"""
Extract serum albumin (ITEMID=50862) from an UPPERCASE LABEVENTS file and clean values.

- Input:  /content/filtered_by_ids/LABEVENTS.csv   (columns are UPPERCASE)
- Output: /content/filtered_by_ids/albumin_50862_clean.csv
- Keeps 4 columns (renamed): subject_id, Albumin, valueuom, Albumin_charttime
- Cleaning rule (g/dL): Albumin < 2.0 or > 5.0  => set to NA
"""

from pathlib import Path
import pandas as pd

SRC = Path("/content/filtered_by_ids/LABEVENTS.csv")
OUT = SRC.with_name("albumin_50862_clean.csv")
ITEMID = 50862
LOW, HIGH = 2.0, 5.0  # g/dL

# Read only needed columns (UPPERCASE in this file)
usecols = ["SUBJECT_ID", "ITEMID", "VALUE", "VALUEUOM", "CHARTTIME"]
df = pd.read_csv(SRC, usecols=usecols)

# Filter target item and select/rename columns
alb = (
    df.loc[df["ITEMID"] == ITEMID, ["SUBJECT_ID", "VALUE", "VALUEUOM", "CHARTTIME"]]
      .rename(columns={
          "SUBJECT_ID": "subject_id",
          "VALUE": "Albumin",
          "VALUEUOM": "valueuom",
          "CHARTTIME": "Albumin_charttime"
      })
      .reset_index(drop=True)
)

# Parse numeric from 'Albumin' (strip leading comparison signs like <, >, <=, >=)
alb["Albumin"] = (
    alb["Albumin"].astype(str)
      .str.strip()
      .str.replace(r"^[<>]=?\s*", "", regex=True)
      .pipe(pd.to_numeric, errors="coerce")
)

# Apply cleaning rule: values <2 or >5 g/dL -> NA
mask_out_of_range = (alb["Albumin"] < LOW) | (alb["Albumin"] > HIGH)
alb.loc[mask_out_of_range, "Albumin"] = pd.NA

# Save only the requested 4 columns
alb[["subject_id", "Albumin", "valueuom", "Albumin_charttime"]].to_csv(OUT, index=False)

print(f"Saved: {OUT}  rows={len(alb)}")
print(alb["valueuom"].value_counts(dropna=False).head())
