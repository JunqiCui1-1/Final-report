#!/usr/bin/env python3
"""
Extract serum albumin (itemid=50862) and clean out-of-range values.

- Input:  /content/filtered_by_ids_20251008_090531/labevents.csv
- Output: /content/filtered_by_ids_20251008_090531/albumin_50862_clean.csv
- Keeps 4 columns: subject_id, Albumin, valueuom, Albumin_charttime
- Cleans rule (g/dL): Albumin < 2.0 or > 5.0  => set to NA
"""

from pathlib import Path
import pandas as pd
import re

SRC = Path("/content/filtered_by_ids_20251008_090531/labevents.csv")
OUT = SRC.with_name("albumin_50862_clean.csv")
ITEMID = 50862
LOW, HIGH = 2.0, 5.0  # g/dL

# Read only what we need for speed/memory
usecols = ["subject_id", "itemid", "value", "valueuom", "charttime"]
df = pd.read_csv(SRC, usecols=usecols)

# Filter target item and select/rename columns
alb = (
    df.loc[df["itemid"] == ITEMID, ["subject_id", "value", "valueuom", "charttime"]]
      .rename(columns={"value": "Albumin", "charttime": "Albumin_charttime"})
      .reset_index(drop=True)
)

# Parse numeric from 'Albumin' (strip leading comparison signs like <, >, <=, >=)
# Anything non-numeric after cleanup becomes NA
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

print(f"Saved: {OUT}")
print(alb.shape)
print(alb["valueuom"].value_counts(dropna=False).head())
