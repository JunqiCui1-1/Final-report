"""
Purpose
-------
Extract ITEMID 226512 from /content/filtered_by_ids/CHARTEVENTS.csv, keeping only
patients/admissions listed in /content/patient_ids_SUBJECT_HADM.csv. The output
contains exactly four columns: SUBJECT_ID, HADM_ID, Weight (renamed from VALUE),
and VALUEUOM. Values outside the physiologic range (<35 or >135) are set to NaN.
The script streams the input in chunks for memory safety.

Output: /content/ITEMID226512_filtered.csv
"""

import os
import numpy as np
import pandas as pd

# File paths
base_path = "/content/patient_ids_SUBJECT_HADM.csv"
target_path = "/content/filtered_by_ids/CHARTEVENTS.csv"
output_path = "/content/ITEMID226512_filtered.csv"

ITEM_ID = 226512  # target ITEMID

# 1) Read base file and collect allowed SUBJECT_ID / HADM_ID
base = (
    pd.read_csv(base_path, dtype={"SUBJECT_ID": "Int64", "HADM_ID": "Int64"})[
        ["SUBJECT_ID", "HADM_ID"]
    ]
    .drop_duplicates()
)
subjects = set(base["SUBJECT_ID"].dropna().astype(int))
hadms = set(base["HADM_ID"].dropna().astype(int))

# 2) Stream target file and filter by ITEMID and base keys
usecols = ["SUBJECT_ID", "HADM_ID", "ITEMID", "VALUE", "VALUEUOM"]
dtypes = {
    "SUBJECT_ID": "Int64",
    "HADM_ID": "Int64",
    "ITEMID": "Int64",
    "VALUE": "string",
    "VALUEUOM": "string",
}
chunksize = 1_000_000
first_write = True

# Remove previous output if present
if os.path.exists(output_path):
    os.remove(output_path)

for chunk in pd.read_csv(
    target_path,
    usecols=usecols,
    dtype=dtypes,
    chunksize=chunksize,
    low_memory=False,
):
    # Keep only ITEMID == 226512
    chunk = chunk[chunk["ITEMID"] == ITEM_ID]
    if chunk.empty:
        continue

    # Keep rows that match either SUBJECT_ID or HADM_ID in the base lists
    mask_subject = chunk["SUBJECT_ID"].isin(subjects)
    mask_hadm = chunk["HADM_ID"].isin(hadms)
    chunk = chunk[mask_subject | mask_hadm]
    if chunk.empty:
        continue

    # Select and prepare the four required columns
    filtered = chunk[["SUBJECT_ID", "HADM_ID", "VALUE", "VALUEUOM"]].copy()

    # Convert VALUE to numeric then rename to Weight
    filtered["VALUE"] = pd.to_numeric(filtered["VALUE"], errors="coerce")
    filtered = filtered.rename(columns={"VALUE": "Weight"})

    # Cleaning: set out-of-range values to NaN
    filtered.loc[(filtered["Weight"] < 35) | (filtered["Weight"] > 135), "Weight"] = np.nan

    # Ensure column order
    filtered = filtered[["SUBJECT_ID", "HADM_ID", "Weight", "VALUEUOM"]]

    # Append to output
    filtered.to_csv(output_path, index=False, mode="a", header=first_write)
    first_write = False

if os.path.exists(output_path):
    print(f"✅ Done. Output -> {output_path}")
else:
    print("No matching rows found (check ITEMID or base filter).")
