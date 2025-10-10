"""
Purpose
-------
From /content/filtered_by_ids_20251008_090531/chartevents.csv (target), extract four
columns — subject_id, hadm_id, Weight (renamed from value), and valueuom — keeping
only rows where subject_id or hadm_id exists in the base file /content/CAD_CABG_loose_intersection.csv,
and only when ITEMID == 226512. Values <35 or >135 are set to NaN. The script streams
the target file in chunks for memory efficiency.

Output: /content/WEIGHT_filtered.csv
"""

import os
import numpy as np
import pandas as pd

base_path = "/content/CAD_CABG_loose_intersection.csv"
target_path = "/content/filtered_by_ids_20251008_090531/chartevents.csv"
output_path = "/content/WEIGHT_filtered.csv"
ITEM_ID = 226512

def resolve_columns(csv_path: str, desired_lower_names):
    """Case-insensitive resolver: maps desired lowercase names to actual column names in the CSV."""
    header_cols = pd.read_csv(csv_path, nrows=0).columns
    lower_map = {c.lower(): c for c in header_cols}
    mapping, missing = {}, []
    for name in desired_lower_names:
        if name in lower_map:
            mapping[name] = lower_map[name]
        else:
            missing.append(name)
    if missing:
        raise ValueError(
            f"Missing required columns in {csv_path}: {', '.join(missing)} (case-insensitive match attempted)"
        )
    return mapping

# Resolve columns for base and target
base_map = resolve_columns(base_path, ["subject_id", "hadm_id"])
target_map = resolve_columns(target_path, ["subject_id", "hadm_id", "itemid", "value", "valueuom"])

# Read base file (only necessary columns) and collect allowed IDs
base = pd.read_csv(
    base_path,
    usecols=[base_map["subject_id"], base_map["hadm_id"]],
    dtype={base_map["subject_id"]: "Int64", base_map["hadm_id"]: "Int64"},
).rename(columns={base_map["subject_id"]: "subject_id", base_map["hadm_id"]: "hadm_id"})

subjects = set(base["subject_id"].dropna().astype(int))
hadms = set(base["hadm_id"].dropna().astype(int))

# Prepare chunked reading of the target file
usecols = [
    target_map["subject_id"],
    target_map["hadm_id"],
    target_map["itemid"],
    target_map["value"],
    target_map["valueuom"],
]
dtypes = {
    target_map["subject_id"]: "Int64",
    target_map["hadm_id"]: "Int64",
    target_map["itemid"]: "Int64",
    target_map["value"]: "string",
    target_map["valueuom"]: "string",
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
    # Normalize column names
    chunk = chunk.rename(
        columns={
            target_map["subject_id"]: "subject_id",
            target_map["hadm_id"]: "hadm_id",
            target_map["itemid"]: "itemid",
            target_map["value"]: "value",
            target_map["valueuom"]: "valueuom",
        }
    )

    # ITEMID filter
    chunk = chunk[chunk["itemid"] == ITEM_ID]
    if chunk.empty:
        continue

    # Keep rows that match either subject_id or hadm_id in the base lists
    mask_subject = chunk["subject_id"].isin(subjects)
    mask_hadm = chunk["hadm_id"].isin(hadms)
    chunk = chunk[mask_subject | mask_hadm]
    if chunk.empty:
        continue

    # Select required columns and clean
    out = chunk[["subject_id", "hadm_id", "value", "valueuom"]].copy()
    out["value"] = pd.to_numeric(out["value"], errors="coerce")
    out = out.rename(columns={"value": "Weight"})
    out.loc[(out["Weight"] < 35) | (out["Weight"] > 135), "Weight"] = np.nan
    out = out[["subject_id", "hadm_id", "Weight", "valueuom"]]

    # Append to output
    out.to_csv(output_path, index=False, mode="a", header=first_write)
    first_write = False

if os.path.exists(output_path):
    print(f"✅ Done. Output -> {output_path}")
else:
    print("No matching rows found (check base filtering, ITEMID, and column names).")
