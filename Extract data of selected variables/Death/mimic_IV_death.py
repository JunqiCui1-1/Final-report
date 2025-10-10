"""
Purpose
-------
From /content/filtered_by_ids_20251008_090531/admissions.csv, extract three columns —
subject_id, hadm_id, and deathtime — then rename deathtime to Death and convert it
to a binary indicator (1 if non-missing, else 0). Save the result as
/content/admissions_subject_hadm_death.csv with columns: subject_id, hadm_id, Death.
"""

import pandas as pd

src = "/content/filtered_by_ids_20251008_090531/admissions.csv"
dst = "/content/admissions_subject_hadm_death.csv"

def resolve_columns(csv_path: str, desired_lower_names):
    """Case-insensitive resolver that maps desired lowercase names to actual CSV headers."""
    cols = pd.read_csv(csv_path, nrows=0).columns
    lower_map = {c.lower(): c for c in cols}
    mapping, missing = {}, []
    for name in desired_lower_names:
        if name in lower_map:
            mapping[name] = lower_map[name]
        else:
            missing.append(name)
    if missing:
        raise ValueError(f"Missing required columns in {csv_path}: {', '.join(missing)}")
    return mapping

# Resolve header names (robust to different casing)
colmap = resolve_columns(src, ["subject_id", "hadm_id", "deathtime"])

# Read only needed columns
df = pd.read_csv(
    src,
    usecols=[colmap["subject_id"], colmap["hadm_id"], colmap["deathtime"]],
    dtype={colmap["subject_id"]: "Int64", colmap["hadm_id"]: "Int64"},
    na_values=["", "NA", "NaN", "NULL", "null", "None", "none", "N/A"],
    keep_default_na=True,
)

# Standardize column names (lowercase) for output
df = df.rename(
    columns={
        colmap["subject_id"]: "subject_id",
        colmap["hadm_id"]: "hadm_id",
    }
)

# Create binary Death indicator: 1 if deathtime present, else 0
df["Death"] = df[colmap["deathtime"]].notna().astype("int8")

# Keep final columns and order
df = df[["subject_id", "hadm_id", "Death"]]

# Save
df.to_csv(dst, index=False)
print(f"✅ Done. Output -> {dst}")
