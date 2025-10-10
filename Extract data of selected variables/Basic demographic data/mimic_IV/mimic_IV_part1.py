"""
Purpose
-------
From /content/filtered_by_ids_20251008_090531/patients.csv, extract three columns —
subject_id, gender, anchor_age — and rename gender -> Sex, anchor_age -> Age.
Saves the result as /content/patients_subject_sex_age.csv with column order:
subject_id, Sex, Age.
"""

import pandas as pd

src = "/content/filtered_by_ids_20251008_090531/patients.csv"
dst = "/content/patients_subject_sex_age.csv"

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

# Resolve actual header names in case the CSV uses different casing
colmap = resolve_columns(src, ["subject_id", "gender", "anchor_age"])

# Read only the needed columns
df = pd.read_csv(
    src,
    usecols=[colmap["subject_id"], colmap["gender"], colmap["anchor_age"]],
    dtype={colmap["subject_id"]: "Int64"},
)

# Rename to requested output names
df = df.rename(
    columns={
        colmap["subject_id"]: "subject_id",
        colmap["gender"]: "Sex",
        colmap["anchor_age"]: "Age",
    }
)

# Enforce final column order
df = df[["subject_id", "Sex", "Age"]]

# Save
df.to_csv(dst, index=False)
print(f"✅ Done. Output -> {dst}")
