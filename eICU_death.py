"""
Purpose
-------
From /content/filtered_by_patientunitstayid_20251009_013746/patient.csv,
extract two columns: patientunitstayid and Death.

Definition
----------
Death = 1 if ICU discharge status indicates death (unitdischargestatus == 'Expired',
case-insensitive, trimmed); otherwise 0.

Output
------
/content/patientunitstayid_Death.csv
    columns: patientunitstayid, Death
"""

import pandas as pd

src = "/content/filtered_by_patientunitstayid_20251009_013746/patient.csv"
dst = "/content/patientunitstayid_Death.csv"

def resolve_columns(csv_path: str, desired_lower_names):
    """Case-insensitive resolver mapping desired lowercase names to actual CSV headers."""
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

# Resolve actual column names (robust to different casing)
colmap = resolve_columns(src, ["patientunitstayid", "unitdischargestatus"])

# Read only the needed columns
df = pd.read_csv(
    src,
    usecols=[colmap["patientunitstayid"], colmap["unitdischargestatus"]],
    dtype={colmap["patientunitstayid"]: "Int64"},
    keep_default_na=True,
)

# Standardize names
df = df.rename(columns={colmap["patientunitstayid"]: "patientunitstayid"})

# Create Death flag: 1 if unitdischargestatus == 'Expired' (case-insensitive), else 0
status = df[colmap["unitdischargestatus"]].astype(str).str.strip().str.lower()
df["Death"] = (status == "expired").astype("int8")

# Keep required columns and save
out = df[["patientunitstayid", "Death"]]
out.to_csv(dst, index=False)
print(f"✅ Done. Output -> {dst}")
