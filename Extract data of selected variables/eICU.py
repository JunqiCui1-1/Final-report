"""
Purpose
-------
From /content/filtered_by_patientunitstayid_20251009_013746/patient.csv, extract four
columns — patientunitstayid, gender -> Sex, age -> Age, admissionweight -> Weight.
Set Weight values <35 or >135 to NaN (missing). Save as:
    /content/patientunitstayid_Sex_Age_Weight.csv
"""

import pandas as pd
import numpy as np

src = "/content/filtered_by_patientunitstayid_20251009_013746/patient.csv"
dst = "/content/patientunitstayid_Sex_Age_Weight.csv"

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

# Resolve actual header names (robust to casing differences)
colmap = resolve_columns(src, ["patientunitstayid", "gender", "age", "admissionweight"])

# Read only needed columns
df = pd.read_csv(
    src,
    usecols=[colmap["patientunitstayid"], colmap["gender"], colmap["age"], colmap["admissionweight"]],
    dtype={colmap["patientunitstayid"]: "Int64"},
    keep_default_na=True,
)

# Rename to requested output names
df = df.rename(columns={
    colmap["patientunitstayid"]: "patientunitstayid",
    colmap["gender"]: "Sex",
    colmap["age"]: "Age",
    colmap["admissionweight"]: "Weight",
})

# Ensure numeric types where appropriate
df["Age"] = pd.to_numeric(df["Age"], errors="coerce")
df["Weight"] = pd.to_numeric(df["Weight"], errors="coerce")

# Clean Weight: set out-of-range values to NaN
df.loc[(df["Weight"] < 35) | (df["Weight"] > 135), "Weight"] = np.nan

# Order columns and save
df = df[["patientunitstayid", "Sex", "Age", "Weight"]]
df.to_csv(dst, index=False)
print(f"✅ Done. Output -> {dst}")
