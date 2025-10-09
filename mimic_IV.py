# -*- coding: utf-8 -*-
"""
Comorbidity flags by (subject_id, hadm_id) with OR matching (either key may match).

This version is tailored to MIMIC-style inputs:
- diagnoses_icd.csv: columns like [subject_id, hadm_id, icd_code, icd_version, ...]
- d_icd_diagnoses.csv: dictionary with [icd_code, icd_version, long_title]

It uses ONLY English regex/text and robust, dotless ICD prefix matching.
If either subject_id OR hadm_id matches the baseline, the admission is eligible.

Outputs 7 columns:
  subject_id, hadm_id, diabetes, hypertension, ckd, chronic_lung_dz, chronic_endocrine_dz
"""

from pathlib import Path
import re
import pandas as pd

# ===== Paths =====
BASE_PATH = "/content/CAD_CABG_loose_intersection.csv"
DIAG_PATH = "/content/filtered_by_patientunitstayid_20251009_013746_unzipped/diagnoses_icd.csv"
DICT_PATH = "/content/filtered_by_patientunitstayid_20251009_013746_unzipped/d_icd_diagnoses.csv"
OUT_PATH  = str(Path(BASE_PATH).with_name(Path(BASE_PATH).stem + "_comorbidities_hadm_or_subject.csv"))

# ===== Load =====
base = pd.read_csv(BASE_PATH, low_memory=False)
diag  = pd.read_csv(DIAG_PATH, low_memory=False)
ddict = pd.read_csv(DICT_PATH, low_memory=False)

# Normalize key names in DIAG if uppercase
rename_map = {}
if "SUBJECT_ID" in diag.columns and "subject_id" not in diag.columns:
    rename_map["SUBJECT_ID"] = "subject_id"
if "HADM_ID" in diag.columns and "hadm_id" not in diag.columns:
    rename_map["HADM_ID"] = "hadm_id"
diag = diag.rename(columns=rename_map)

# Check baseline schema
for col in ["subject_id", "hadm_id"]:
    if col not in base.columns:
        raise ValueError(f"BASE file missing required column: {col}")

# Types
to_int = lambda x: pd.to_numeric(x, errors="coerce").astype("Int64")
base["subject_id"] = to_int(base["subject_id"])
base["hadm_id"]    = to_int(base["hadm_id"])

if "subject_id" in diag.columns:
    diag["subject_id"] = to_int(diag["subject_id"])
if "hadm_id" in diag.columns:
    diag["hadm_id"]    = to_int(diag["hadm_id"])

# Identify code/version columns
code_col = None
for c in ["icd_code", "ICD_CODE", "code", "diagnosis_code"]:
    if c in diag.columns:
        code_col = c
        break
if code_col is None:
    # fallback: pick first column containing 'icd' and 'code'
    for c in diag.columns:
        cl = c.lower()
        if ("icd" in cl) and ("code" in cl):
            code_col = c
            break
if code_col is None:
    raise ValueError("Could not find an ICD code column in diagnoses_icd.")

version_col = None
for c in ["icd_version", "ICD_VERSION", "version"]:
    if c in diag.columns:
        version_col = c
        break
if version_col is None:
    raise ValueError("Could not find icd_version column; this script expects 9/10 codes disambiguation.")

# Prepare dotless, uppercase code
diag["_code_raw"]   = diag[code_col].astype(str).str.strip().str.upper()
diag["_code_clean"] = diag["_code_raw"].str.replace(".", "", regex=False)

# Filter DIAG to baseline using OR logic (either subject_id or hadm_id)
subj_set = set(base["subject_id"].dropna().unique())
hadm_set = set(base["hadm_id"].dropna().unique())

mask = pd.Series(False, index=diag.index)
if "subject_id" in diag.columns:
    mask = mask | diag["subject_id"].isin(subj_set)
if "hadm_id" in diag.columns:
    mask = mask | diag["hadm_id"].isin(hadm_set)
diag = diag.loc[mask].copy()

# Early exit guard to avoid "all zeros" due to accidental empty filter
if diag.empty:
    # Still produce an all-zero output aligned to baseline
    out = base[["subject_id","hadm_id"]].drop_duplicates().copy()
    for c in ["diabetes", "hypertension", "ckd", "chronic_lung_dz", "chronic_endocrine_dz"]:
        out[c] = 0
    out.to_csv(OUT_PATH, index=False)
    print("Warning: No diagnosis rows matched baseline by subject_id OR hadm_id. Wrote an all-zero file.")
    print(f"Wrote: {OUT_PATH}")
    raise SystemExit(0)

# Prepare dictionary: dotless code + version -> long_title
dict_code_col = None
for c in ["icd_code", "ICD_CODE", "code"]:
    if c in ddict.columns:
        dict_code_col = c
        break
dict_ver_col = None
for c in ["icd_version", "ICD_VERSION", "version"]:
    if c in ddict.columns:
        dict_ver_col = c
        break
dict_title_col = None
for c in ["long_title", "LONG_TITLE", "title", "description"]:
    if c in ddict.columns:
        dict_title_col = c
        break

if dict_code_col and dict_ver_col and dict_title_col:
    ddict["_code_clean"] = ddict[dict_code_col].astype(str).str.strip().str.upper().str.replace(".", "", regex=False)
    ddict["_ver"] = pd.to_numeric(ddict[dict_ver_col], errors="coerce").astype("Int64")
    ddict = ddict[["_code_clean", "_ver", dict_title_col]].dropna(subset=["_code_clean", "_ver"])
else:
    ddict = pd.DataFrame(columns=["_code_clean", "_ver", "long_title"])

# Merge long_title onto diagnoses
diag["_ver"] = pd.to_numeric(diag[version_col], errors="coerce").astype("Int64")
diag = diag.merge(ddict.rename(columns={dict_title_col: "__long_title__"}), on=["_code_clean", "_ver"], how="left")

# ===== Prefix sets (dotless) =====
ICD10_DIAB = ("E10", "E11", "E12", "E13", "E14")
ICD9_DIAB  = ("250",)

ICD10_HTN  = ("I10", "I11", "I12", "I13", "I15")
ICD9_HTN   = ("401", "402", "403", "404", "405")

ICD10_CKD  = ("N18", "Z992")       # Z99.2 -> Z992
ICD9_CKD   = ("585", "V4511", "V56")

ICD10_CLD  = ("J41", "J42", "J43", "J44", "J45", "J47", "J84")
ICD9_CLD   = ("491", "492", "493", "494", "496")

ICD10_ENDO = ("E03", "E05", "E20", "E21", "E22", "E23", "E24", "E27", "E28", "E34", "E89")
ICD9_ENDO  = ("240", "241", "242", "243", "244", "245", "246", "252", "253", "255", "256")

# ===== Text regex (English only) =====
RE_DIABETES = re.compile(r"\b(diabetes|diabetic|dm|dka|hyperglyc[a|e]mia)\b", re.IGNORECASE)
RE_HTN      = re.compile(r"\b(htn|hypertension|high\s*blood\s*pressure)\b", re.IGNORECASE)
RE_CKD      = re.compile(r"\b(chronic\s+kidney\s+disease|ckd|end[- ]stage\s+renal\s+disease|esrd|chronic\s+renal\s+failure|dialysis|hemodialysis|peritoneal\s+dialysis)\b", re.IGNORECASE)
RE_CLD      = re.compile(r"\b(copd|emphysema|chronic\s*bronchitis|bronchiectasis|interstitial\s+lung\s+disease|pulmonary\s+fibrosis|asthma)\b", re.IGNORECASE)
RE_ENDO     = re.compile(r"\b(hypothyroid|hyperthyroid|thyroiditis|goitre|goiter|cushing|addison|adrenal\s+insufficiency|hyperparathyroid|hypoparathyroid|pituitary|acromegaly|pheochromocytoma|pcos|polycystic\s+ovary)\b", re.IGNORECASE)

def startswith_any(series: pd.Series, prefixes: tuple[str, ...]) -> pd.Series:
    """Vectorized prefix check on an uppercase, dotless code series."""
    s = series.fillna("").astype(str)
    mask = pd.Series(False, index=s.index)
    for p in prefixes:
        mask = mask | s.str.startswith(p)
    return mask

# Version masks
m_v9  = diag["_ver"] == 9
m_v10 = diag["_ver"] == 10

codes = diag["_code_clean"]

# Code-based hits (by version)
hit_diab = (m_v10 & startswith_any(codes, ICD10_DIAB)) | (m_v9 & startswith_any(codes, ICD9_DIAB))
hit_htn  = (m_v10 & startswith_any(codes, ICD10_HTN )) | (m_v9 & startswith_any(codes, ICD9_HTN ))
hit_ckd  = (m_v10 & startswith_any(codes, ICD10_CKD )) | (m_v9 & startswith_any(codes, ICD9_CKD ))
hit_cld  = (m_v10 & startswith_any(codes, ICD10_CLD )) | (m_v9 & startswith_any(codes, ICD9_CLD ))
hit_endo = (m_v10 & startswith_any(codes, ICD10_ENDO)) | (m_v9 & startswith_any(codes, ICD9_ENDO))

# Text-based fallback using dictionary long titles (if available)
titles = diag["__long_title__"].fillna("").astype(str).str.lower()

txt_diab = titles.str.contains(RE_DIABETES)
txt_htn  = titles.str.contains(RE_HTN)
txt_ckd  = titles.str.contains(RE_CKD)
txt_cld  = titles.str.contains(RE_CLD)
txt_endo = titles.str.contains(RE_ENDO)

# Final per-row flags (code OR text)
row_diab = (hit_diab | txt_diab).astype(int)
row_htn  = (hit_htn  | txt_htn ).astype(int)
row_ckd  = (hit_ckd  | txt_ckd ).astype(int)
row_cld  = (hit_cld  | txt_cld ).astype(int)
row_endo = (hit_endo | txt_endo).astype(int)

diag_flags = pd.DataFrame({
    "subject_id": diag["subject_id"] if "subject_id" in diag.columns else pd.Series([pd.NA]*len(diag), dtype="Int64"),
    "hadm_id":    diag["hadm_id"]    if "hadm_id"    in diag.columns else pd.Series([pd.NA]*len(diag), dtype="Int64"),
    "diabetes":   row_diab,
    "hypertension": row_htn,
    "ckd":        row_ckd,
    "chronic_lung_dz": row_cld,
    "chronic_endocrine_dz": row_endo,
})

# Aggregate separately by subject and by hadm (OR logic when combining)
agg_cols = ["diabetes","hypertension","ckd","chronic_lung_dz","chronic_endocrine_dz"]

by_subject = None
if "subject_id" in diag.columns:
    sub = diag_flags.dropna(subset=["subject_id"])
    if not sub.empty:
        by_subject = sub.groupby("subject_id", as_index=False)[agg_cols].max()

by_hadm = None
if "hadm_id" in diag.columns:
    had = diag_flags.dropna(subset=["hadm_id"])
    if not had.empty:
        by_hadm = had.groupby("hadm_id", as_index=False)[agg_cols].max()

# Merge onto baseline and combine via max
out = base[["subject_id","hadm_id"]].drop_duplicates().copy()
for c in agg_cols:
    out[c] = 0

if by_subject is not None:
    out = out.merge(by_subject, on="subject_id", how="left", suffixes=("", "_sub"))
    for c in agg_cols:
        out[c] = out[[c, f"{c}_sub"]].max(axis=1, skipna=True)
    out = out.drop(columns=[f"{c}_sub" for c in agg_cols])

if by_hadm is not None:
    out = out.merge(by_hadm, on="hadm_id", how="left", suffixes=("", "_hadm"))
    for c in agg_cols:
        out[c] = out[[c, f"{c}_hadm"]].max(axis=1, skipna=True)
    out = out.drop(columns=[f"{c}_hadm" for c in agg_cols])

# Final types
for c in agg_cols:
    out[c] = out[c].fillna(0).astype("int8")

# Save
out.to_csv(OUT_PATH, index=False)

# Minimal diagnostics to help verify matches
print(f"Wrote: {OUT_PATH}")
print("Preview:")
print(out.head(10))
print("\nRow-level hits (any):",
      f"diabetes={row_diab.sum()}",
      f"hypertension={row_htn.sum()}",
      f"ckd={row_ckd.sum()}",
      f"chronic_lung_dz={row_cld.sum()}",
      f"chronic_endocrine_dz={row_endo.sum()}",
      sep="\n")
