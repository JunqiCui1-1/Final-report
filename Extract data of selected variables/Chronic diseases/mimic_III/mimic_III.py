# -*- coding: utf-8 -*-
"""
Comorbidity flags by (subject_id, hadm_id) with OR matching (either key may match).

This version is robust to:
- MIMIC-IV style diagnoses_icd: [subject_id, hadm_id, icd_code, icd_version, ...]
- MIMIC-III style DIAGNOSES_ICD: [subject_id, hadm_id, icd9_code, ...]  (no icd_version)
- Dictionary D_ICD_DIAGNOSES with:
    * MIMIC-IV: [icd_code, icd_version, long_title]
    * MIMIC-III: [icd9_code, short_title, long_title]  (no icd_version)

Inputs
-------
BASE_PATH = /content/patient_ids_SUBJECT_HADM.csv
DIAG_PATH = /content/DIAGNOSES_ICD.csv
DICT_PATH = /content/D_ICD_DIAGNOSES.csv

Output
-------
<BASE_STEM>_comorbidities_or.csv with columns:
subject_id, hadm_id, diabetes, hypertension, ckd, chronic_lung_dz, chronic_endocrine_dz
(1 = present, 0 = absent)

Notes
-----
- Uses ICD-9/10 code-prefix rules (dot-insensitive) plus dictionary long_title regex fallback.
- Filters diagnoses to the baseline cohort with OR logic: subject_id OR hadm_id match.
- Aggregates per baseline row; subject-based and hadm-based hits are combined via OR (max).
"""

from pathlib import Path
import re
import pandas as pd
from typing import Optional, Tuple

# ===== Paths =====
BASE_PATH = "/content/patient_ids_SUBJECT_HADM.csv"
DIAG_PATH = "/content/DIAGNOSES_ICD.csv"
DICT_PATH = "/content/D_ICD_DIAGNOSES.csv"
OUT_PATH  = str(Path(BASE_PATH).with_name(Path(BASE_PATH).stem + "_comorbidities_or.csv"))

# ===== Load =====
base = pd.read_csv(BASE_PATH, low_memory=False)
diag  = pd.read_csv(DIAG_PATH, low_memory=False)
ddict = pd.read_csv(DICT_PATH, low_memory=False)

# ===== Helpers =====
def find_col(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    low = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in low:
            return low[cand.lower()]
    return None

def canon_keys(df: pd.DataFrame) -> pd.DataFrame:
    mapping = {}
    for Want, Cands in {
        "subject_id": ["subject_id", "SUBJECT_ID"],
        "hadm_id":    ["hadm_id", "HADM_ID"],
    }.items():
        col = find_col(df, Cands)
        if col and col != Want:
            mapping[col] = Want
    return df.rename(columns=mapping)

def to_int64(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").astype("Int64")

def strip_dots(s: str) -> str:
    return s.replace(".", "")

# Canonicalize keys
base = canon_keys(base)
diag = canon_keys(diag)
ddict = ddict.copy()  # will canonicalize below

# Check baseline schema
for col in ["subject_id", "hadm_id"]:
    if col not in base.columns:
        raise ValueError(f"BASE file is missing required column: {col}")

# Normalize dtypes on keys
base["subject_id"] = to_int64(base["subject_id"])
base["hadm_id"]    = to_int64(base["hadm_id"])
if "subject_id" in diag.columns:
    diag["subject_id"] = to_int64(diag["subject_id"])
if "hadm_id" in diag.columns:
    diag["hadm_id"]    = to_int64(diag["hadm_id"])

# Detect diagnosis code/version columns (MIMIC-IV and MIMIC-III compatible)
code_col = find_col(diag, ["icd_code", "ICD_CODE", "icd9_code", "ICD9_CODE", "code", "diagnosis_code"])
if code_col is None:
    # Fallback: any column containing both "icd" and "code"
    for c in diag.columns:
        cl = c.lower()
        if "icd" in cl and "code" in cl:
            code_col = c
            break
if code_col is None:
    raise ValueError("Could not find an ICD code column in DIAGNOSES_ICD.")

ver_col = find_col(diag, ["icd_version", "ICD_VERSION", "version"])

# Prepare clean code and a version column (infer if missing)
diag["_code_raw"]   = diag[code_col].astype(str).str.strip().str.upper()
diag["_code_clean"] = diag["_code_raw"].str.replace(".", "", regex=False)

if ver_col:
    diag["_ver"] = pd.to_numeric(diag[ver_col], errors="coerce").astype("Int64")
else:
    # If DIAG has no version, infer from dictionary (if it has versions) or use heuristics, fallback to 9.
    # Build a map from dictionary first.
    dict_code_col = find_col(ddict, ["icd_code", "ICD_CODE", "icd9_code", "ICD9_CODE", "code"])
    dict_ver_col  = find_col(ddict, ["icd_version", "ICD_VERSION", "version"])
    if dict_code_col:
        ddict["_code_clean"] = ddict[dict_code_col].astype(str).str.strip().str.upper().map(strip_dots)
    if dict_ver_col:
        ddict["_ver"] = pd.to_numeric(ddict[dict_ver_col], errors="coerce").astype("Int64")

    ver_map = {}
    if dict_code_col and dict_ver_col:
        # Prefer dictionary-driven version mapping when unambiguous
        tmp = ddict.dropna(subset=["_code_clean", "_ver"]).drop_duplicates(["_code_clean", "_ver"])
        # In case of duplicates across versions, we won't add to the map
        counts = tmp["_code_clean"].value_counts()
        unique_codes = set(counts[counts == 1].index)
        ver_map = dict(tmp[tmp["_code_clean"].isin(unique_codes)][["_code_clean","_ver"]].values.tolist())

    def infer_version(code_clean: str) -> int:
        if code_clean in ver_map:
            return int(ver_map[code_clean])
        if not code_clean:
            return 9
        first = code_clean[0]
        # If it starts with obvious ICD-10 letters (not E or V which are ambiguous due to ICD-9 E/V codes)
        if first in set("ABCDFGHIJKLMNOPQRSTUVWXYZ") - set("EV"):
            return 10
        # Otherwise assume ICD-9
        return 9

    diag["_ver"] = diag["_code_clean"].map(infer_version).astype("Int64")

# OR-filter diagnoses to the baseline cohort
subj_set = set(base["subject_id"].dropna().unique())
hadm_set = set(base["hadm_id"].dropna().unique())
mask = pd.Series(False, index=diag.index)
if "subject_id" in diag.columns:
    mask |= diag["subject_id"].isin(subj_set)
if "hadm_id" in diag.columns:
    mask |= diag["hadm_id"].isin(hadm_set)
diag = diag.loc[mask].copy()

# If nothing matched, emit an all-zero file aligned to baseline
if diag.empty:
    out = base[["subject_id","hadm_id"]].drop_duplicates().copy()
    for c in ["diabetes","hypertension","ckd","chronic_lung_dz","chronic_endocrine_dz"]:
        out[c] = 0
    out.to_csv(OUT_PATH, index=False)
    print(f"Warning: no diagnosis rows matched baseline by subject_id OR hadm_id. Wrote: {OUT_PATH}")
    raise SystemExit(0)

# ===== Build dictionary map: (dotless code, version) -> long_title (fallback to short_title if needed) =====
dict_code_col = find_col(ddict, ["icd_code", "ICD_CODE", "icd9_code", "ICD9_CODE", "code"])
dict_ver_col  = find_col(ddict, ["icd_version", "ICD_VERSION", "version"])
dict_long_col = find_col(ddict, ["long_title", "LONG_TITLE", "title", "description"])
dict_short_col= find_col(ddict, ["short_title", "SHORT_TITLE"])

if dict_code_col is None:
    raise ValueError("D_ICD_DIAGNOSES must include a code column (e.g., icd_code or icd9_code).")

ddict["_code_clean"] = ddict[dict_code_col].astype(str).str.strip().str.upper().map(strip_dots)
if dict_ver_col is not None:
    ddict["_ver"] = pd.to_numeric(ddict[dict_ver_col], errors="coerce").astype("Int64")
else:
    ddict["_ver"] = 9  # dictionary without version -> assume ICD-9

# Pick a title column
if dict_long_col is None and dict_short_col is not None:
    dict_long_col = dict_short_col  # fallback to short_title
elif dict_long_col is None:
    # No usable title; create an empty string column
    ddict["__tmp_title__"] = ""
    dict_long_col = "__tmp_title__"

ddict = ddict[["_code_clean", "_ver", dict_long_col]]

# Merge titles onto diagnoses
diag = diag.merge(ddict.rename(columns={dict_long_col: "__long_title__"}), on=["_code_clean","_ver"], how="left")
titles = diag["__long_title__"].fillna("").astype(str).str.lower()

# ===== Prefix sets (dotless) =====
ICD10_DIAB = ("E10","E11","E12","E13","E14")
ICD9_DIAB  = ("250",)

ICD10_HTN  = ("I10","I11","I12","I13","I15")
ICD9_HTN   = ("401","402","403","404","405")

ICD10_CKD  = ("N18","Z992")      # Z99.2 -> Z992 when dotless
ICD9_CKD   = ("585","V4511","V56")

ICD10_CLD  = ("J41","J42","J43","J44","J45","J47","J84")
ICD9_CLD   = ("491","492","493","494","496")

ICD10_ENDO = ("E03","E05","E20","E21","E22","E23","E24","E27","E28","E34","E89")  # excludes E10–E14
ICD9_ENDO  = ("240","241","242","243","244","245","246","252","253","255","256")

# ===== Title regex (English only) =====
RE_DIABETES = re.compile(r"\b(diabetes|diabetic|dm|dka|hyperglyc[a|e]mia)\b", re.IGNORECASE)
RE_HTN      = re.compile(r"\b(htn|hypertension|high\s*blood\s*pressure)\b", re.IGNORECASE)
RE_CKD      = re.compile(r"\b(chronic\s+kidney\s+disease|ckd|end[- ]stage\s+renal\s+disease|esrd|chronic\s+renal\s+failure|dependence\s+on\s+renal\s+dialysis|hemodialysis|peritoneal\s+dialysis)\b", re.IGNORECASE)
RE_CLD      = re.compile(r"\b(copd|emphysema|chronic\s*bronchitis|bronchiectasis|interstitial\s+lung\s+disease|pulmonary\s+fibrosis|asthma)\b", re.IGNORECASE)
RE_ENDO     = re.compile(r"\b(hypothyroid|hyperthyroid|thyroiditis|goitre|goiter|cushing|addison|adrenal\s+insufficiency|hyperparathyroid|hypoparathyroid|hypopituitarism|panhypopituitarism|acromegaly|pheochromocytoma|pcos|polycystic\s+ovary)\b", re.IGNORECASE)

def startswith_any(series: pd.Series, prefixes: Tuple[str, ...]) -> pd.Series:
    s = series.fillna("").astype(str)
    m = pd.Series(False, index=s.index)
    for p in prefixes:
        m = m | s.str.startswith(p)
    return m

# Version masks
m_v9  = diag["_ver"] == 9
m_v10 = diag["_ver"] == 10
codes = diag["_code_clean"]

# Code-based hits
hit_diab = (m_v10 & startswith_any(codes, ICD10_DIAB)) | (m_v9 & startswith_any(codes, ICD9_DIAB))
hit_htn  = (m_v10 & startswith_any(codes, ICD10_HTN )) | (m_v9 & startswith_any(codes, ICD9_HTN ))
hit_ckd  = (m_v10 & startswith_any(codes, ICD10_CKD )) | (m_v9 & startswith_any(codes, ICD9_CKD ))
hit_cld  = (m_v10 & startswith_any(codes, ICD10_CLD )) | (m_v9 & startswith_any(codes, ICD9_CLD ))
hit_endo = (m_v10 & startswith_any(codes, ICD10_ENDO)) | (m_v9 & startswith_any(codes, ICD9_ENDO))

# Title-based hits
txt_diab = titles.str.contains(RE_DIABETES)
txt_htn  = titles.str.contains(RE_HTN)
txt_ckd  = titles.str.contains(RE_CKD)
txt_cld  = titles.str.contains(RE_CLD)
txt_endo = titles.str.contains(RE_ENDO)

# Final row-level flags (code OR title)
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

# ===== Aggregate by subject and/or hadm, then combine via OR (max) =====
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

# Start with baseline pairs
out = base[["subject_id","hadm_id"]].drop_duplicates().copy()
for c in agg_cols:
    out[c] = 0

# Merge subject-level flags
if by_subject is not None:
    out = out.merge(by_subject, on="subject_id", how="left", suffixes=("", "_sub"))
    for c in agg_cols:
        out[c] = out[[c, f"{c}_sub"]].max(axis=1, skipna=True)
    out = out.drop(columns=[f"{c}_sub" for c in agg_cols])

# Merge hadm-level flags
if by_hadm is not None:
    out = out.merge(by_hadm, on="hadm_id", how="left", suffixes=("", "_hadm"))
    for c in agg_cols:
        out[c] = out[[c, f"{c}_hadm"]].max(axis=1, skipna=True)
    out = out.drop(columns=[f"{c}_hadm" for c in agg_cols])

# Final types and save
for c in agg_cols:
    out[c] = out[c].fillna(0).astype("int8")

out.to_csv(OUT_PATH, index=False)
print(f"Wrote: {OUT_PATH}")
print(out.head(10))
