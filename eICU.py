# -*- coding: utf-8 -*-
"""
Build comorbidity flags by patientunitstayid using diagnosis text and ICD-9/10 prefixes.

Input
------
- BASE_PATH: CSV with the baseline cohort (patientunitstayid)
- DIAG_PATH: CSV with diagnosis records filtered to the same cohort

Output
------
- OUT_PATH: CSV with 6 columns:
  patientunitstayid, diabetes, hypertension, ckd, chronic_lung_dz, chronic_endocrine_dz
  (1 = present, 0 = absent)

Notes
------
- Text matching is keyword/regex-based (case-insensitive) over concatenated diagnosis text columns.
- Code matching is prefix-based over any columns that look like ICD/code columns.
- CKD logic excludes "pure AKI" unless there is an "acute on chronic" indication.
- Chronic endocrine disease excludes diabetes.
"""

import re
import pandas as pd

# ========= Paths =========
BASE_PATH = "/content/patientunitstayid_intersection.csv"
DIAG_PATH = "/content/diagnosis.csv"
OUT_PATH  = "/content/patientunitstayid_comorbidities.csv"

# ========= Load data =========
base = pd.read_csv(BASE_PATH, dtype={"patientunitstayid": "Int64"}, low_memory=False)
diag = pd.read_csv(DIAG_PATH, low_memory=False)

# Normalize patientunitstayid type
def to_int64_safe(s: pd.Series) -> pd.Series:
    try:
        return pd.to_numeric(s, errors="coerce").astype("Int64")
    except Exception:
        return pd.Series(pd.NA, index=s.index, dtype="Int64")

if "patientunitstayid" not in diag.columns:
    raise ValueError(
        "Missing 'patientunitstayid' column in the diagnosis file. "
        "Please check /content/filtered_by_patientunitstayid/diagnosis.csv"
    )

diag["patientunitstayid"] = to_int64_safe(diag["patientunitstayid"])
base["patientunitstayid"] = to_int64_safe(base["patientunitstayid"])

# Keep only diagnosis rows within the baseline cohort
diag = diag[diag["patientunitstayid"].isin(base["patientunitstayid"].dropna().unique())].copy()

# ========= Identify text/code columns & build a unified text field =========
TEXT_HINTS = ["diag", "desc", "string", "text", "name", "problem", "impression",
              "assessment", "note", "complaint", "history"]
text_cols = [c for c in diag.columns if any(h in c.lower() for h in TEXT_HINTS)]

CODE_HINTS = ["icd", "code"]
code_cols = [c for c in diag.columns if any(h in c.lower() for h in CODE_HINTS)]

def combine_text(row: pd.Series, cols: list[str]) -> str:
    parts = []
    for c in cols:
        v = row.get(c, None)
        if pd.isna(v):
            continue
        parts.append(str(v))
    return " | ".join(parts) if parts else ""

if len(text_cols) == 0 and len(code_cols) == 0:
    raise ValueError("No usable diagnosis text or ICD/code columns were detected. Please check the file schema.")

for c in code_cols:
    diag[c] = diag[c].astype(str).str.upper()

diag["_all_text_"] = diag.apply(lambda r: combine_text(r, text_cols).lower(), axis=1)

# ========= Regex vocab & ICD prefixes =========
# Diabetes: ICD-10 E10–E14; ICD-9 250; keywords DM/diabetes/diabetic/DKA etc.
RE_DIABETES = re.compile(r"\b(diabetes|diabetic|dm|dka|hyperglyc[a|e]mia)", re.IGNORECASE)
ICD10_DIAB = ("E10", "E11", "E12", "E13", "E14")
ICD9_DIAB  = ("250",)

# Hypertension: ICD-10 I10–I15; ICD-9 401–405; keywords HTN/hypertension
RE_HTN = re.compile(r"\b(htn|hypertension|high\s*blood\s*pressure)", re.IGNORECASE)
ICD10_HTN = ("I10", "I11", "I12", "I13", "I15")
ICD9_HTN  = ("401", "402", "403", "404", "405")

# CKD: ICD-10 N18, Z99.2; ICD-9 585, V45.11/V56.*; keywords CKD/ESRD;
# avoid pure AKI unless "acute on chronic".
RE_CKD_CORE = re.compile(
    r"(chronic\s+kidney\s+disease|ckd|end[- ]stage\s+renal\s+disease|esrd|chronic\s+renal\s+failure)",
    re.IGNORECASE
)
RE_AKI_ONLY = re.compile(r"\b(aki|acute\s+(kidney|renal)\s+(injury|failure)))", re.IGNORECASE)
RE_CHRONIC_WORD = re.compile(r"\bchronic\b|慢性", re.IGNORECASE)

ICD10_CKD = ("N18", "Z99.2")
ICD9_CKD  = ("585", "V45.11", "V56")

# Chronic lung disease: COPD/emphysema/chronic bronchitis/bronchiectasis/ILD/fibrosis/asthma
RE_CLD = re.compile(
    r"(copd|emphysema|chronic\s*bronchitis|bronchiectasis|interstitial\s+lung\s+disease|pulmonary\s+fibrosis|asthma|"
    re.IGNORECASE
)
ICD10_CLD = ("J44", "J43", "J41", "J42", "J47", "J84", "J45")
ICD9_CLD  = ("491", "492", "493", "494", "496")

# Chronic endocrine disease (excluding diabetes): thyroid, adrenal, pituitary, parathyroid, PCOS...
RE_ENDO = re.compile(
    r"(hypothyroid|hyperthyroid|thyroiditis|goitre|goiter|cushing|addison|adrenal\s+insufficiency|hyperparathyroid|"
    r"hypoparathyroid|pituitary|acromegaly|pheochromocytoma|pcos|polycystic\s+ovary|"
    re.IGNORECASE
)
ICD10_ENDO = ("E03", "E05", "E20", "E21", "E22", "E23", "E24", "E27", "E28", "E34", "E89")  # excludes E10–E14 (diabetes)
ICD9_ENDO  = ("240", "241", "242", "243", "244", "245", "246",  # thyroid
              "252", "253", "255", "256")                      # parathyroid/pituitary/adrenal/ovary

# ========= Matching helpers =========
def icd_prefix_match_any(df: pd.DataFrame, code_columns: list[str], prefixes: tuple[str, ...]) -> pd.Series:
    """Return a boolean Series where any ICD/code column starts with any of the given prefixes (case-insensitive)."""
    if not code_columns or not prefixes:
        return pd.Series(False, index=df.index)
    safe = [re.escape(p.upper()) for p in prefixes]
    pat = re.compile(r"^(" + "|".join(safe) + r")\b", re.IGNORECASE)
    hit = pd.Series(False, index=df.index)
    for c in code_columns:
        s = df[c].astype(str).str.upper()
        hit = hit | s.str.match(pat)
    return hit

def text_match(df: pd.DataFrame, regex: re.Pattern) -> pd.Series:
    if "_all_text_" not in df.columns:
        return pd.Series(False, index=df.index)
    return df["_all_text_"].str.contains(regex)

# ========= Row-level matches =========
# Diabetes
m_diab = text_match(diag, RE_DIABETES) | icd_prefix_match_any(diag, code_cols, ICD10_DIAB + ICD9_DIAB)

# Hypertension
m_htn  = text_match(diag, RE_HTN) | icd_prefix_match_any(diag, code_cols, ICD10_HTN + ICD9_HTN)

# CKD (avoid pure AKI; count acute-on-chronic as CKD)
text_has_ckd  = text_match(diag, RE_CKD_CORE)
text_has_aki  = text_match(diag, RE_AKI_ONLY)
text_has_chronic_word = text_match(diag, RE_CHRONIC_WORD)

m_ckd_text = text_has_ckd | (text_has_aki & text_has_chronic_word)  # acute on chronic -> CKD
m_ckd_code = icd_prefix_match_any(diag, code_cols, ICD10_CKD + ICD9_CKD)
m_ckd = m_ckd_text | m_ckd_code

# Chronic lung disease
m_cld = text_match(diag, RE_CLD) | icd_prefix_match_any(diag, code_cols, ICD10_CLD + ICD9_CLD)

# Chronic endocrine disease (excluding diabetes)
m_endo = (text_match(diag, RE_ENDO) | icd_prefix_match_any(diag, code_cols, ICD10_ENDO + ICD9_ENDO)) & (~m_diab)

# ========= Aggregate to patientunitstayid (any hit -> 1) =========
flags = (
    pd.DataFrame({
        "patientunitstayid": diag["patientunitstayid"],
        "diabetes": m_diab.astype(int),
        "hypertension": m_htn.astype(int),
        "ckd": m_ckd.astype(int),
        "chronic_lung_dz": m_cld.astype(int),
        "chronic_endocrine_dz": m_endo.astype(int),
    })
    .groupby("patientunitstayid", as_index=False)
    .max()
)

# ========= Left-join with baseline and fill missing with 0 =========
out = base[["patientunitstayid"]].drop_duplicates().merge(flags, on="patientunitstayid", how="left")
for col in ["diabetes", "hypertension", "ckd", "chronic_lung_dz", "chronic_endocrine_dz"]:
    out[col] = out[col].fillna(0).astype("int8")

# ========= Save =========
out.to_csv(OUT_PATH, index=False)
print(f"Wrote: {OUT_PATH}")
print(out.head(10))
