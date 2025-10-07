# CAD × CABG Patient ID Extraction — README

> One-stop scripts to extract patient IDs for **coronary artery disease (CAD)** and **coronary artery bypass grafting (CABG)** cohorts from heterogeneous CSV exports (MIMIC-style and eICU-style).

---

## ✨ TL;DR

- **MIMIC-style output:** two columns `SUBJECT_ID,HADM_ID`  
- **eICU-style output:** one column `patientunitstayid`  
- Supports **intersection **, **multi-file inputs**, mixed **ICD lists** or **free-text DESCRIPTION** lists, robust column-name and encoding handling.

---

## 📦 Requirements

- Python ≥ 3.8  
- `pandas` (install with `pip install pandas`)

---

## 🗂️ Files & Formats (what the scripts expect)

### MIMIC-style (diagnoses/procedures + ICD code lists)
- **Tables:** `DIAGNOSES_ICD.csv`, `PROCEDURES_ICD.csv`  
  - Must contain patient and visit IDs and a code column  
  - Accepted column names (any case):  
    - IDs: `SUBJECT_ID/subject_id`, `HADM_ID/hadm_id`  
    - Codes: `icd_code`, `ICD9_CODE`, `ICD10_CODE`, `ICD`, `code`
- **Code lists:** `cad_diagnosis_icd.csv`, `cabg_procedure_icd.csv`  
  - One column with ICD codes is enough; supports trailing `*` as a **prefix wildcard** (e.g., `I25*`)

### eICU-style (diagnosis/treatment + “from_*.csv” DESCRIPTION lists)
- **Tables:** `diagnosis.csv` (has `patientunitstayid`, `diagnosisstring`), `treatment.csv` (has `patientunitstayid`, `treatmentstring`)
- **Lists:** `CAD_from_diagnosis.csv`, `CABG_from_treatment.csv`  
  - Often only have a `DESCRIPTION` column (e.g.,  
    `"<some_id>  <patientunitstayid>  <offset>  cardiovascular|...|CABG  <flag>"`)  
  - Scripts **parse the 2nd integer** in each DESCRIPTION as `patientunitstayid` (fallback: scan all integers and take the 2nd)

---

## 🧠 Matching Rules

- **ICD normalization (MIMIC side):** uppercase, **remove dots**, support `*` prefix wildcards  
- **CAD keywords (eICU diagnosisstring):**  
  `coronary artery disease`, `CAD`, `ischemic heart disease`, `IHD`, `ASHD`, `coronary disease`
- **CABG keywords (eICU treatmentstring):**  
  `CABG`, `coronary artery bypass graft`, `coronary artery bypass`
- **Encodings:** tries `utf-8-sig`, `utf-8`, `gb18030`, `latin1` automatically
- **Notebook-safe:** all CLIs tolerate extra args (e.g., Jupyter’s `-f`)

---

## 🛠️ Scripts Overview

| Script | Inputs | Output | What it does |
|---|---|---|---|
| **`extract_ids.py`** | `DIAGNOSES_ICD.csv`, `PROCEDURES_ICD.csv`, `cad_diagnosis_icd.csv`, `cabg_procedure_icd.csv` | `patient_ids_SUBJECT_HADM.csv` (2 cols) | **Three modes**: `strict` (pairwise intersection on `SUBJECT_ID+HADM_ID`), `loose` (**subject OR hadm** overlaps), `union`. Auto-detects code column; ICD normalization + `*` wildcard. |
| **`extract_loose_ids_multi.py`** | Multi-file inputs (e.g., add `diagnosis.csv`, `CAD_from_diagnosis.csv`, etc.) | `patient_ids_SUBJECT_HADM.csv` (2 cols) | **Multi-file + loose-intersection**. Merges within side, then applies subject-or-hadm overlap. Skips missing files. |
| **`extract_patientunitstayid_intersection.py`** | Same four eICU files | `patientunitstayid_intersection.csv` (1 col) | **Intersection (CAD side ∩ CABG side)**. This is the **strict intersection** version for eICU style. |

> Why results might be 0: if you only tried ICD-column matching on “from_*” files, note that their `ICD_CODE` can be empty and the real info lives in `DESCRIPTION`. Use the eICU scripts above (they parse DESCRIPTION + use text keywords).

---

## 🚀 Quick Start

### MIMIC-style (two-column output)

**Three modes** (default `--mode loose`):
```bash
python extract_ids.py \
  --diagnoses DIAGNOSES_ICD.csv \
  --procedures PROCEDURES_ICD.csv \
  --cad_codes cad_diagnosis_icd.csv \
  --cabg_codes cabg_procedure_icd.csv \
  --out patient_ids_SUBJECT_HADM.csv \
  --mode loose / union
