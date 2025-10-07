# Search for CABG and CAD — ICD Code Extraction

## Purpose
This module extracts **ICD codes and descriptions** for two cardiac topics:
- **CAD** — Coronary Artery Disease (diagnoses)
- **CABG** — Coronary Artery Bypass Grafting (procedures)

It reads the standard dictionary tables, applies robust keyword/regex matching (case-insensitive, synonym-aware), and writes two tidy CSVs with exactly two columns: `ICD_CODE`, `DESCRIPTION`.

---

## What it does
- **Inputs**
  - `D_ICD_DIAGNOSES.csv` (diagnosis dictionary)
  - `D_ICD_PROCEDURES.csv` (procedure dictionary)
- **Outputs**
  - `icd_extracts/cad_diagnosis_icd.csv`
  - `icd_extracts/cabg_procedure_icd.csv`

Each output contains unique rows with the two columns only.

---

## Matching logic (high-recall, cardiac-specific)
We use non-capturing regex patterns (no warnings) and allow common synonyms/variants:

```txt
CAD (diagnoses)
- \bcoronary artery disease\b
- \bcoronary atheroscl
- \batherosclerotic heart disease\b
- \bische?mic heart disease\b.*coronary
- \bcoronary arter(?:ioscl|y)\b
- \bchronic ischemic heart disease\b.*coronary
- \batherosclerosis of (?:native )?coronary artery\b
- \bCAD\b

CABG (procedures)
- \baorto.?coronary.*bypass\b
- \bcoronary artery bypass\b
- \bbypass.*coronary artery\b
- \bCABG\b
- \bcoronary revascularization\b.*bypass
