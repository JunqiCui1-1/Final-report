# README — Lab Cleaning Spec (Creatinine / Sodium / Potassium / Hemoglobin / Albumin)

## Purpose
Standardize extraction and cleaning of four core labs (Creatinine, Sodium, Potassium, Hemoglobin) plus Albumin from MIMIC-style and eICU-style sources. Outputs use **unified units**, **fixed value ranges**, and the **earliest valid measurement per individual**, to enable reliable analysis and modeling.

---

## Inputs
- **MIMIC-like (subject-level)**
  - `/content/filtered_by_ids/LABEVENTS.csv`
  - `/content/filtered_by_ids_20251008_090531/labevents.csv`
- **eICU-like (stay-level)**
  - `/content/filtered_by_patientunitstayid_20251009_013746/lab.csv`

> Albumin is also extracted from these sources:
> - MIMIC: `LABEVENTS` with `itemid = 50862`
> - eICU: `labname` contains “albumin” (or “白蛋白”)

---

## Variables and Target Units
| Variable    | Target Unit |
|-------------|-------------|
| Creatinine  | **mg/dL**   |
| Sodium      | **mmol/L**  |
| Potassium   | **mmol/L**  |
| Hemoglobin  | **g/dL**    |
| Albumin     | **g/dL**    |

---

## Fixed Cleaning Ranges (applied consistently across all files)
Values outside these ranges are set to NA. Additionally, **≤ 0** is set to NA for Creatinine, Potassium, and Hemoglobin.

| Variable    | Keep if (inclusive) | Extra rule (≤ 0 → NA) |
|-------------|----------------------|------------------------|
| Creatinine  | **0.1 – 20.0 mg/dL** | ✅                     |
| Sodium      | **110 – 170 mmol/L** | —                      |
| Potassium   | **1.5 – 8.0 mmol/L** | ✅                     |
| Hemoglobin  | **3 – 22 g/dL**      | ✅                     |
| Albumin     | **2 – 5 g/dL**       | — (typically not needed)|

> Albumin cleaning rule (from previous spec): **Albumin < 2 or > 5 → NA**.

---

## Source → Target Mapping

### MIMIC (`LABEVENTS`)
- **One ID per analyte** (choose the most frequent *blood* item):
  - Creatinine → `itemid = 50912`
  - Sodium     → `itemid = 50983`
  - Potassium  → `itemid = 50971`
  - Hemoglobin → `itemid = 51222`
  - Albumin    → `itemid = 50862`
- Required columns: `SUBJECT_ID/subject_id`, `ITEMID`, `VALUENUM`, `VALUEUOM`, `CHARTTIME`
- **Unit policy**
  - Sodium & Potassium: accept `mmol/L` and `mEq/L` (numeric 1:1). Output label normalized to `mmol/L`.
  - Others: keep rows already in the target unit; drop/convert others (see conversions below).
- **Cleaning**
  - Apply the fixed ranges table above.
  - Also set **≤ 0 → NA** for Creatinine, Potassium, Hemoglobin.
- **Selection**

- > Albumin is typically exported as a separate 4-column table (see below).

### eICU (`lab.csv`)
- **Row selection by name** (case-insensitive; exclude “urine”):
- Creatinine: `creatin*`
- Sodium: `sodium`, `na`, `na+`
- Potassium: `potassium`, `k`, `k+`
- Hemoglobin: `hemoglobin`, `hgb`, `hb`
- Albumin: `albumin`
- Required columns: `patientunitstayid`, `labname`, `labresult`, `labresultoffset`, and a unit column (e.g., `labmeasurenamesystem` / `labunit` / `units` / `valueuom`, depending on source)
- **Unit conversion → target unit**
- Creatinine → mg/dL  
  - `mg/L → mg/dL`: ÷ 10  
  - `µmol/L (umol/L) → mg/dL`: ÷ **88.4**
- Sodium → mmol/L  
  - `mEq/L → mmol/L`: 1:1  
  - `mg/dL → mmol/L`: × **(10 / 22.989)**
- Potassium → mmol/L  
  - `mEq/L → mmol/L`: 1:1  
  - `mg/dL → mmol/L`: × **(10 / 39.098)**
- Hemoglobin → g/dL  
  - `g/L → g/dL`: ÷ 10  
  - `mmol/L → g/dL`: × **6.45**
- Albumin → g/dL  
  - `g/L → g/dL`: ÷ 10  
  - `mg/dL → g/dL`: ÷ **100**
- **Cleaning**
- Apply the fixed ranges table above.
- Also set **≤ 0 → NA** for Creatinine, Potassium, Hemoglobin.
- **Selection**
- Per `patientunitstayid`, keep the **earliest valid** record (smallest `labresultoffset`).
- **Output schema (13 columns)**

  - Per `SUBJECT_ID`, keep the **earliest valid** record (smallest `CHARTTIME`).
- **Output schema (13 columns)**
