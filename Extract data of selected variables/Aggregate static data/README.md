# README — How the Three Final Aggregated Files Were Built

This document summarizes **how each of the three final datasets was aggregated** from their source files, including join keys, row-reduction rules, and key cleaning steps.

---

## 📦 Outputs (3 files)

1) **eICU (patientunitstayid-level)**
   - `/content/patientunitstayid_merged.csv`

2) **MIMIC-IV (subject-level, first ICU stay cohort)**
   - `/content/subject_level_merged_firstICU.csv`

3) **MIMIC-III (subject-level, first ICU stay cohort)**
   - `/content/subject_level_firstICU_MIMICIII.csv`

---

## 🧩 Common Conventions

- **ID normalization**
  - Convert ID columns to string, trim whitespace, and remove trailing `.0`.

- **Binary vs. non-binary aggregation**
  - When reducing multiple rows **within a file** to one row per ID:
    - **Binary 0/1 columns** → take **max** (if any 1 ⇒ 1).
    - **Non-binary columns** → take **first non-null** value.

- **Sex recoding**
  - Map to numeric (nullable Int64):
    - `M`/`male` ⇒ **1**
    - `F`/`female` ⇒ **0**
    - anything else ⇒ **NA**
  - If multiple sex-like columns arise after merges, **collapse** to a single `Sex`/`sex` using row-wise max.

---

## 1) `/content/patientunitstayid_merged.csv` (eICU)

**Goal:** Produce a **patientunitstayid-level** table by merging four eICU sources.

**Inputs**
- `/content/patient_eICU.csv`
- `/content/patientunitstayid_Death.csv`
- `/content/patientunitstayid_Sex_Age_Weight.csv`
- `/content/patientunitstayid_comorbidities.csv`

**Key & Merge Strategy**
- **Join key:** `patientunitstayid`
- **Join type:** **outer join** across the four files to keep all IDs observed in any source.

**Per-file Reduction (before merge)**
- Group by `patientunitstayid` and reduce to **one row per ID** using the **binary/non-binary** rules above.

**Cleaning & Harmonization**
- **Sex:** recode `Male/Female/M/F` → `1/0` (nullable Int64).
- If multiple `Sex` columns appear post-merge (e.g., `Sex_x`, `Sex_y`), **collapse** to a single `Sex`.

**Result**
- One row per `patientunitstayid` with harmonized demographics, comorbidities, and death flag where available.

---

## 2) `/content/subject_level_merged_firstICU.csv` (MIMIC-IV)

**Goal:** Build a **subject-level** dataset restricted to each subject’s **first ICU admission**, then merge additional sources.

**Inputs**
- Cohort driver: `/content/icustays_mimic_IV.csv`
- Additional sources:
  - `/content/CAD_CABG_loose_intersection_comorbidities_hadm_or_subject.csv`
  - `/content/admissions_subject_hadm_death.csv`
  - `/content/patients_subject_sex_age.csv`
  - `/content/WEIGHT_filtered.csv`

**Step A — First ICU Cohort**
- In `/content/icustays_mimic_IV.csv`:
  - Normalize `subject_id`, parse `intime` to datetime.
  - Sort by `subject_id, intime` ascending and **keep the earliest row per `subject_id`**.
  - Retain `subject_id` and earliest time (as `first_icu_intime`).
  - The resulting subject set defines the **cohort** for downstream merges.

**Step B — Per-file Reduction**
- For each additional source:
  - Normalize `subject_id`.
  - **Drop all `hadm_id` columns** (any case, including suffixed variants).
  - Restrict to the **cohort** from Step A.
  - Reduce to **one row per `subject_id`** using the **binary/non-binary** rules above.

**Step C — Merge & Clean**
- **Left-join** each reduced table onto the cohort driver to keep **cohort subjects only**.
- **Drop any residual `hadm_id` columns**.
- **Sex:** recode `M/F` → `1/0` and collapse any duplicates to a single `Sex` (nullable Int64).

**Result**
- One row per `subject_id` for the first-ICU cohort with demographics, admissions/death flags, comorbidities, and weight where available.

---

## 3) `/content/subject_level_firstICU_MIMICIII.csv` (MIMIC-III)

**Goal:** Build a **subject-level** dataset restricted to each subject’s **first ICU admission**, then merge additional MIMIC-III sources.

**Inputs**
- Cohort driver: `/content/ICUSTAYS_mimic_III.csv`
- Additional sources:
  - `/content/ITEMID226512_filtered.csv`
  - `/content/PATIENTS_mimic_III.csv`
  - `/content/mimic_III_death.csv`
  - `/content/patient_ids_SUBJECT_HADM_comorbidities_or.csv`

**Step A — First ICU Cohort**
- In `/content/ICUSTAYS_mimic_III.csv`:
  - Normalize `SUBJECT_ID`, parse `INTIME` to datetime.
  - Sort by `SUBJECT_ID, INTIME` ascending and **keep the earliest row per `SUBJECT_ID`**.
  - Retain `SUBJECT_ID` and earliest time (as `FIRST_ICU_INTIME`).
  - The resulting subject set defines the **cohort**.

**Step B — Per-file Reduction**
- For each additional source:
  - Normalize `SUBJECT_ID`.
  - **Drop all `HADM_ID` columns** (case-insensitive, including suffixed variants).
  - Restrict to the **cohort** from Step A.
  - Reduce to **one row per `SUBJECT_ID`** using the **binary/non-binary** rules above.

**Step C — Merge & Clean**
- **Left-join** each reduced table onto the cohort driver (cohort subjects only).
- **Drop any residual `HADM_ID` columns**.
- **sex/gender:** recode `M/F` → `1/0` and **collapse** to a single `sex` column (nullable Int64).

**Result**
- One row per `SUBJECT_ID` for the first-ICU cohort with patient demographics, outcomes, comorbidities, and selected item data where available.

---

## ✅ Quality Checks (suggested for all three builds)

- **Uniqueness:** Each output must have unique keys  
  (`patientunitstayid` for eICU; `subject_id`/`SUBJECT_ID` for MIMIC).
- **No `hadm_id` leakage:** Confirm no `hadm_id` columns remain in MIMIC outputs.
- **Sex encoding:** Values limited to `{1, 0, <NA>}`; dtype is nullable Int64.
- **Row counts:**  
  - eICU: equals number of unique `patientunitstayid` across sources (outer-join result).  
  - MIMIC-IV & MIMIC-III: equals number of cohort subjects (first-ICU sets).

---
