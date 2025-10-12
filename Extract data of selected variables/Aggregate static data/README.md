# README — Subject-Level Aggregation (MIMIC-III)

## Goal
Produce a **subject-level** dataset limited to each subject’s **first ICU admission**, then merge selected sources on `SUBJECT_ID`, with clean keys and harmonized fields (notably `sex`).

## Inputs (3 files)
- `/content/ICUSTAYS_mimic_III.csv`
- `/content/PATIENTS_mimic_III.csv`
- `/content/mimic_III_death.csv`

> The pipeline can be extended to other sources (e.g., `ITEMID226512_filtered.csv`, `patient_ids_SUBJECT_HADM_comorbidities_or.csv`) using the same merge rules described below.

## Output
- `/content/subject_level_firstICU_MIMICIII.csv`

---

## Common Preprocessing (applies to all inputs)
1. **ID normalization**
   - Ensure a `SUBJECT_ID` column exists (case-insensitive aliasing permitted).
   - Cast to string, `strip()` whitespace, and remove trailing `.0`.

2. **Drop all `HADM_ID` columns**
   - Remove any column whose name contains `HADM_ID` (case-insensitive), including merge suffixes.

3. **De-duplication policy (within each file before merging)**
   - Group by `SUBJECT_ID` and reduce to **one row per subject**:
     - **Binary 0/1 columns** → take **max** (if any 1, result = 1).
     - **Non-binary columns** → take the **first non-null** value in time/row order.
   - This yields a clean subject-level slice from each source.

---

## How each file is aggregated

### 1) `ICUSTAYS_mimic_III.csv` → **First ICU stay cohort**
- Parse an ICU admission time column (prefer `INTIME`).
- Convert `INTIME` to datetime and **sort by `SUBJECT_ID`, `INTIME` ascending**.
- For each `SUBJECT_ID`, **keep only the earliest row** (first ICU stay).
- Keep:
  - `SUBJECT_ID`
  - Renamed earliest time as `FIRST_ICU_INTIME`
- This table defines the **cohort** (the set of subjects retained downstream).

### 2) `PATIENTS_mimic_III.csv` → **Demographics**
- After common preprocessing and de-duplication:
  - **Recode `sex`** (case-insensitive; accepts `sex`/`gender`):
    - `M`/`male` → `1`
    - `F`/`female` → `0`
    - anything else → `NA` (nullable `Int64`)
  - If multiple sex-like columns exist (e.g., merge artifacts), **collapse** them to a single `sex` via row-wise max (so any 1 wins; 0 if only 0/NA).
- Result is **one row per subject** with a harmonized `sex` (1/0/NA) and any available demographics (e.g., age) chosen by **first non-null**.

### 3) `mimic_III_death.csv` → **Mortality/outcomes**
- After common preprocessing and de-duplication:
  - All **binary flags** (e.g., in-hospital death, ICU death) are aggregated by **max** per subject.
  - **Datetime** or non-binary fields (e.g., `DEATHTIME`) take the **first non-null** value.
- Output is **one row per subject** containing consolidated outcome indicators.

---

## Merge Order & Keys
1. Start from the **first-ICU cohort** derived from `ICUSTAYS_mimic_III.csv`.
2. **Left-join** the de-duplicated subject-level tables:
   - `PATIENTS_mimic_III.csv` (demographics)
   - `mimic_III_death.csv` (outcomes)
3. Because we left-join onto the cohort table, the final output contains **only subjects with a first ICU stay**.

---

## Post-merge Cleaning
- Run the **`sex` collapse** again if multiple sex-like columns appear after merging.
- Ensure `sex` is `Int64` (nullable) with values `{1, 0, <NA>}`.
- Reconfirm **no `HADM_ID` columns** remain.

---

## Extending to additional sources (optional)
If you also include:
- `/content/ITEMID226512_filtered.csv` (e.g., a specific lab/item),
- `/content/patient_ids_SUBJECT_HADM_comorbidities_or.csv` (comorbidities),

apply **the same per-file aggregation** (group by `SUBJECT_ID`, binary→max, others→first non-null), then left-join them to the first-ICU cohort in the same way as above.

---

## Reproducibility
This README corresponds to the accompanying Python script that:
- builds the **first ICU** cohort,
- aggregates each source to **one row per subject**, 
- **recodes `sex`**, drops `HADM_ID`, and 
- merges everything into `/content/subject_level_firstICU_MIMICIII.csv`.

