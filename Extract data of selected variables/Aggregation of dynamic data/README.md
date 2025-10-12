# README

This README explains how **three related scripts** aggregate long-format lab data into a **model-ready dynamic panel** using a **2-hour grid** spanning **8 hours pre-op to 30 days post-op**.  
The pipelines differ only in the **base cohort file**, the **ID column**, and the **baseline time (t0)**. The aggregation logic is identical.

---

## What gets built

For every patient and each 2-hour bin (right endpoints: `10, 12, …, 720` hours from t0 **by absolute value**), we keep the **last observation within the bin** for five labs:

- `Albumin`, `Creatinine`, `Hemoglobin`, `Potassium`, `Sodium`

Each lab also has a `<var>_obs` flag (`0/1`) marking whether anything was observed in that bin.  
**All patients** from the base cohort appear in the output; **missing values** remain `NaN`.

---

## Common inputs (five lab files)

- `/content/albumin_50862_clean.csv`  
- `/content/creatinine_long_4cols.csv`  
- `/content/hemoglobin_long_4cols.csv`  
- `/content/potassium_long_4cols.csv`  
- `/content/sodium_long_4cols.csv`

Each CSV may use different column names. The scripts **auto-detect**:

- **ID column** (case-insensitive): e.g., `subject_id`, `patientunitstayid`, `patient_id`, etc.  
- **Time column** (priority order): `*_charttime`, `labresultoffset`, `offset`, `timeoffset`, `hours`, `hour`, `charttime`, `time`, `timestamp`.  
- **Value column** (priority order): the variable name, then `valuenum`, `value`, `labresult`, `resultvalue`, `measurevalue`, or the first numeric column (excluding units/flags/IDs/times).

---

## Time handling (shared across all pipelines)

- **Window**: positive time on absolute hours from t0, i.e. `|hours from t0| ∈ [8, 720)`.

- Records in `(8,10]` map to `10`, `(10,12]` map to `12`, …, `(718,720]` map to `720`.

- **Offsets vs. absolute timestamps**:
- If the time column looks like an **offset** (`offset`, `*_charttime`, `hours`, `hour`): parse as numeric.  
  If `|median| > 500`, treat as **minutes** and divide by 60; else hours.
- Otherwise, treat as an **absolute timestamp** and convert to **relative hours** via per-patient **t0** from the base cohort.

- **Per-bin aggregation**: within each `(patient, rel_hour)` bin, sort by the raw relative hours and take the **last** record (closest to the right endpoint).

---

## Patient × time skeleton (shared)

We cross-join **all IDs** from the base cohort with the grid of `rel_hour` endpoints (`10..720`) to get the **patient × time skeleton**, then left-merge each variable’s binned data.  
This guarantees that **every patient** has **every bin**, even if all five labs are missing.  
Each lab also gets `<var>_obs` filled to `0/1`.

---

## The three pipelines (what changes)

### A) eICU-style (earlier draft)
- **Base**: `/content/patientunitstayid_merged.csv`  
- **ID**: `patientunitstayid`  
- **t0**: offsets expected; absolute timestamps would require a t0 column (not guaranteed).  
- **Output**: `/content/dynamic_2h_panel_5labs_abs_8h_30d.csv`

### B) MIMIC-III (uses `FIRST_ICU_INTIME`)
- **Base**: `/content/subject_level_firstICU_MIMICIII.csv`  
- **ID**: `SUBJECT_ID`  
- **t0 column**: `FIRST_ICU_INTIME` (case-insensitive)  
- **Output**: `/content/mimiciii_dynamic_2h_panel_5labs_abs_8h_30d.csv`

### C) Merged first ICU (current default)
- **Base**: `/content/subject_level_merged_firstICU.csv`  
- **ID**: `subject_id`  
- **t0 column**: `first_icu_intime` (case-insensitive)  
- **Output**: `/content/dynamic_2h_panel_5labs_abs_8h_30d.csv`

> Aside from the base file, ID column, and t0 column, **all other logic is identical**.

---

## Output schema (wide panel)

| Column | Type | Description |
|---|---|---|
| `subject_id` / `patientunitstayid` | int | Patient identifier (depends on pipeline) |
| `rel_hour` | int | Right endpoint of the 2h bin in `{10, 12, …, 720}` |
| `Albumin` … `Sodium` | float | Last value observed within the bin; `NaN` if none |
| `Albumin_obs` … `Sodium_obs` | int (0/1) | `1` if any record landed in the bin; else `0` |

**Semantics**:
- `rel_hour = 10` corresponds to the window `(8h, 10h]` from t0 on **absolute time** (both pre- and post-op are mirrored onto the positive axis).
- Values are **not imputed**; they remain `NaN` when missing. `_obs` flags help models distinguish **unobserved** vs **observed**.

---

## Coverage report

At the end, each script prints per-ID coverage:

```text
[COVERAGE - Unique ID level] {
'Albumin':    (covered_ids, total_ids, percent),
'Creatinine': (...),
'Hemoglobin': (...),
'Potassium':  (...),
'Sodium':     (...)
}

  This captures **8h pre-op → 30d post-op** on a single positive axis.

- **2-hour grid**: bins with right endpoints `10, 12, …, 720`.

- **Bin assignment** (on absolute time):  
