# ICU Unit Mapping

Unifies ICU unit names from **MIMIC-IV** and **eICU** into the **five MIMIC-III baseline classes**:

`CSRU`, `CCU`, `MICU`, `SICU`, `TSICU`.


---

## Master Mapping Table

| MIMIC-III | MIMIC-IV careunit | eICU unittype |
|---|---|---|
| **CSRU** | Cardiac Vascular ICU (CVICU); Cardiac Surgery ICU; Cardiothoracic ICU (CTICU); CSRU | CCU-CTICU; CSICU; CTICU; Cardiac ICU *(see note)* |
| **CCU**  | Coronary Care Unit (CCU); Cardiac ICU *(see note)* | Cardiac ICU *(see note)* |
| **MICU** | Medical ICU (MICU) | MICU |
| **SICU** | Surgical ICU (SICU); MICU/SICU; Neuro ICU / Neuro SICU; Med-Surg ICU / M/S ICU / MSICU | SICU; Med-Surg ICU; Neuro ICU |
| **TSICU**| Trauma SICU (TSICU); Trauma ICU | TSICU; Trauma ICU; Trauma SICU |

> **Note – “Cardiac ICU”**: By default we map **Cardiac ICU → CCU** (medical cardiac).  
---

## Mapping Rules & Priority (for code)

Apply in order (first match wins; case-insensitive, trim spaces):

1. `TRAUMA` **and** `ICU` → **TSICU**  
2. Heart surgery / cardiothoracic: `CVICU` or `CTICU` or `CSICU`, or `CARDIAC` + (`SURGERY` \| `CTS` \| `CV`) → **CSRU**  
3. `CORONARY` or standalone `CCU` (not part of `MICU`) → **CCU**  
4. `MICU` or `MEDICAL`+`ICU` → **MICU**  
5. Surgical/mixed/neuro: `SICU`, `SURGICAL ICU`, `MICU/SICU`, `MED-SURG ICU`, `M/S ICU`, `MSICU`, `NEURO ICU`, `NSICU` → **SICU**  
6. Non-ICU / stepdown / transitional areas (`PACU`, `INTERMEDIATE`, `STEPDOWN`, pediatrics, neonates) → **NA**

---

## Outputs

- **Mapping workbook**: `icu_mapping_mimicIII_baseline.xlsx`  
  - Sheet **`icu_mapping`**: the master table above  
  - Sheet **`notes`**: guidance on the “Cardiac ICU” choice
- **Harmonized extracts** *(examples)*:
  - `icustays_mimic_III.csv` → columns: `subject_id`, `hadm_id`, `ICUtype`, `intime`, `los`
  - `patient_mimic_III.csv` → columns: `patientunitstayid`, `ICUtype`, `LOS` (where `LOS = unitdischargeoffset / 1440` days)

---
