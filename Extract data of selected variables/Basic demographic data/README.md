# Admission Weight Inclusion & Cleaning Rules

## Purpose
This project includes patients **based on admission weight** to ensure consistent, comparable analyses across cohorts. The rules below define how admission weight is handled before any statistics are computed.

## Data Field
- **Admission weight** (stored as `admissionweight` in raw data; renamed to **`Weight`** for analysis).

## Units
- All weights are expressed in **kilograms (kg)**.

## Inclusion & Cleaning
1. Analyses that rely on weight use the **admission weight** value for each patient/admission.
2. To standardize the dataset and remove implausible/out-of-scope values, **any admission weight `< 35 kg` or `> 135 kg` is set to `NA`**.
3. Records with `Weight = NA` are **excluded from numerical summaries** and any computations that require a valid weight value.

## Resulting Variables
- **`Weight`** (kg): numeric; values outside `[35, 135]` are recoded to `NA`.

## Notes
- These thresholds are applied uniformly across all extracted cohorts and tables.
- Downstream analyses should treat `NA` as missing data and exclude or impute as appropriate for the specific statistical method.
