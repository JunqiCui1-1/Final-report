# Filter CSVs by `id`

A small utility to **filter very large CSV/CSV.GZ files** by a baseline list of `id` values.  
It **keeps the header row** and **writes only the matching rows** to a separate output folder.

---

## Purpose

Given a baseline file, this script:
- Finds the `id` column (case-insensitive).
- Loads its unique IDs.
- Streams through target CSV/CSV.GZ files **in chunks** (memory-friendly).
- Writes **header + matched rows** to `/content/filtered_by_patientunitstayid`.

---

## What it filters

By default, the script processes these files:

- `/content/diagnosis.csv`  
- `/content/lab.csv`  
- `/content/patient.csv`  
- `/content/vitalAperiodic.csv`  
- `/content/vitalPeriodic.csv`

> It also supports `.csv.gz` transparently (`compression='infer'`).

---

## Why chunked filtering?

Many clinical/EHR files are huge. Reading in chunks avoids out-of-memory errors and keeps the workflow stable and reproducible.

---

## Quick start

1. Ensure Python 3.9+ and `pandas` are available:
   ```bash
   pip install -U pandas
