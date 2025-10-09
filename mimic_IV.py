#!/usr/bin/env python3
"""
Load /content/icustays.csv, keep [subject_id, hadm_id, last_careunit, intime, los],
rename last_careunit -> ICUtype, map units to 5 ICU classes, and mark non-ICU as NA.
Save to /content/icustays_mimic_IV.csv.
"""

from pathlib import Path
import pandas as pd

INPUT = Path("/content/icustays.csv")
OUTPUT = INPUT.with_name(f"{INPUT.stem}_mimic_IV{INPUT.suffix}")
REQUIRED = ["subject_id", "hadm_id", "last_careunit", "intime", "los"]

def map_last_to_icutype(x) -> pd.Series:
    """Return one of {CSRU, CCU, MICU, SICU, TSICU} or <NA> for non-ICU/unknown."""
    if pd.isna(x):
        return pd.NA
    s = str(x).strip().upper()

    # already an abbreviation
    if s in {"CSRU", "CCU", "MICU", "SICU", "TSICU"}:
        return s

    # deterministic aliases
    if "CVICU" in s or "CARDIAC VASCULAR" in s or "CARDIAC SURGERY" in s:
        return "CSRU"
    if "CORONARY" in s:
        return "CCU"
    if "TRAUMA" in s and "SICU" in s:
        return "TSICU"
    if "MICU/SICU" in s:
        return "SICU"
    if "NEURO" in s and "SICU" in s:
        return "SICU"
    if "MICU" in s:
        return "MICU"
    if "SICU" in s:
        return "SICU"

    # non-ICU / stepdown / other -> NA (do not drop, as requested)
    if any(k in s for k in ["PACU", "INTERMEDIATE", "STEPDOWN"]):
        return pd.NA

    # unknown -> NA
    return pd.NA

def main():
    df = pd.read_csv(INPUT)

    # tolerant column resolution (case/spacing)
    name_map = {c.lower().strip(): c for c in df.columns}
    missing = [c for c in REQUIRED if c not in name_map]
    if missing:
        raise KeyError(f"Missing required column(s): {missing}; got {list(df.columns)}")

    # select and rename
    out = (
        df[[name_map[c] for c in REQUIRED]]
        .rename(columns={name_map["last_careunit"]: "ICUtype"})
    )

    # map ICU types; non-ICU stays become <NA> (kept)
    out["ICUtype"] = out["ICUtype"].apply(map_last_to_icutype)

    # enforce output column order
    out = out[["subject_id", "hadm_id", "ICUtype", "intime", "los"]]

    # save
    out.to_csv(OUTPUT, index=False)

    # quick sanity print
    print(f"Saved: {OUTPUT}")
    print("ICUtype distribution (including NA):")
    print(out["ICUtype"].value_counts(dropna=False).sort_index())

if __name__ == "__main__":
    main()
