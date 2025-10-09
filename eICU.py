#!/usr/bin/env python3
"""
Process /content/patient.csv:
- Keep [patientunitstayid, unittype, unitdischargeoffset]
- Rename:
    unittype -> ICUtype  (map to {CSRU, CCU, MICU, SICU, TSICU}; non-ICU/unknown -> <NA>)
    unitdischargeoffset -> LOS (minutes -> days by dividing 1440)
- Save to /content/patient_eICU.csv
"""

from pathlib import Path
import pandas as pd

INPUT = Path("/content/patient.csv")
OUTPUT = INPUT.with_name(f"{INPUT.stem}_eICU{INPUT.suffix}")
REQUIRED = ["patientunitstayid", "unittype", "unitdischargeoffset"]

# Hand-tuned aliases for your current values (fast path)
DIRECT_MAP = {
    "CCU-CTICU": "CSRU",
    "CSICU": "CSRU",
    "CTICU": "CSRU",
    "CARDIAC ICU": "CCU",  # change to "CSRU" if you prefer surgical tilt
    "MICU": "MICU",
    "MED-SURG ICU": "SICU",
    "NEURO ICU": "SICU",
    "SICU": "SICU",
    "TSICU": "TSICU",
    "TRAUMA ICU": "TSICU",
    "TRAUMA SICU": "TSICU",
}

def map_unittype(x):
    """Return one of {CSRU, CCU, MICU, SICU, TSICU} or <NA> for non-ICU/unknown."""
    if pd.isna(x):
        return pd.NA
    s = str(x).strip().upper()

    # Exact hits
    if s in DIRECT_MAP:
        return DIRECT_MAP[s]

    # Rule-based fallbacks (priority)
    if "TRAUMA" in s and "ICU" in s:
        return "TSICU"
    if any(k in s for k in ["CVICU", "CTICU", "CSICU"]) or (
        "CARDIAC" in s and any(k in s for k in ["SURGERY", "CTS", "CV"])
    ):
        return "CSRU"
    if "CORONARY" in s or (("CCU" in s) and ("MICU" not in s)):
        return "CCU"
    if ("MICU" in s) or ("MEDICAL" in s and "ICU" in s):
        return "MICU"
    if any(k in s for k in ["SICU", "SURGICAL ICU", "MED-SURG ICU", "MICU/SICU", "M/S ICU", "MSICU", "NEURO ICU", "NSICU"]):
        return "SICU"

    # Non-ICU / stepdown / others -> NA (kept)
    if any(k in s for k in ["PACU", "INTERMEDIATE", "STEPDOWN", "PEDI", "NEONAT"]):
        return pd.NA

    return pd.NA

def main():
    df = pd.read_csv(INPUT)

    # Tolerant column resolution (case/spacing)
    cmap = {c.lower().strip(): c for c in df.columns}
    missing = [c for c in REQUIRED if c not in cmap]
    if missing:
        raise KeyError(f"Missing required column(s): {missing}; got {list(df.columns)}")

    # Select needed columns
    out = df[[cmap[c] for c in REQUIRED]].copy()

    # Rename columns
    out = out.rename(columns={
        cmap["unittype"]: "ICUtype",
        cmap["unitdischargeoffset"]: "LOS",
        # patientunitstayid keeps its original name
    })

    # Map ICUtype; keep NA for non-ICU/unknown (do NOT drop)
    out["ICUtype"] = out["ICUtype"].apply(map_unittype)

    # Convert LOS from minutes to days
    out["LOS"] = pd.to_numeric(out["LOS"], errors="coerce") / 1440.0

    # Enforce output column order
    out = out[["patientunitstayid", "ICUtype", "LOS"]]

    # Save
    out.to_csv(OUTPUT, index=False)

    # Quick sanity prints
    print(f"Saved: {OUTPUT}")
    print("ICUtype distribution (incl. NA):")
    print(out["ICUtype"].value_counts(dropna=False).sort_index())
    print("LOS summary (days):")
    print(out["LOS"].describe())

if __name__ == "__main__":
    main()

