# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from pathlib import Path

# =============== CONFIG ===============
BASE_IDS_CSV = "/content/patientunitstayid_merged.csv"
VAR_FILES = {
    "Albumin":     "/content/albumin_from_lab_clean.csv",
    "Creatinine":  "/content/creatinine_long_4cols.csv",
    "Hemoglobin":  "/content/hemoglobin_long_4cols.csv",
    "Potassium":   "/content/potassium_long_4cols.csv",
    "Sodium":      "/content/sodium_long_4cols.csv",
}
OUT_CSV = "/content/dynamic_2h_panel_5labs_abs_8h_30d.csv"

# Time window (use "positive time": |hours| ∈ [8h, 720h)), 2-hour bins at even endpoints.
# Right endpoints: 10, 12, ..., 720
ABS_START_H = 8
ABS_END_H   = 30 * 24   # 720
BIN_H       = 2
GRID_ENDS   = np.arange(ABS_START_H + BIN_H, ABS_END_H + 1, BIN_H)  # 10..720
N_BINS      = len(GRID_ENDS)

# =============== UTILITIES ===============
def _guess_patient_col(cols):
    cset = {c.lower(): c for c in cols}
    for key in ["patientunitstayid", "patient_id", "subject_id", "pid", "patient"]:
        if key in cset: return cset[key]
    raise ValueError("Patient ID column not found (e.g., patientunitstayid/subject_id).")

def _guess_time_col(cols, var_name=None):
    """Prefer *_charttime / labresultoffset / offset / hours / charttime, etc."""
    lowmap = {c.lower(): c for c in cols}
    if var_name:
        cand = f"{var_name.lower()}_charttime"
        if cand in lowmap: return lowmap[cand]
    for key in ["labresultoffset", "offset", "timeoffset", "hours", "hour", "charttime", "time", "timestamp"]:
        if key in lowmap: return lowmap[key]
    # Fallback: pick any column containing charttime/offset
    for c in cols:
        lc = c.lower()
        if ("charttime" in lc) or ("offset" in lc) or (lc == "time"):
            return c
    raise ValueError("Time column not found (e.g., *_charttime/labresultoffset/charttime/hours).")

def _guess_value_col(df, var_name=None):
    cols = df.columns
    lowmap = {c.lower(): c for c in cols}
    if var_name and var_name.lower() in lowmap:
        return lowmap[var_name.lower()]
    for key in ["valuenum", "value", "labresult", "resultvalue", "measurevalue"]:
        if key in lowmap: return lowmap[key]
    # Fallback: choose a numeric column (excluding patient/time/unit/flag)
    for c in cols:
        lc = c.lower()
        if any(k in lc for k in ["unit", "uom", "flag"]):
            continue
        if lc in ["patientunitstayid", "patient_id", "subject_id"]:
            continue
        if pd.api.types.is_numeric_dtype(df[c]):
            return c
    raise ValueError("Numeric value column not found (e.g., variable name/valuenum/value/labresult).")

def _ensure_hours_series(s, time_col_name):
    """
    Normalize to 'relative hours from t0' (can be negative). Auto-detect units:
      - If column name contains 'offset' or ends with '_charttime': treat as offset.
        If |median| > 500, assume minutes and divide by 60; otherwise hours.
      - Otherwise try to parse directly as numeric hours.
      - If it's an absolute timestamp (datetime), raise since we need per-patient t0 to convert.
    """
    name = time_col_name.lower()
    s_num = pd.to_numeric(s, errors="coerce")

    if ("offset" in name) or name.endswith("_charttime"):
        med = float(np.nanmedian(s_num.values)) if s_num.notna().any() else np.nan
        if np.isnan(med):
            raise ValueError(f"{time_col_name} cannot be parsed as numeric offset.")
        return s_num / 60.0 if abs(med) > 500 else s_num

    if s_num.notna().any():
        return s_num

    s_dt = pd.to_datetime(s, errors="coerce")
    if s_dt.notna().any():
        raise ValueError(
            f"Detected absolute timestamps in {time_col_name}. Missing per-patient t0 (surgery/ICU admission) to convert to relative hours."
        )
    raise ValueError(f"Failed to parse {time_col_name} as hour offsets.")

def _assign_bin_end_hours_abs(h_series):
    """
    Positive-time binning: map records with |hours| in [8, 720) to the 2h window's right endpoint (10..720).
    Use the right-closed symmetry rule for (-2h, 0]: idx = ceil((|h|-8)/2), end = 8 + idx*2.
    """
    abs_h = h_series.abs()
    mask = (abs_h >= ABS_START_H) & (abs_h < ABS_END_H)
    h = abs_h[mask].to_numpy(dtype=float)

    idx = np.ceil((h - ABS_START_H) / BIN_H)
    idx = np.clip(idx, 1, N_BINS).astype(int)
    bin_end = ABS_START_H + idx * BIN_H  # 10..720

    out = pd.Series(index=h_series.index[mask], data=bin_end)
    return out

def _bin_one_variable(var_name, csv_path):
    """
    Read a long-format variable CSV and discretize to 2h bins using 'positive time'.
    For each patient × bin, keep the *last* record within the window.
    Returns: columns [patientunitstayid, rel_hour(positive), <var>, <var>_obs]
    """
    df = pd.read_csv(csv_path)
    patient_col = _guess_patient_col(df.columns)
    time_col    = _guess_time_col(df.columns, var_name)
    value_col   = _guess_value_col(df, var_name)

    # Normalize ID
    df[patient_col] = pd.to_numeric(df[patient_col], errors="coerce").astype("Int64")
    df = df[df[patient_col].notna()].copy()
    df[patient_col] = df[patient_col].astype(int)

    # Normalize hour offsets (may be negative), then bin by absolute value
    df["_hours"] = _ensure_hours_series(df[time_col], time_col)
    df["_rel_hour"] = _assign_bin_end_hours_abs(df["_hours"])

    # Keep only rows that fall into the window
    df = df[df["_rel_hour"].notna()].copy()

    # Numeric values
    df["_val"] = pd.to_numeric(df[value_col], errors="coerce")

    # For each patient × bin: keep the last record in the window (by original |hours|)
    df = df.sort_values([patient_col, "_rel_hour", "_hours"])
    agg = (
        df.groupby([patient_col, "_rel_hour"], as_index=False)
          .tail(1)[[patient_col, "_rel_hour", "_val"]]
          .rename(columns={patient_col: "patientunitstayid",
                           "_rel_hour": "rel_hour",
                           "_val": var_name})
    )
    agg[f"{var_name}_obs"] = 1
    return agg

# =============== MAIN ===============
def main():
    # 1) Read base IDs and build the patient × time skeleton (positive-time grid)
    ids = pd.read_csv(BASE_IDS_CSV)
    base_id_col = _guess_patient_col(ids.columns)
    ids[base_id_col] = pd.to_numeric(ids[base_id_col], errors="coerce").astype("Int64")
    ids = ids[ids[base_id_col].notna()].drop_duplicates(subset=[base_id_col]).copy()
    ids[base_id_col] = ids[base_id_col].astype(int)
    ids = ids.rename(columns={base_id_col: "patientunitstayid"})
    ids = ids[["patientunitstayid"]].drop_duplicates().reset_index(drop=True)

    grid = pd.DataFrame({"rel_hour": GRID_ENDS})  # 10..720
    skeleton = ids.merge(grid, how="cross")  # Patient × all 2h right endpoints (positive)

    panel = skeleton.copy()

    # 2) Discretize and merge variables
    for var, path in VAR_FILES.items():
        print(f"[INFO] Processing {var} <- {path}")
        try:
            agg = _bin_one_variable(var, path)
        except Exception as e:
            print(f"[WARN] Failed to process {var}: {e}. Using all-missing placeholder.")
            agg = pd.DataFrame(columns=["patientunitstayid", "rel_hour", var, f"{var}_obs"])

        panel = panel.merge(agg, on=["patientunitstayid", "rel_hour"], how="left")
        obs_col = f"{var}_obs"
        if obs_col in panel.columns:
            panel[obs_col] = panel[obs_col].fillna(0).astype(int)
        else:
            panel[obs_col] = 0

    # 3) Sort & export
    panel = panel.sort_values(["patientunitstayid", "rel_hour"]).reset_index(drop=True)
    Path(OUT_CSV).parent.mkdir(parents=True, exist_ok=True)
    panel.to_csv(OUT_CSV, index=False)
    print(f"[OK] Exported panel to: {OUT_CSV}")

    # 4) Coverage summary (ID-level: covered if there is ≥1 observation)
    total_ids = ids["patientunitstayid"].nunique()
    cov = {}
    for var in VAR_FILES:
        obs_col = f"{var}_obs"
        covered = (
            panel.groupby("patientunitstayid")[obs_col]
                 .max()
                 .reindex(ids["patientunitstayid"].unique(), fill_value=0)
                 .sum()
        )
        cov[var] = (int(covered), total_ids, round(covered/total_ids*100, 2))
    print("[COVERAGE - Unique ID level]", cov)

if __name__ == "__main__":
    main()
