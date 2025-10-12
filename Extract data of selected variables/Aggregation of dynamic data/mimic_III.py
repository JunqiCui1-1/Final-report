# -*- coding: utf-8 -*-
"""
Dynamic 2-hour panel for five labs using FIRST_ICU_INTIME as surgery t0.
- Base cohort: /content/subject_level_firstICU_MIMICIII.csv (all SUBJECT_IDs)
- t0: FIRST_ICU_INTIME (interpreted as the surgery time)
- Window: |hours from t0| in [8h, 30d), binned at 2h endpoints (10..720)
- For each SUBJECT_ID × bin, keep the *last* record within the bin
- Output: wide panel with five lab columns + five <var>_obs flags
"""

import pandas as pd
import numpy as np
from pathlib import Path

# =============== CONFIG ===============
BASE_IDS_CSV = "/content/subject_level_firstICU_MIMICIII.csv"
T0_COL_NAME  = "FIRST_ICU_INTIME"  # surgery time baseline (t0)

VAR_FILES = {
    "Albumin":     "/content/albumin_50862_clean.csv",
    "Creatinine":  "/content/creatinine_long_4cols.csv",
    "Hemoglobin":  "/content/hemoglobin_long_4cols.csv",
    "Potassium":   "/content/potassium_long_4cols.csv",
    "Sodium":      "/content/sodium_long_4cols.csv",
}
OUT_CSV = "/content/mimiciii_dynamic_2h_panel_5labs_abs_8h_30d.csv"

# Positive-time window: |hours| ∈ [8, 720) with 2h endpoints 10..720
ABS_START_H = 8
ABS_END_H   = 30 * 24   # 720
BIN_H       = 2
GRID_ENDS   = np.arange(ABS_START_H + BIN_H, ABS_END_H + 1, BIN_H)  # 10..720
N_BINS      = len(GRID_ENDS)


# =============== UTILITIES ===============
def _guess_patient_col(cols):
    low = {c.lower(): c for c in cols}
    for key in ["subject_id", "patientunitstayid", "patient_id", "pid", "patient"]:
        if key in low:
            return low[key]
    raise ValueError("Patient ID column not found (e.g., SUBJECT_ID/subject_id).")

def _find_col_case_insensitive(cols, target):
    low = {c.lower(): c for c in cols}
    return low.get(target.lower(), None)

def _guess_time_col(cols, var_name=None):
    """Prefer *_charttime / labresultoffset / offset / hours / charttime / time / timestamp (case-insensitive)."""
    low = {c.lower(): c for c in cols}
    if var_name:
        cand = f"{var_name.lower()}_charttime"
        if cand in low:
            return low[cand]
    for key in ["labresultoffset", "offset", "timeoffset", "hours", "hour", "charttime", "time", "timestamp"]:
        if key in low:
            return low[key]
    # Fallback: any column containing charttime/offset/time keywords
    for c in cols:
        lc = c.lower()
        if ("charttime" in lc) or ("offset" in lc) or (lc in ["time", "timestamp"]):
            return c
    raise ValueError("Time column not found (e.g., charttime/labresultoffset/offset/hours).")

def _guess_value_col(df, var_name=None):
    low = {c.lower(): c for c in df.columns}
    if var_name and var_name.lower() in low:
        return low[var_name.lower()]
    for key in ["valuenum", "value", "labresult", "resultvalue", "measurevalue"]:
        if key in low:
            return low[key]
    # Fallback: first numeric column excluding id/unit/flag/time-like names
    for c in df.columns:
        lc = c.lower()
        if any(k in lc for k in ["unit", "uom", "flag", "time", "offset", "charttime"]):
            continue
        if lc in ["subject_id", "patientunitstayid", "patient_id", "pid", "patient"]:
            continue
        if pd.api.types.is_numeric_dtype(df[c]):
            return c
    raise ValueError("Numeric value column not found.")

def _ensure_hours_series(s, time_col_name):
    """
    Try to interpret the time column as a relative offset (hours or minutes).
    Rules:
      - If name contains 'offset' or endswith '_charttime' or equals 'hours/hour':
          treat as numeric offset; if |median|>500, assume minutes → /60; else hours.
      - Else, if numeric → treat as hours.
      - Else, return None (likely absolute timestamps; will convert using t0).
    """
    name = time_col_name.lower()
    s_num = pd.to_numeric(s, errors="coerce")
    if ("offset" in name) or name.endswith("_charttime") or (name in ["hours", "hour"]):
        med = float(np.nanmedian(s_num.values)) if s_num.notna().any() else np.nan
        if np.isnan(med):
            return None
        return s_num / 60.0 if abs(med) > 500 else s_num
    if s_num.notna().any():
        return s_num
    return None

def _to_relative_hours_with_t0(df, time_col, t0_map, id_col):
    """Convert absolute timestamps to relative hours using per-subject t0."""
    ts = pd.to_datetime(df[time_col], errors="coerce")
    t0 = df[id_col].map(t0_map)
    rel_hours = (ts - t0).dt.total_seconds() / 3600.0
    return rel_hours

def _assign_bin_end_hours_abs(h_series):
    """
    Positive-time binning: map |hours| in [8, 720) to 2h right endpoints (10..720).
    idx = ceil((|h|-8)/2); end = 8 + idx*2 (clamped).
    """
    abs_h = h_series.abs()
    mask = (abs_h >= ABS_START_H) & (abs_h < ABS_END_H)
    h = abs_h[mask].to_numpy(dtype=float)
    idx = np.ceil((h - ABS_START_H) / BIN_H)
    idx = np.clip(idx, 1, N_BINS).astype(int)
    bin_end = ABS_START_H + idx * BIN_H
    out = pd.Series(index=h_series.index[mask], data=bin_end)
    return out

def _load_base_ids_and_t0():
    """Load SUBJECT_IDs and mandatory t0 from FIRST_ICU_INTIME in the base CSV."""
    base = pd.read_csv(BASE_IDS_CSV)
    id_col = _guess_patient_col(base.columns)
    base[id_col] = pd.to_numeric(base[id_col], errors="coerce").astype("Int64")
    base = base[base[id_col].notna()].copy()
    base[id_col] = base[id_col].astype(int)

    # Find FIRST_ICU_INTIME (case-insensitive exact name)
    t0_col = _find_col_case_insensitive(base.columns, T0_COL_NAME)
    if t0_col is None:
        raise ValueError(
            f"Base table must contain '{T0_COL_NAME}' as the surgery time t0."
        )
    base["_t0"] = pd.to_datetime(base[t0_col], errors="coerce")

    # Normalize id name to SUBJECT_ID
    base = base.rename(columns={id_col: "SUBJECT_ID"})
    ids = base[["SUBJECT_ID"]].drop_duplicates().reset_index(drop=True)

    # Build t0 map (allow NaT; rows with NaT will drop if a var file only has absolute times)
    t0_map = base.set_index("SUBJECT_ID")["_t0"]
    return ids, t0_map

def _bin_one_variable(var_name, csv_path, ids_df, t0_map):
    """
    Load one variable, resolve time to relative hours using:
      1) direct offsets if present, else
      2) absolute timestamps converted via t0 (FIRST_ICU_INTIME).
    Then bin to 2h positive-time grid and keep last record per SUBJECT_ID × bin.
    Return: [SUBJECT_ID, rel_hour, <var>, <var>_obs]
    """
    df = pd.read_csv(csv_path)
    id_col   = _guess_patient_col(df.columns)
    time_col = _guess_time_col(df.columns, var_name)
    val_col  = _guess_value_col(df, var_name)

    # Normalize ID
    df[id_col] = pd.to_numeric(df[id_col], errors="coerce").astype("Int64")
    df = df[df[id_col].notna()].copy()
    df[id_col] = df[id_col].astype(int)
    df = df.rename(columns={id_col: "SUBJECT_ID"})

    # Keep only base IDs
    df = df[df["SUBJECT_ID"].isin(ids_df["SUBJECT_ID"])].copy()
    if df.empty:
        return pd.DataFrame(columns=["SUBJECT_ID", "rel_hour", var_name, f"{var_name}_obs"])

    # Resolve relative hours
    hours = _ensure_hours_series(df[time_col], time_col)
    if hours is None:
        # Need absolute → relative conversion using t0
        hours = _to_relative_hours_with_t0(df, time_col, t0_map, "SUBJECT_ID")

    df["_hours"] = pd.to_numeric(hours, errors="coerce")
    df["_rel_hour"] = _assign_bin_end_hours_abs(df["_hours"])

    # Keep only rows inside the window
    df = df[df["_rel_hour"].notna()].copy()

    # Values
    df["_val"] = pd.to_numeric(df[val_col], errors="coerce")

    # For each SUBJECT_ID × bin: keep the last record (largest _hours within the bin)
    df = df.sort_values(["SUBJECT_ID", "_rel_hour", "_hours"])
    agg = (
        df.groupby(["SUBJECT_ID", "_rel_hour"], as_index=False)
          .tail(1)[["SUBJECT_ID", "_rel_hour", "_val"]]
          .rename(columns={"_rel_hour": "rel_hour", "_val": var_name})
    )
    agg[f"{var_name}_obs"] = 1
    return agg


# =============== MAIN ===============
def main():
    # 1) Load base IDs and t0 from FIRST_ICU_INTIME
    ids, t0_map = _load_base_ids_and_t0()
    print(f"[INFO] Base cohort size: {len(ids)} subjects. "
          f"t0 column: {T0_COL_NAME} (parsed to datetime).")

    # 2) Patient × time skeleton: 2h right endpoints 10..720
    grid = pd.DataFrame({"rel_hour": GRID_ENDS})
    skeleton = ids.merge(grid, how="cross")  # SUBJECT_ID × all 2h endpoints
    panel = skeleton.copy()

    # 3) Bin and merge variables
    for var, path in VAR_FILES.items():
        print(f"[INFO] Processing {var} <- {path}")
        try:
            agg = _bin_one_variable(var, path, ids, t0_map)
        except Exception as e:
            print(f"[WARN] Failed to process {var}: {e}. Using all-missing placeholder.")
            agg = pd.DataFrame(columns=["SUBJECT_ID", "rel_hour", var, f"{var}_obs"])

        panel = panel.merge(agg, on=["SUBJECT_ID", "rel_hour"], how="left")
        obs_col = f"{var}_obs"
        if obs_col in panel.columns:
            panel[obs_col] = panel[obs_col].fillna(0).astype(int)
        else:
            panel[obs_col] = 0

    # 4) Sort & export
    panel = panel.sort_values(["SUBJECT_ID", "rel_hour"]).reset_index(drop=True)
    Path(OUT_CSV).parent.mkdir(parents=True, exist_ok=True)
    panel.to_csv(OUT_CSV, index=False)
    print(f"[OK] Exported panel to: {OUT_CSV}")

    # 5) Coverage summary (ID-level: covered if there is ≥1 observation)
    total_ids = ids["SUBJECT_ID"].nunique()
    cov = {}
    for var in VAR_FILES:
        obs_col = f"{var}_obs"
        covered = (
            panel.groupby("SUBJECT_ID")[obs_col]
                 .max()
                 .reindex(ids["SUBJECT_ID"].unique(), fill_value=0)
                 .sum()
        )
        cov[var] = (int(covered), total_ids, round(covered/total_ids*100, 2))
    print("[COVERAGE - Unique ID level]", cov)


if __name__ == "__main__":
    main()
