#!/usr/bin/env python3
"""
Read /content/ICUSTAYS.csv, keep 5 columns, rename LAST_CAREUNIT -> ICUtype,
and save as <original_stem>_mimic_III<original_suffix>.
"""

from pathlib import Path
import pandas as pd

INPUT = Path("/content/ICUSTAYS.csv")
REQUIRED_COLS = ["SUBJECT_ID", "HADM_ID", "LAST_CAREUNIT", "INTIME", "LOS"]


def _select_and_rename(df: pd.DataFrame) -> pd.DataFrame:
    """Keep required columns and rename LAST_CAREUNIT -> ICUtype, preserving order."""
    df = df.rename(columns={"LAST_CAREUNIT": "ICUtype"})
    return df[["SUBJECT_ID", "HADM_ID", "ICUtype", "INTIME", "LOS"]]


def load_icustays(path: Path) -> pd.DataFrame:
    """
    Load ICUSTAYS with a robust fallback:
    - First, try selecting columns directly (fast).
    - If that fails (e.g., stray spaces or case issues), strip headers and map case-insensitively.
    """
    try:
        df = pd.read_csv(path, usecols=REQUIRED_COLS)
        return _select_and_rename(df)
    except ValueError:
        # Fallback: read all columns, normalize headers, map case-insensitively
        df = pd.read_csv(path)
        df.columns = df.columns.str.strip()
        upper_map = {c.upper(): c for c in df.columns}
        missing = [c for c in REQUIRED_COLS if c not in upper_map]
        if missing:
            raise KeyError(f"Missing required column(s): {missing}")
        df = df[[upper_map[c] for c in REQUIRED_COLS]]
        return _select_and_rename(df)


def save_with_suffix(df: pd.DataFrame, src_path: Path) -> Path:
    """
    Save to the same folder as the source with suffix '_mimic_III' before the extension.
    Example: ICUSTAYS.csv -> ICUSTAYS_mimic_III.csv
    """
    out_path = src_path.with_name(f"{src_path.stem}_mimic_III{src_path.suffix}")
    df.to_csv(out_path, index=False)
    return out_path


def main() -> None:
    icu = load_icustays(INPUT)
    out_path = save_with_suffix(icu, INPUT)
    print(f"Shape: {icu.shape}")
    print(f"Saved: {out_path}")


if __name__ == "__main__":
    main()
