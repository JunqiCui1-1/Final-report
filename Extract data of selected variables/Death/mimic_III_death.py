import pandas as pd
from pathlib import Path

# --- Config ---
INPUT_PATH = Path("/content/ADMISSIONS.csv")
OUTPUT_PATH = INPUT_PATH.with_name("mimic_III_death.csv")

def find_col(df: pd.DataFrame, target: str) -> str | None:
    """
    Return the actual column name in df that matches `target`
    in a case/space-insensitive way, else None.
    """
    t = target.strip().lower()
    for c in df.columns:
        if str(c).strip().lower() == t:
            return c
    return None

def main() -> None:
    # Load
    df = pd.read_csv(INPUT_PATH)

    # Resolve required columns (case-insensitive)
    col_subj  = find_col(df, "SUBJECT_ID")
    col_hadm  = find_col(df, "HADM_ID")
    col_dtime = find_col(df, "DEATHTIME")

    missing = [name for name, col in {
        "SUBJECT_ID": col_subj,
        "HADM_ID": col_hadm,
        "DEATHTIME": col_dtime
    }.items() if col is None]
    if missing:
        raise ValueError(
            f"Required columns not found (case-insensitive): {missing}. "
            f"Found columns: {list(df.columns)}"
        )

    # Build output: SUBJECT_ID, HADM_ID, Death (1 if DEATHTIME has a value, else 0)
    out = pd.DataFrame({
        "SUBJECT_ID": df[col_subj],
        "HADM_ID": df[col_hadm],
        "Death": (
            df[col_dtime]
            .astype(str)
            .str.strip()
            .ne("") & df[col_dtime].notna()
        ).astype(int)
    })

    # Save
    out.to_csv(OUTPUT_PATH, index=False)
    print(f"Saved: {OUTPUT_PATH} | shape={out.shape}")

if __name__ == "__main__":
    main()

