import pandas as pd
from pathlib import Path

# --- Config ---
INPUT_PATH = Path("/content/PATIENTS.csv")

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

def build_output_path(input_path: Path, suffix: str = "_mimic_III") -> Path:
    """
    Append a suffix to the input filename (before extension).
    Example: PATIENTS.csv -> PATIENTS_mimic_III.csv
    """
    return input_path.with_name(input_path.stem + suffix + input_path.suffix)

def main():
    # Load
    df = pd.read_csv(INPUT_PATH)

    # Resolve required columns (case-insensitive)
    col_subject = find_col(df, "SUBJECT_ID")
    col_gender  = find_col(df, "GENDER")
    col_age     = find_col(df, "AGE")

    missing = [name for name, col in {
        "SUBJECT_ID": col_subject,
        "GENDER": col_gender,
        "AGE": col_age
    }.items() if col is None]

    if missing:
        raise ValueError(
            f"Required columns not found (case-insensitive): {missing}. "
            f"Found columns: {list(df.columns)}"
        )

    # Keep only the three columns and rename as requested
    out = df[[col_subject, col_gender, col_age]].copy()
    out = out.rename(columns={
        col_gender: "sex",
        col_age: "age"
        # Keep SUBJECT_ID as-is (exactly 'SUBJECT_ID')
    })
    # Ensure 'SUBJECT_ID' column label is exactly that
    if col_subject != "SUBJECT_ID":
        out = out.rename(columns={col_subject: "SUBJECT_ID"})

    # Optional: cast age to numeric
    out["age"] = pd.to_numeric(out["age"], errors="coerce")

    # Build output path and save (no death column requested)
    output_path = build_output_path(INPUT_PATH, "_mimic_III")
    out.to_csv(output_path, index=False)
    print(f"Saved: {output_path} | shape={out.shape}")

if __name__ == "__main__":
    main()
