# filter_by_patientunitstayid.py
# -----------------------------------------------------------------------------
# Purpose:
#   Filter multiple (very large) CSV/CSV.GZ files by a baseline list of
#   patientunitstayid values. Keep the header (first row) + all matching rows.
#
# Features:
#   - Chunked reading for large files (memory friendly)
#   - Case-insensitive detection of the `patientunitstayid` column
#   - Writes output to a separate folder; creates it if missing
#   - Works with .csv and .csv.gz (compression='infer')
#
# Usage:
#   - Put this script in the same environment where your files live.
#   - Adjust the CONFIG section if needed.
#   - Run:  python filter_by_patientunitstayid.py
# -----------------------------------------------------------------------------

from pathlib import Path
import pandas as pd

# ============================== CONFIG =======================================
IDS_PATH = Path("/content/patientunitstayid_intersection.csv")  # baseline file
INPUT_FILES = [
    "/content/diagnosis.csv",
    "/content/lab.csv",
    "/content/patient.csv",
    "/content/vitalAperiodic.csv",
    "/content/vitalPeriodic.csv",
]
OUTPUT_DIR = Path("/content/filtered_by_patientunitstayid")
CHUNKSIZE = 1_000_000  # tune based on your memory
# =============================================================================


def find_id_col(cols):
    """Return the column name that equals 'patientunitstayid' (case-insensitive)."""
    for c in cols:
        if str(c).strip().lower() == "patientunitstayid":
            return c
    return None


def load_id_set(ids_path: Path) -> set:
    """Load baseline IDs as integers from CSV/CSV.GZ; accept single unnamed column."""
    if not ids_path.exists():
        raise FileNotFoundError(f"Baseline file not found: {ids_path}")

    df = pd.read_csv(ids_path, dtype="object", compression="infer")

    # map lower->original for case-insensitive access
    lower_map = {c.lower(): c for c in df.columns}
    if "patientunitstayid" in lower_map:
        col = lower_map["patientunitstayid"]
        ser = df[col]
    elif df.shape[1] == 1:
        ser = df.iloc[:, 0]
    else:
        raise ValueError(
            "Could not find 'patientunitstayid' in baseline file and it has >1 column."
        )

    ser = pd.to_numeric(ser, errors="coerce").dropna().astype("Int64")
    return set(int(x) for x in ser.unique())


def filter_one_file(src_path: str, id_set: set, out_dir: Path, chunksize: int):
    """Write header + matching rows to out_dir/src.name."""
    src = Path(src_path)
    if not src.exists():
        print(f"⚠️  Skip (file not found): {src}")
        return

    # Read only the header to (a) discover the ID column and (b) write header out
    try:
        header_df = pd.read_csv(src, nrows=0, compression="infer")
    except Exception as e:
        print(f"❌ Failed to read header: {src.name} -> {e}")
        return

    id_col = find_id_col(header_df.columns)
    if id_col is None:
        print(f"⚠️  Skip (no 'patientunitstayid' column): {src.name}")
        return

    out_path = out_dir / src.name
    header_df.to_csv(out_path, index=False)  # ensure header is always written
    wrote_any = False
    rows_kept = 0

    try:
        for chunk in pd.read_csv(src, chunksize=chunksize, compression="infer"):
            pid = pd.to_numeric(chunk[id_col], errors="coerce").astype("Int64")
            mask = pid.isin(id_set)
            if mask.any():
                hit = chunk.loc[mask]
                hit.to_csv(out_path, mode="a", header=False, index=False)
                wrote_any = True
                rows_kept += len(hit)
    except Exception as e:
        print(f"❌ Chunked filtering failed: {src.name} -> {e}")
        return

    if wrote_any:
        print(f"✅ Done: {src.name} -> {out_path.name} (matched rows: {rows_kept})")
    else:
        print(f"ℹ️  Done: {src.name} -> {out_path.name} (header only, no matches)")


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    id_set = load_id_set(IDS_PATH)
    print(f"✅ Baseline ID count: {len(id_set)}")

    for f in INPUT_FILES:
        filter_one_file(f, id_set, OUTPUT_DIR, CHUNKSIZE)

    print(f"\n🎯 All outputs written to: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
