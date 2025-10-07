#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Extract ICD code lists for:
  1) CABG procedures (ICD-10-PCS 021***** and ICD-9-CM procedures 36.1x)
  2) CAD diagnoses (ICD-10-CM I25.* and ICD-9-CM 414.*)
  3) CABG status/history (ICD-10-CM Z95.1, ICD-9-CM V45.81)

The script reads two dictionary tables:
  - d_icd_diagnoses.csv
  - d_icd_procedures.csv

And writes three two-column CSVs:
  - cabg_icd.csv (ICD_CODE, DESCRIPTION)
  - cad_icd.csv  (ICD_CODE, DESCRIPTION)
  - cabg_status_icd.csv (ICD_CODE, DESCRIPTION)

Usage:
  python extract_cabg_cad_codes.py \
      --diag d_icd_diagnoses.csv \
      --proc d_icd_procedures.csv \
      --outdir .
"""

from __future__ import annotations

import argparse
import logging
import re
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd


# ------------------------------ I/O -------------------------------------------

def read_icd_table(path: Path) -> pd.DataFrame:
    """
    Read an ICD dictionary table and normalize to columns: icd_code, description.
    Supports common header variants across datasets.

    Required columns (any one of):
      code:      ["icd_code", "code", "icd", "icd9_code", "icd10_code"]
      desc/text: ["long_title", "longtitle", "title", "description", "desc",
                  "short_title", "shorttitle"]
    """
    df = pd.read_csv(path, dtype=str, encoding="utf-8-sig", keep_default_na=False)
    df.columns = [c.strip().lower() for c in df.columns]

    code_cols = ["icd_code", "code", "icd", "icd9_code", "icd10_code"]
    text_cols = ["long_title", "longtitle", "title", "description", "desc", "short_title", "shorttitle"]

    code_col = next((c for c in code_cols if c in df.columns), None)
    text_col = next((c for c in text_cols if c in df.columns), None)

    if code_col is None or text_col is None:
        raise ValueError(
            f"Cannot locate code/description columns in {path}. "
            f"Found columns: {list(df.columns)}"
        )

    out = df[[code_col, text_col]].copy()
    out.columns = ["icd_code", "description"]
    out["icd_code"] = out["icd_code"].astype(str).str.strip().str.upper()
    out["description"] = out["description"].astype(str).str.strip()
    return out


# --------------------------- Normalization ------------------------------------

def remove_dots_spaces(s: Optional[str]) -> str:
    """Normalize code by removing dots and spaces, uppercase."""
    return re.sub(r"[.\s]", "", (s or "")).upper()


# ----------------------------- Rules ------------------------------------------

def is_icd10pcs_cabg(code: str) -> bool:
    """
    CABG in ICD-10-PCS: exactly 7 characters, alphanumeric, starting with '021'.
    """
    c = (code or "").strip().upper()
    return len(c) == 7 and c.startswith("021") and c.isalnum()


def is_icd9proc_cabg(code_nodot: str) -> bool:
    """
    CABG in ICD-9-CM procedures: 36.1x (stored without dot as 361*).
    """
    return code_nodot.startswith("361")


def is_excluded_pcs_prefix(code: str) -> bool:
    """
    Explicitly exclude non-procedure imaging/fluoroscopy PCS prefixes (e.g., B21****).
    Extend this list if your source dictionary contains other non-procedure sections.
    """
    c = (code or "").strip().upper()
    return c.startswith("B21")


def is_cad_diag(code: str, code_nodot: str) -> bool:
    """
    CAD in diagnoses:
      - ICD-10-CM: I25.*
      - ICD-9-CM : 414.*
    """
    return code.upper().startswith("I25") or code_nodot.startswith("414")


# --------------------------- Extraction ---------------------------------------

def extract_cabg(proc_df: pd.DataFrame) -> pd.DataFrame:
    """Return two-column CABG list from procedures dictionary."""
    tmp = proc_df.copy()
    tmp["_code_nodot"] = tmp["icd_code"].map(remove_dots_spaces)

    mask = (
        tmp["icd_code"].map(is_icd10pcs_cabg)
        | tmp["_code_nodot"].map(is_icd9proc_cabg)
    ) & ~tmp["icd_code"].map(is_excluded_pcs_prefix)

    out = (
        tmp.loc[mask, ["icd_code", "description"]]
            .drop_duplicates()
            .sort_values("icd_code")
            .rename(columns={"icd_code": "ICD_CODE", "description": "DESCRIPTION"})
    )
    return out


def extract_cad(diag_df: pd.DataFrame) -> pd.DataFrame:
    """Return two-column CAD list from diagnoses dictionary."""
    tmp = diag_df.copy()
    tmp["_code_nodot"] = tmp["icd_code"].map(remove_dots_spaces)

    mask = tmp.apply(lambda r: is_cad_diag(r["icd_code"], r["_code_nodot"]), axis=1)
    out = (
        tmp.loc[mask, ["icd_code", "description"]]
            .drop_duplicates()
            .sort_values("icd_code")
            .rename(columns={"icd_code": "ICD_CODE", "description": "DESCRIPTION"})
    )
    return out


def extract_cabg_status(diag_df: pd.DataFrame) -> pd.DataFrame:
    """Return CABG status/history codes (separate from procedures)."""
    cabg_status_set = {"Z95.1", "V45.81"}
    mask = diag_df["icd_code"].isin(cabg_status_set)
    out = (
        diag_df.loc[mask, ["icd_code", "description"]]
            .drop_duplicates()
            .sort_values("icd_code")
            .rename(columns={"icd_code": "ICD_CODE", "description": "DESCRIPTION"})
    )
    return out


# ----------------------------- CLI / Main -------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Extract CABG/CAD ICD code lists from diagnosis/procedure dictionaries."
    )
    p.add_argument("--diag", type=Path, default=Path("d_icd_diagnoses.csv"),
                   help="Path to diagnoses dictionary CSV (default: d_icd_diagnoses.csv).")
    p.add_argument("--proc", type=Path, default=Path("d_icd_procedures.csv"),
                   help="Path to procedures dictionary CSV (default: d_icd_procedures.csv).")
    p.add_argument("--outdir", type=Path, default=Path("."),
                   help="Output directory (default: current directory).")
    p.add_argument("--loglevel", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                   help="Logging level (default: INFO).")
    return p.parse_args()


def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s"
    )


def main() -> None:
    args = parse_args()
    configure_logging(args.loglevel)

    logging.info("Reading dictionaries...")
    diag_df = read_icd_table(args.diag)
    proc_df = read_icd_table(args.proc)

    logging.info("Extracting CABG procedures...")
    cabg_out = extract_cabg(proc_df)
    logging.info("Extracting CAD diagnoses...")
    cad_out = extract_cad(diag_df)
    logging.info("Extracting CABG status/history...")
    cabg_status_out = extract_cabg_status(diag_df)

    args.outdir.mkdir(parents=True, exist_ok=True)
    cabg_path = args.outdir / "cabg_icd.csv"
    cad_path = args.outdir / "cad_icd.csv"
    cabg_status_path = args.outdir / "cabg_status_icd.csv"

    cabg_out.to_csv(cabg_path, index=False, encoding="utf-8")
    cad_out.to_csv(cad_path, index=False, encoding="utf-8")
    cabg_status_out.to_csv(cabg_status_path, index=False, encoding="utf-8")

    logging.info("Done.")
    logging.info("CABG codes: %d -> %s", len(cabg_out), cabg_path)
    logging.info("CAD  codes: %d -> %s", len(cad_out), cad_path)
    logging.info("CABG status codes: %d -> %s", len(cabg_status_out), cabg_status_path)


if __name__ == "__main__":
    main()
