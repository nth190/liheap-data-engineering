#!/usr/bin/env python3
"""
Script: 04_etl_bls_laus_profile_to_annual.py

Purpose:
    Convert BLS LAUS "Profile" Excel export (one sheet per county) into annual
    unemployment rates for California counties.

    The Profile format contains:
      - Metadata rows at the top (Series ID, Series Title, Area, etc.)
      - A monthly data table with columns like: Year, Period, Observation Value

    This script:
      1) Reads each county sheet
      2) Extracts metadata (Series ID, Series Title, County)
      3) Reads monthly unemployment rate (M01–M12 only)
      4) Aggregates to annual average (mean of monthly rates)
      5) Splits full years (12 months) vs partial years (<12 months)
      6) Writes one Excel output with two sheets

Inputs:
    - data/raw/Bls_laus/2026-01-01_download.xlsx

Outputs:
    - data/clean/bls_laus/bls_laus_unemp_rate_ca_county_annual.xlsx
      Sheet 1: Full_Years_2023_2024
      Sheet 2: YTD_2025

Notes:
    - We exclude period M13 because it is already an annual average in BLS exports.
      We compute our own annual average from monthly values to keep logic consistent.
    - Partial year results (<12 months) should be used carefully in comparisons.
"""

import pandas as pd
from pathlib import Path


# =============================================================================
# CONFIGURATION
# =============================================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

BLS_PROFILE_XLSX = (
    PROJECT_ROOT / "data" / "raw" / "Bls_laus" / "2026-01-01_download.xlsx"
)

OUT_DIR = PROJECT_ROOT / "data" / "clean" / "bls_laus"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_ANNUAL_COMBINED = OUT_DIR / "bls_laus_unemp_rate_ca_county_annual.xlsx"


def section(title: str) -> None:
    """Print a clean section header."""
    line = "=" * 70
    print(f"\n{line}\n{title}\n{line}")


def _find_table_header_row(raw: pd.DataFrame) -> int | None:
    """
    Find the row index where the monthly data table starts.

    We read the sheet with header=None, so the table header appears as normal rows.
    The table starts where column 0 == 'Year' and column 1 == 'Period'.

    Returns:
        int: row index of the table header
        None: if not found
    """
    if raw.shape[1] < 2:
        return None

    for i in range(len(raw)):
        a = str(raw.iloc[i, 0]).strip()
        b = str(raw.iloc[i, 1]).strip()
        if a == "Year" and b == "Period":
            return i

    return None


def _extract_meta(raw: pd.DataFrame) -> dict:
    """
    Extract metadata key-value pairs from the top of a Profile sheet.

    Metadata is usually stored as:
      - Column 0: key
      - Column 1: value

    We read the first two columns and build a dictionary.
    """
    meta_pairs = raw.iloc[:, :2].dropna(how="all").copy()
    meta_pairs.columns = ["key", "value"]

    meta_pairs["key"] = meta_pairs["key"].astype(str).str.strip()
    meta_pairs["value"] = meta_pairs["value"].astype(str).str.strip()

    return dict(zip(meta_pairs["key"], meta_pairs["value"]))


def _county_from_series_title(series_title: str) -> str:
    """
    Extract a county name from the 'Series Title' field.

    Expected format:
      'Unemployment Rate: Glenn County, CA (U)'

    Returns:
      'Glenn County'
    """
    if not isinstance(series_title, str):
        return ""

    s = series_title.strip()
    s = s.replace("Unemployment Rate: ", "")
    s = s.replace(" (U)", "")
    s = s.replace(", CA", "")
    return s.strip()


def main() -> None:
    """
    Run the ETL process:
      - Read each sheet
      - Parse metadata + monthly data
      - Aggregate to annual values
      - Split full years vs partial years
      - Save to Excel (2 sheets)
    """
    section("STEP 1: VALIDATE INPUT AND READ WORKBOOK")

    if not BLS_PROFILE_XLSX.exists():
        raise FileNotFoundError(f"Input file not found: {BLS_PROFILE_XLSX}")

    print(f"Input file: {BLS_PROFILE_XLSX.name}")
    xls = pd.ExcelFile(BLS_PROFILE_XLSX)
    print(f"Sheets found: {len(xls.sheet_names)}")

    annual_rows: list[pd.DataFrame] = []

    section("STEP 2: PROCESS SHEETS (METADATA + MONTHLY DATA)")

    processed_sheets = 0
    skipped_no_table = 0
    skipped_no_value_col = 0

    for sh in xls.sheet_names:
        raw = pd.read_excel(BLS_PROFILE_XLSX, sheet_name=sh, header=None)

        meta = _extract_meta(raw)
        series_title = meta.get("Series Title", "")
        series_id = meta.get("Series ID", "")
        area = meta.get("Area", "")

        county_name = _county_from_series_title(series_title)
        if not county_name:
            if isinstance(area, str) and "County" in area:
                county_name = area.replace(", CA", "").strip()

        header_idx = _find_table_header_row(raw)
        if header_idx is None:
            skipped_no_table += 1
            continue

        df = pd.read_excel(BLS_PROFILE_XLSX, sheet_name=sh, header=header_idx)

        if "Observation Value" not in df.columns:
            skipped_no_value_col += 1
            continue

        df = df.rename(columns={"Observation Value": "value"})
        df = df[["Year", "Period", "value"]].copy()

        df["Period"] = df["Period"].astype(str).str.strip()

        # Keep only monthly periods M01–M12 and exclude M13 (annual average provided by BLS)
        df = df[df["Period"].str.match(r"^M\d{2}$", na=False)].copy()
        df = df[df["Period"].isin([f"M{m:02d}" for m in range(1, 13)])].copy()

        df["Year"] = pd.to_numeric(df["Year"], errors="coerce").astype("Int64")
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.dropna(subset=["Year", "value"])

        ann = (
            df.groupby("Year", as_index=False)
            .agg(
                unemployment_rate=("value", "mean"),
                month_count=("Period", "nunique"),
            )
        )

        ann["County"] = county_name
        ann["Series_ID"] = series_id
        ann["Series_Title"] = series_title

        annual_rows.append(ann)
        processed_sheets += 1

    print("Sheet processing summary:")
    print(f"  Processed sheets: {processed_sheets}")
    print(f"  Skipped (no table header): {skipped_no_table}")
    print(f"  Skipped (missing 'Observation Value'): {skipped_no_value_col}")

    section("STEP 3: CONSOLIDATE ANNUAL RESULTS")

    if not annual_rows:
        raise RuntimeError("No usable county sheets were processed from the input file.")

    annual = pd.concat(annual_rows, ignore_index=True)
    print(f"Annual rows created: {len(annual):,}")
    print(f"Counties found: {annual['County'].nunique():,}")
    print(f"Years found: {sorted(annual['Year'].dropna().unique().tolist())}")

    section("STEP 4: SPLIT FULL YEARS VS PARTIAL YEARS")

    annual_full = annual[annual["month_count"] == 12].copy()
    annual_full = annual_full.drop(columns=["month_count"])
    annual_full = annual_full.sort_values(["County", "Year"])

    annual_ytd = annual[annual["month_count"] < 12].copy()
    annual_ytd["is_partial_year"] = True
    annual_ytd = annual_ytd.sort_values(["County", "Year"])

    print("Split summary:")
    print(f"  Full years rows (12 months): {len(annual_full):,}")
    print(f"  Partial years rows (<12 months): {len(annual_ytd):,}")

    section("STEP 5: SAVE OUTPUT EXCEL (TWO SHEETS)")

    with pd.ExcelWriter(OUT_ANNUAL_COMBINED, engine="openpyxl") as writer:
        annual_full.to_excel(writer, sheet_name="Full_Years_2023_2024", index=False)
        annual_ytd.to_excel(writer, sheet_name="YTD_2025", index=False)

    print("Output file saved:")
    print(f"  Path: {OUT_ANNUAL_COMBINED}")
    print(f"  Size: {OUT_ANNUAL_COMBINED.stat().st_size / 1024:.1f} KB")

    section("FINAL SUMMARY")

    if len(annual_full) > 0:
        full_years = sorted(annual_full["Year"].unique().tolist())
        print("Full years:")
        print(f"  Counties: {annual_full['County'].nunique():,}")
        print(f"  Years   : {full_years}")

    if len(annual_ytd) > 0:
        ytd_years = sorted(annual_ytd["Year"].unique().tolist())
        print("Partial years (YTD):")
        print(f"  Counties: {annual_ytd['County'].nunique():,}")
        print(f"  Years   : {ytd_years}")
        print("  Note: Use partial years carefully in comparisons.")


if __name__ == "__main__":
    main()