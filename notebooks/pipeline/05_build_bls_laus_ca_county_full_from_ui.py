#!/usr/bin/env python3
"""
Script: 05_build_bls_laus_ca_county_full_from_ui.py

Purpose:
    Convert county-level BLS unemployment rates into ZIP-level unemployment rates
    using the HUD ZIP-County crosswalk.

    A ZIP can belong to multiple counties. To keep a simple 1-to-1 mapping, this script
    assigns each ZIP to its "dominant county" (the county with the highest TOT_RATIO).
    Then it joins county unemployment rates onto each ZIP for each year.

Inputs:
    1) data/clean/bls_laus/bls_laus_unemp_rate_ca_county_annual.xlsx
       Columns (expected): Year, County, unemployment_rate, Series_ID (or County_FIPS)

    2) data/raw/data_raw/ZIP_COUNTY_122024.xlsx
       Columns (expected): ZIP, COUNTY, USPS_ZIP_PREF_STATE, TOT_RATIO, ...

Outputs:
    - data/clean/bls_laus/ca_zip_unemployment_annual_option1.xlsx
    - data/clean/bls_laus/ca_zip_unemployment_annual_option1.csv

Output columns:
    Zip_Code, Year, unemployment_rate, County, County_FIPS, zip_to_county_weight

Notes:
    - TOT_RATIO is used only to pick the dominant county (highest value per ZIP).
    - If a county is missing in the BLS file, ZIPs mapped to that county will have
      unemployment_rate = NaN.
"""

import pandas as pd
from pathlib import Path


# =============================================================================
# CONFIGURATION
# =============================================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

BLS_PATH = (
    PROJECT_ROOT
    / "data" / "clean" / "bls_laus"
    / "bls_laus_unemp_rate_ca_county_annual.xlsx"
)

XW_PATH = (
    PROJECT_ROOT
    / "data" / "raw"
    / "ZIP_COUNTY_122024.xlsx"
)

OUT_DIR = PROJECT_ROOT / "data" / "clean" / "bls_laus"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_XLSX = OUT_DIR / "ca_zip_unemployment_annual_option1.xlsx"
OUT_CSV = OUT_DIR / "ca_zip_unemployment_annual_option1.csv"


def section(title: str) -> None:
    """Print a clean section header."""
    line = "=" * 70
    print(f"\n{line}\n{title}\n{line}")


# =============================================================================
# STEP 1: LOAD INPUT DATA
# =============================================================================

section("STEP 1: LOAD INPUT FILES")

if not BLS_PATH.exists():
    raise FileNotFoundError(f"Input file not found: {BLS_PATH}")

if not XW_PATH.exists():
    raise FileNotFoundError(f"Input file not found: {XW_PATH}")

df_bls = pd.read_excel(BLS_PATH)
df_xw = pd.read_excel(XW_PATH)

print("Loaded BLS county unemployment file:")
print(f"  File   : {BLS_PATH.name}")
print(f"  Shape  : {df_bls.shape[0]:,} rows x {df_bls.shape[1]} cols")

print("Loaded HUD ZIP-County crosswalk file:")
print(f"  File   : {XW_PATH.name}")
print(f"  Shape  : {df_xw.shape[0]:,} rows x {df_xw.shape[1]} cols")


# =============================================================================
# STEP 2: BUILD COUNTY_FIPS IN BLS DATA
# =============================================================================

section("STEP 2: PREPARE BLS DATA (COUNTY_FIPS)")

# County_FIPS can be derived from Series_ID when County_FIPS is not provided.
# Series_ID example: LAUCN060010000000003
# State FIPS: positions [5:7] -> '06'
# County FIPS (3-digit): positions [7:10] -> '001'
# County_FIPS: '06' + '001' -> '06001'

if "County_FIPS" not in df_bls.columns:
    if "Series_ID" not in df_bls.columns:
        raise KeyError("BLS file must include either 'County_FIPS' or 'Series_ID'.")

    series = df_bls["Series_ID"].astype(str)
    df_bls["County_FIPS"] = (series.str[5:7] + series.str[7:10]).astype(str).str.zfill(5)

print("BLS County_FIPS prepared.")
print(f"  Counties: {df_bls['County_FIPS'].nunique():,}")
print(f"  Years   : {sorted(df_bls['Year'].dropna().unique().tolist())}")

# Keep only columns needed for the join and output
required_bls_cols = {"Year", "County_FIPS", "County", "unemployment_rate"}
missing_bls_cols = required_bls_cols - set(df_bls.columns)
if missing_bls_cols:
    raise KeyError(f"BLS file missing required columns: {sorted(missing_bls_cols)}")

df_bls_clean = df_bls[["Year", "County_FIPS", "County", "unemployment_rate"]].copy()


# =============================================================================
# STEP 3: FILTER AND STANDARDIZE CROSSWALK
# =============================================================================

section("STEP 3: PREPARE HUD CROSSWALK (CA ONLY, STANDARDIZE KEYS)")

required_xw_cols = {"ZIP", "COUNTY", "USPS_ZIP_PREF_STATE", "TOT_RATIO"}
missing_xw_cols = required_xw_cols - set(df_xw.columns)
if missing_xw_cols:
    raise KeyError(f"Crosswalk file missing required columns: {sorted(missing_xw_cols)}")

df_xw_ca = df_xw[df_xw["USPS_ZIP_PREF_STATE"].astype(str).str.upper() == "CA"].copy()

df_xw_ca["Zip_Code"] = df_xw_ca["ZIP"].astype(str).str.zfill(5)
df_xw_ca["County_FIPS"] = df_xw_ca["COUNTY"].astype(str).str.zfill(5)

print("Crosswalk filtered to California.")
print(f"  Rows (CA): {len(df_xw_ca):,}")
print(f"  Unique ZIPs (CA): {df_xw_ca['Zip_Code'].nunique():,}")
print(f"  Unique counties (CA): {df_xw_ca['County_FIPS'].nunique():,}")


# =============================================================================
# STEP 4: ASSIGN EACH ZIP TO A DOMINANT COUNTY
# =============================================================================

section("STEP 4: MAP EACH ZIP TO ONE DOMINANT COUNTY (MAX TOT_RATIO)")

# For each ZIP, pick the county with the largest TOT_RATIO.
# This avoids multiple counties per ZIP, which would create duplicate matches later.

df_zip_primary = (
    df_xw_ca.sort_values(["Zip_Code", "TOT_RATIO"], ascending=[True, False])
    .drop_duplicates(subset=["Zip_Code"], keep="first")
    [["Zip_Code", "County_FIPS", "TOT_RATIO"]]
    .rename(columns={"TOT_RATIO": "zip_to_county_weight"})
)

print("Dominant county mapping created.")
print(f"  ZIPs mapped: {df_zip_primary['Zip_Code'].nunique():,}")
print("  Note: Some ZIPs may exist in multiple counties, but only the top county is kept.")


# =============================================================================
# STEP 5: JOIN ZIP -> COUNTY WITH BLS ANNUAL UNEMPLOYMENT
# =============================================================================

section("STEP 5: JOIN ZIP MAPPING WITH BLS UNEMPLOYMENT DATA")

df_zip_unemp = df_zip_primary.merge(
    df_bls_clean,
    on="County_FIPS",
    how="left"
)

print("Join completed (LEFT JOIN on County_FIPS).")
print(f"  Output rows: {len(df_zip_unemp):,}")
print(f"  ZIPs: {df_zip_unemp['Zip_Code'].nunique():,}")
print(f"  Years present: {sorted(df_zip_unemp['Year'].dropna().unique().tolist())}")


# =============================================================================
# STEP 6: ORGANIZE OUTPUT COLUMNS
# =============================================================================

section("STEP 6: FINALIZE OUTPUT COLUMNS")

df_zip_unemp = df_zip_unemp[
    [
        "Zip_Code",
        "Year",
        "unemployment_rate",
        "County",
        "County_FIPS",
        "zip_to_county_weight",
    ]
].copy()

print("Final columns:")
print(f"  {df_zip_unemp.columns.tolist()}")


# =============================================================================
# STEP 7: SAVE OUTPUT FILES
# =============================================================================

section("STEP 7: SAVE OUTPUT FILES")

df_zip_unemp.to_excel(OUT_XLSX, index=False, sheet_name="zip_unemployment")
df_zip_unemp.to_csv(OUT_CSV, index=False)

print("Files saved.")
print(f"  Excel: {OUT_XLSX}")
print(f"  CSV  : {OUT_CSV}")


# =============================================================================
# STEP 8: SUMMARY AND DATA QUALITY
# =============================================================================

section("STEP 8: SUMMARY AND DATA QUALITY")

total_zips = df_zip_unemp["Zip_Code"].nunique()
total_rows = len(df_zip_unemp)
years = sorted(df_zip_unemp["Year"].dropna().unique().tolist())
counties = df_zip_unemp["County"].nunique()

missing_rate = df_zip_unemp["unemployment_rate"].isna().sum()
available_rate = total_rows - missing_rate

print("Summary:")
print(f"  Total ZIP codes: {total_zips:,}")
print(f"  Total ZIP-Year rows: {total_rows:,}")
print(f"  Years covered: {years}")
print(f"  Counties (from BLS join): {counties:,}")

print("Data quality:")
print(f"  Rows with unemployment_rate: {available_rate:,} ({available_rate/total_rows*100:.1f}%)")
print(f"  Rows missing unemployment_rate: {missing_rate:,} ({missing_rate/total_rows*100:.1f}%)")
print("  Missing values usually mean the county did not exist in the BLS export file.")