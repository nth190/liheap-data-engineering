#!/usr/bin/env python3
"""
Script: 06_join_liheap_acs_unemployment.py

Purpose:
    Join LIHEAP + ACS dataset with ZIP-level unemployment rates to create the final
    analysis dataset (one row per ZIP-Year with LIHEAP activity).

Inputs:
    1) data/raw/Acs_ca_zcta/liheap_acs_combined.xlsx
       Columns: Zip_Code, Year, total_pledge, record_count, Median_Income, Population

    2) data/clean/bls_laus/ca_zip_unemployment_annual_option1.xlsx
       Columns: Zip_Code, Year, unemployment_rate, County, County_FIPS, zip_to_county_weight

Output:
    - data/clean/liheap_full_combined.xlsx
      Sheet: LIHEAP_Full_Combined
      Columns: Zip_Code, Year, total_pledge, record_count, Median_Income, Population,
               unemployment_rate, County, County_FIPS

Business logic:
    - LEFT JOIN on (Zip_Code, Year)
      This keeps all LIHEAP rows and adds unemployment data when available.
"""

import pandas as pd
from pathlib import Path


# =============================================================================
# CONFIGURATION
# =============================================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

LIHEAP_ACS_FILE = (
    PROJECT_ROOT / "data" / "raw" / "Acs_ca_zcta" / "liheap_acs_combined.xlsx"
)

UNEMP_FILE = (
    PROJECT_ROOT / "data" / "clean" / "bls_laus" / "ca_zip_unemployment_annual_option1.xlsx"
)

OUTPUT_FILE = PROJECT_ROOT / "data" / "clean" / "liheap_full_combined.xlsx"


def section(title: str) -> None:
    """Print a clean section header."""
    line = "=" * 70
    print(f"\n{line}\n{title}\n{line}")


# =============================================================================
# STEP 1: LOAD INPUT DATA
# =============================================================================

section("STEP 1: LOAD INPUT DATA")

if not LIHEAP_ACS_FILE.exists():
    raise FileNotFoundError(f"Input file not found: {LIHEAP_ACS_FILE}")

if not UNEMP_FILE.exists():
    raise FileNotFoundError(f"Input file not found: {UNEMP_FILE}")

df_liheap_acs = pd.read_excel(LIHEAP_ACS_FILE)
df_unemp = pd.read_excel(UNEMP_FILE)

print("Loaded LIHEAP + ACS data:")
print(f"  File  : {LIHEAP_ACS_FILE.name}")
print(f"  Shape : {df_liheap_acs.shape[0]:,} rows x {df_liheap_acs.shape[1]} cols")
print(f"  Columns: {df_liheap_acs.columns.tolist()}")

print("Loaded unemployment (ZIP-level) data:")
print(f"  File  : {UNEMP_FILE.name}")
print(f"  Shape : {df_unemp.shape[0]:,} rows x {df_unemp.shape[1]} cols")
print(f"  Columns: {df_unemp.columns.tolist()}")


# =============================================================================
# STEP 2: STANDARDIZE JOIN KEYS AND SELECT COLUMNS
# =============================================================================

section("STEP 2: STANDARDIZE JOIN KEYS")

# Standardize Zip_Code to 5-digit strings (keeps leading zeros)
df_liheap_acs["Zip_Code"] = df_liheap_acs["Zip_Code"].astype(str).str.strip().str.zfill(5)
df_unemp["Zip_Code"] = df_unemp["Zip_Code"].astype(str).str.strip().str.zfill(5)

# Standardize Year as integers
df_liheap_acs["Year"] = pd.to_numeric(df_liheap_acs["Year"], errors="coerce").astype("Int64")
df_unemp["Year"] = pd.to_numeric(df_unemp["Year"], errors="coerce").astype("Int64")

# Drop unemployment rows without valid keys (defensive)
df_unemp = df_unemp.dropna(subset=["Zip_Code", "Year"]).copy()

# Keep only columns needed for the join and final output
unemp_cols = ["Zip_Code", "Year", "unemployment_rate", "County", "County_FIPS"]
missing_unemp_cols = set(unemp_cols) - set(df_unemp.columns)
if missing_unemp_cols:
    raise KeyError(f"Unemployment file missing required columns: {sorted(missing_unemp_cols)}")

df_unemp_clean = df_unemp[unemp_cols].copy()

print("Join key standardization completed.")
print(f"  LIHEAP+ACS ZIP sample: {df_liheap_acs['Zip_Code'].head(5).tolist()}")
print(f"  Unemployment ZIP sample: {df_unemp_clean['Zip_Code'].head(5).tolist()}")


# =============================================================================
# STEP 3: JOIN DATASETS
# =============================================================================

section("STEP 3: JOIN LIHEAP+ACS WITH UNEMPLOYMENT (LEFT JOIN)")

df_combined = df_liheap_acs.merge(
    df_unemp_clean,
    on=["Zip_Code", "Year"],
    how="left"
)

print("Join completed.")
print(f"  Result shape: {df_combined.shape[0]:,} rows x {df_combined.shape[1]} cols")


# =============================================================================
# STEP 4: FINAL COLUMN ORDER AND SORT
# =============================================================================

section("STEP 4: FINALIZE OUTPUT COLUMNS")

# Define a clear column order for analysis
column_order = [
    "Zip_Code",
    "Year",
    "total_pledge",
    "record_count",
    "Median_Income",
    "Population",
    "unemployment_rate",
    "County",
    "County_FIPS",
]

# Keep only columns that exist (defensive)
final_columns = [c for c in column_order if c in df_combined.columns]
df_final = df_combined[final_columns].copy()

# Sort for readability
df_final = df_final.sort_values(["Zip_Code", "Year"])

print("Final dataset columns:")
for i, col in enumerate(df_final.columns, start=1):
    print(f"  {i:>2}. {col}")

print(f"Final dataset shape: {df_final.shape[0]:,} rows x {df_final.shape[1]} cols")


# =============================================================================
# STEP 5: DATA QUALITY REPORT
# =============================================================================

section("STEP 5: DATA QUALITY REPORT")

total_rows = len(df_final)
unique_zips = df_final["Zip_Code"].nunique()
years = sorted(df_final["Year"].dropna().unique().tolist())

print("Coverage:")
print(f"  Rows: {total_rows:,}")
print(f"  Unique ZIP codes: {unique_zips:,}")
print(f"  Years: {years}")

# Missing values by key measures (report only if missing exists)
missing = df_final.isna().sum()
missing = missing[missing > 0].sort_values(ascending=False)

if len(missing) == 0:
    print("Missing values: none found.")
else:
    print("Missing values (columns with at least 1 missing):")
    for col, cnt in missing.items():
        print(f"  {col}: {cnt:,} ({cnt/total_rows*100:.1f}%)")

# Completeness for main indicators
need_cols = ["Median_Income", "Population", "unemployment_rate"]
for col in need_cols:
    if col in df_final.columns:
        available = df_final[col].notna().sum()
        print(f"{col} available: {available:,} / {total_rows:,} ({available/total_rows*100:.1f}%)")

# ZIP overlap diagnostics (LIHEAP ZIPs vs unemployment ZIPs with non-null rate)
liheap_zips = set(df_liheap_acs["Zip_Code"].dropna().unique().tolist())
unemp_zips_with_data = set(
    df_unemp_clean[df_unemp_clean["unemployment_rate"].notna()]["Zip_Code"]
    .dropna()
    .unique()
    .tolist()
)
overlap_count = len(liheap_zips & unemp_zips_with_data)

print("ZIP overlap check:")
print(f"  LIHEAP ZIPs: {len(liheap_zips):,}")
print(f"  Unemployment ZIPs with data: {len(unemp_zips_with_data):,}")
print(f"  Overlap: {overlap_count:,}")

if overlap_count == 0:
    print("Warning: No ZIP overlap between LIHEAP and unemployment data.")
    print("  This usually means the BLS export is missing the counties that cover LIHEAP ZIPs.")
    print("  Example: If LIHEAP ZIPs are mostly in San Diego County (06073),")
    print("  the BLS file must include San Diego County unemployment series.")


# =============================================================================
# STEP 6: SAVE OUTPUT
# =============================================================================

section("STEP 6: SAVE OUTPUT FILE")

OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

df_final.to_excel(OUTPUT_FILE, index=False, sheet_name="LIHEAP_Full_Combined")

print("Output saved.")
print(f"  Path: {OUTPUT_FILE}")
print(f"  Sheet: LIHEAP_Full_Combined")
print(f"  Rows: {len(df_final):,}")
print(f"  Columns: {len(df_final.columns):,}")

section("DONE")
print("Next steps:")
print("  1) If unemployment coverage is low, update the BLS export and rerun scripts 04–06.")
print("  2) Start EDA on liheap_full_combined.xlsx (correlations, trends, maps).")