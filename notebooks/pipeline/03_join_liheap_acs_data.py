"""
Script: 03_join_liheap_acs_data.py

Purpose:
    Join LIHEAP annual ZIP-level metrics with ACS (2023) ZIP-level indicators
    (median household income and population) for socioeconomic analysis.

Inputs:
    1) dataliheap_zip_year_sanity_check.xlsx
       Columns: Zip_Code, Year, total_pledge, record_count

    2) acs_median_income_ca_zcta.xlsx (sheet: "Data Clean")
       Columns: ZIPCODE, Median household income in the past 12 months (in 2023 inflation-adjusted dollars)

    3) acs_population_ca_zcta.xlsx (sheet: "Clean data")
       Columns: ZIPCODE, Population

Output:
    liheap_acs_combined.xlsx
    Columns: Zip_Code, Year, total_pledge, record_count, Median_Income, Population

Business logic:
    - LEFT JOIN from LIHEAP to ACS using Zip_Code only (not Year).
    - ACS 2023 is applied to all LIHEAP years (2023–2025) because ACS is cross-sectional
      and usually changes slowly over short periods.

Data quality notes:
    - Missing ACS values are expected for some ZIPs.
    - ACS uses ZCTA geography, which may not match USPS ZIPs perfectly.
"""

import pandas as pd
from pathlib import Path


# =============================================================================
# CONFIGURATION
# =============================================================================

# Project root: 2 levels up from this script file
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Input files
LIHEAP_FILE = PROJECT_ROOT / "data" / "clean" / "dataliheap_zip_year_sanity_check.xlsx"
INCOME_FILE = PROJECT_ROOT / "data" / "raw" / "Acs_ca_zcta" / "acs_median_income_ca_zcta.xlsx"
POP_FILE = PROJECT_ROOT / "data" / "raw" / "Acs_ca_zcta" / "acs_population_ca_zcta.xlsx"

# Output file
OUTPUT_FILE = PROJECT_ROOT / "data" / "raw" / "Acs_ca_zcta" / "liheap_acs_combined.xlsx"


def section(title: str) -> None:
    """Print a clean section header."""
    line = "=" * 70
    print(f"\n{line}\n{title}\n{line}")


def print_loaded(label: str, path: Path, df: pd.DataFrame, sheet: str | None = None) -> None:
    """Print a standard summary after reading a dataset."""
    print(f"{label}")
    print(f"  Source: {path.name}" + (f" | Sheet: {sheet}" if sheet else ""))
    print(f"  Shape : {df.shape[0]:,} rows x {df.shape[1]} cols")
    print(f"  Columns: {df.columns.tolist()}")


# =============================================================================
# STEP 1: LOAD DATA
# =============================================================================

section("STEP 1: LOAD SOURCE DATA")

df_liheap = pd.read_excel(LIHEAP_FILE)
print_loaded("LIHEAP (ZIP-Year metrics) loaded.", LIHEAP_FILE, df_liheap)

df_income = pd.read_excel(INCOME_FILE, sheet_name="Data Clean")
print_loaded("ACS median income loaded.", INCOME_FILE, df_income, sheet="Data Clean")

df_population = pd.read_excel(POP_FILE, sheet_name="Clean data")
print_loaded("ACS population loaded.", POP_FILE, df_population, sheet="Clean data")


# =============================================================================
# STEP 2: STANDARDIZE COLUMNS AND TYPES
# =============================================================================

section("STEP 2: STANDARDIZE COLUMNS AND TYPES")

# Rename columns to a consistent naming style
df_income = df_income.rename(
    columns={
        "ZIPCODE": "Zip_Code",
        "Median household income in the past 12 months (in 2023 inflation-adjusted dollars)": "Median_Income",
    }
)

df_population = df_population.rename(columns={"ZIPCODE": "Zip_Code"})

print("Renamed ACS columns for consistency:")
print(f"  Income columns     : {df_income.columns.tolist()}")
print(f"  Population columns : {df_population.columns.tolist()}")

# Standardize ZIP codes as 5-digit strings in all datasets (keeps leading zeros)
for df, name in [(df_liheap, "LIHEAP"), (df_income, "ACS income"), (df_population, "ACS population")]:
    df["Zip_Code"] = df["Zip_Code"].astype(str).str.strip().str.zfill(5)

print("Standardized Zip_Code format to 5-digit strings.")
print(f"  Sample Zip_Code (LIHEAP): {df_liheap['Zip_Code'].head(5).tolist()}")

# Ensure Year is an integer (LIHEAP only)
df_liheap["Year"] = df_liheap["Year"].astype(int)
print("Standardized Year type to int (LIHEAP).")


# =============================================================================
# STEP 3: JOIN DATASETS
# =============================================================================

section("STEP 3: JOIN LIHEAP WITH ACS (LEFT JOIN ON Zip_Code)")

# Join 1: add median income
df_combined = df_liheap.merge(df_income, on="Zip_Code", how="left")
print("Joined LIHEAP + ACS median income (LEFT JOIN on Zip_Code).")
print(f"  Result shape: {df_combined.shape[0]:,} rows x {df_combined.shape[1]} cols")

# Join 2: add population
df_combined = df_combined.merge(df_population, on="Zip_Code", how="left")
print("Joined result + ACS population (LEFT JOIN on Zip_Code).")
print(f"  Result shape: {df_combined.shape[0]:,} rows x {df_combined.shape[1]} cols")


# =============================================================================
# STEP 4: DATA QUALITY CHECKS
# =============================================================================

section("STEP 4: DATA QUALITY CHECKS")

print("Final dataset columns:")
for i, col in enumerate(df_combined.columns, start=1):
    print(f"  {i:>2}. {col}")

# Missing value report (only show columns that have missing values)
missing_counts = df_combined.isna().sum()
missing_counts = missing_counts[missing_counts > 0].sort_values(ascending=False)

if len(missing_counts) == 0:
    print("\nMissing values: none found.")
else:
    print("\nMissing values (expected for some ZIPs in ACS/ZCTA):")
    total_rows = len(df_combined)
    for col, cnt in missing_counts.items():
        pct = cnt / total_rows * 100
        print(f"  {col}: {cnt:,} rows ({pct:.1f}%)")

# Small sample preview
print("\nSample rows (first 10):")
print(df_combined.head(10).to_string(index=False))

# Coverage summary
unique_zips = df_combined["Zip_Code"].nunique()
years = sorted(df_combined["Year"].unique().tolist())

income_cov = df_combined["Median_Income"].notna().sum()
pop_cov = df_combined["Population"].notna().sum()
total = len(df_combined)

print("\nCoverage summary:")
print(f"  Unique ZIP codes: {unique_zips:,}")
print(f"  Years covered   : {years}")
print(f"  Median_Income available: {income_cov:,} / {total:,} ({income_cov/total*100:.1f}%)")
print(f"  Population available   : {pop_cov:,} / {total:,} ({pop_cov/total*100:.1f}%)")


# =============================================================================
# STEP 5: SAVE OUTPUT
# =============================================================================

section("STEP 5: SAVE OUTPUT")

OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
df_combined.to_excel(OUTPUT_FILE, index=False, sheet_name="Combined_Data")

print("Saved output Excel file.")
print(f"  Path: {OUTPUT_FILE}")
print(f"  Size: {OUTPUT_FILE.stat().st_size / 1024:.1f} KB")

section("DONE")
print("Next steps:")
print("  1) Review missing ZIP patterns (ACS coverage).")
print("  2) Continue with unemployment integration (scripts 04–06).")
print("  3) Start analysis: LIHEAP metrics vs income and population.")