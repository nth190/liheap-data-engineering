"""
Script: 02_aggregate_liheap_by_zip.py

Purpose:
    Aggregate cleaned LIHEAP data by ZIP code and Year to prepare for analysis.
    This script transforms individual LIHEAP pledge records into summarized metrics
    that can be joined with demographic and economic data.

Input:
    - liheap_clean_2023_2025.xlsx: Cleaned LIHEAP data from script 01
      Columns: City, Zip_Code, YearMo, Pledge_Amount

Output:
    - dataliheap_zip_year_sanity_check.xlsx: Aggregated data
      Columns: Zip_Code, Year, total_pledge, record_count

Business Logic:
    - Group LIHEAP pledges by ZIP code and calendar year
    - Calculate total pledge amount and count of records for each ZIP-Year combination
    - This aggregation enables time-series analysis and geographic comparisons

Author: [Your Name]
Date: December 2025
"""

import pandas as pd
from pathlib import Path

# =============================================================================
# CONFIGURATION
# =============================================================================

# Define project root directory (2 levels up from this script)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Input file: cleaned LIHEAP data from previous step
INPUT_FILE = PROJECT_ROOT / "data" / "clean" / "liheap_clean_2023_2025.xlsx"

# Output file: aggregated data for further analysis
OUTPUT_FILE = PROJECT_ROOT / "data" / "clean" / "dataliheap_zip_year_sanity_check.xlsx"

# Data quality check: valid year range for LIHEAP data
VALID_YEAR_MIN = 2023
VALID_YEAR_MAX = 2025

# =============================================================================
# STEP 1: LOAD DATA
# =============================================================================

print("="*70)
print("STEP 1: LOADING CLEANED LIHEAP DATA")
print("="*70)

df = pd.read_excel(INPUT_FILE, sheet_name="LIHEAP_Data")

print(f"✓ Loaded {len(df):,} records from {INPUT_FILE.name}")
print(f"  Columns: {df.columns.tolist()}")
print(f"\nSample data (first 5 rows):")
print(df.head())

# =============================================================================
# STEP 2: NORMALIZE ZIP CODE FORMAT
# =============================================================================

print("\n" + "="*70)
print("STEP 2: NORMALIZING ZIP CODE FORMAT")
print("="*70)

# Convert ZIP codes to string type to preserve leading zeros
df["Zip_Code"] = df["Zip_Code"].astype(str)

# Remove any trailing '.0' from ZIP codes (artifact from Excel numeric storage)
df["Zip_Code"] = df["Zip_Code"].str.replace(r"\.0$", "", regex=True)

# Ensure all ZIP codes are exactly 5 digits with leading zeros if needed
df["Zip_Code"] = df["Zip_Code"].str.zfill(5)

print(f"✓ ZIP codes normalized to 5-digit format")
print(f"  Sample ZIP codes: {df['Zip_Code'].head(10).tolist()}")

# =============================================================================
# STEP 3: PARSE DATE AND EXTRACT YEAR
# =============================================================================

print("\n" + "="*70)
print("STEP 3: PARSING DATES AND EXTRACTING YEAR")
print("="*70)

# Parse YearMo (format: 'YYYY-MM') into datetime for proper date handling
df["YearMo_dt"] = pd.to_datetime(df["YearMo"], format="%Y-%m")

# Extract calendar year for aggregation
df["Year"] = df["YearMo_dt"].dt.year

print(f"✓ Parsed {len(df):,} date values")
print(f"  Year range: {df['Year'].min()} to {df['Year'].max()}")

# =============================================================================
# STEP 4: DATA QUALITY CHECK - FILTER BY VALID YEAR RANGE
# =============================================================================

print("\n" + "="*70)
print("STEP 4: QUALITY CHECK - FILTERING BY VALID YEAR RANGE")
print("="*70)

initial_count = len(df)

# Filter to keep only records within expected year range (sanity check)
df = df[df["Year"].between(VALID_YEAR_MIN, VALID_YEAR_MAX)]

filtered_count = len(df)
removed_count = initial_count - filtered_count

print(f"✓ Year filter applied: {VALID_YEAR_MIN} to {VALID_YEAR_MAX}")
print(f"  Records before filter: {initial_count:,}")
print(f"  Records after filter:  {filtered_count:,}")
print(f"  Records removed:       {removed_count:,}")

if removed_count > 0:
    print(f"  ⚠️  Warning: {removed_count} records were outside valid year range")

# =============================================================================
# STEP 5: AGGREGATE BY ZIP CODE AND YEAR
# =============================================================================

print("\n" + "="*70)
print("STEP 5: AGGREGATING DATA BY ZIP CODE AND YEAR")
print("="*70)

# Group by ZIP and Year, then calculate:
#   - total_pledge: sum of all pledge amounts for that ZIP-Year
#   - record_count: number of LIHEAP pledges for that ZIP-Year
liheap_zip_year = (
    df
    .groupby(["Zip_Code", "Year"], as_index=False)
    .agg(
        total_pledge=("Pledge_Amount", "sum"),
        record_count=("Pledge_Amount", "count")
    )
)

print(f"✓ Aggregation complete")
print(f"  Unique ZIP codes: {liheap_zip_year['Zip_Code'].nunique():,}")
print(f"  Unique years: {sorted(liheap_zip_year['Year'].unique())}")
print(f"  Total ZIP-Year combinations: {len(liheap_zip_year):,}")

# =============================================================================
# STEP 6: SORT FOR READABILITY
# =============================================================================

print("\n" + "="*70)
print("STEP 6: SORTING RESULTS")
print("="*70)

# Sort by ZIP code first, then by Year for easy reading and debugging
liheap_zip_year = liheap_zip_year.sort_values(["Zip_Code", "Year"])

print(f"✓ Data sorted by Zip_Code, Year")

# =============================================================================
# STEP 7: PREVIEW RESULTS
# =============================================================================

print("\n" + "="*70)
print("STEP 7: PREVIEW OF AGGREGATED DATA")
print("="*70)

print("\nFirst 20 rows:")
print(liheap_zip_year.head(20).to_string(index=False))

# Summary statistics
print("\n" + "="*70)
print("SUMMARY STATISTICS")
print("="*70)
print(f"\nTotal pledge amount (all ZIPs, all years): ${liheap_zip_year['total_pledge'].sum():,.2f}")
print(f"Average pledge per ZIP-Year: ${liheap_zip_year['total_pledge'].mean():,.2f}")
print(f"Average records per ZIP-Year: {liheap_zip_year['record_count'].mean():.1f}")

# =============================================================================
# STEP 8: SAVE OUTPUT FILE
# =============================================================================

print("\n" + "="*70)
print("STEP 8: SAVING OUTPUT FILE")
print("="*70)

# Ensure output directory exists
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

# Save to Excel file for further analysis or joining with other datasets
liheap_zip_year.to_excel(OUTPUT_FILE, index=False)

print(f"✓ Output saved to: {OUTPUT_FILE}")
print(f"  File size: {OUTPUT_FILE.stat().st_size / 1024:.1f} KB")

print("\n" + "="*70)
print("AGGREGATION COMPLETE ✓")
print("="*70)
