"""
Test script to verify reproducibility across different runs
Run this script multiple times and compare checksums
"""
import pandas as pd
from pathlib import Path
import hashlib

script_dir = Path(__file__).parent.parent
output_file = script_dir / "data_clean" / "liheap_clean_2023_2025.xlsx"

# Read the output file
df = pd.read_excel(output_file)

# Create a deterministic string representation
data_str = df.to_csv(index=False)

# Calculate checksum
checksum = hashlib.md5(data_str.encode()).hexdigest()

print(f"✅ File loaded successfully")
print(f"📊 Shape: {df.shape}")
print(f"🔐 MD5 Checksum: {checksum}")
print(f"\n📈 Sample statistics:")
print(f"   Total Pledge Amount: ${df['Pledge_Amount'].sum():,.2f}")
print(f"   Average Pledge: ${df['Pledge_Amount'].mean():,.2f}")
print(f"   Unique Cities: {df['City'].nunique()}")
print(f"   Unique ZIP Codes: {df['Zip_Code'].nunique()}")
print(f"\n🎯 First 5 rows:")
print(df.head())
print(f"\n💡 Run this script multiple times - the checksum should ALWAYS be the same!")