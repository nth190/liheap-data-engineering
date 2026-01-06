import io
import zipfile
import urllib.request
import random
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


# =========================
#  GLOBAL CONFIG & CONSTANTS
# =========================

# Set random seed for reproducibility (if random sampling is added later)
np.random.seed(42)
random.seed(42)

# Robust script_dir definition (works in scripts and in notebooks)
try:
    SCRIPT_DIR = Path(__file__).resolve().parent.parent
except NameError:
    # __file__ is not defined (e.g., running in a notebook) → use current working dir
    SCRIPT_DIR = Path.cwd().parent

DATA_FOLDER = SCRIPT_DIR / "Data_raw_SDGE LIHEAP"
COMBINED_OUTPUT = SCRIPT_DIR / "data_clean" / "combined_raw_liheap_2023_2025.xlsx"
FINAL_OUTPUT = SCRIPT_DIR / "data_clean" / "liheap_clean_2023_2025.xlsx"

# Column mapping from raw names to standardized names
COLUMN_MAPPING = {
    # City
    "CV_EnergyAssistance[City(Service Address)]": "City",
    "Service City": "City",
    "City": "City",

    # Zip / Postal Code
    "CV_EnergyAssistance[Post Code (Service Address)]": "Zip_Code",
    "CV_EnergyAssistance[Zipcode (Business Partner Address)]": "Zip_Code",
    "Zipcode (Business Partner Address)": "Zip_Code",
    "Zip Code": "Zip_Code",
    "ZIP": "Zip_Code",
    "Zip_Code": "Zip_Code",

    # Date (Created On)
    "CV_EnergyAssistance[created On (Pledge Details)]": "Created_On",
    "CV_EnergyAssistance[Created On (Pledge Details)]": "Created_On",
    "CV_EnergyAssistance[Created on (MM/DD/YYYY)]": "Created_On",
    "CV_EnergyAssistance[Created On (PL)]": "Created_On",
    "Created_On": "Created_On",
    "Created On": "Created_On",

    # Pledge Amount
    "[Pledge_Amount]": "Pledge_Amount",
    "Pledge Amount": "Pledge_Amount",
    "Pledge_Amount": "Pledge_Amount",
    "CV_EnergyAssistance[Pledge Amount]": "Pledge_Amount",
}

REQUIRED_STRICT = ["Zip_Code", "Created_On", "Pledge_Amount"]
OPTIONAL_COLS = ["City"]


# =========================
#  UTILITY FUNCTIONS
# =========================

def detect_header_row(file_path: Path, max_rows: int = 10) -> int:
    """
    Try to detect which row is the header row in an Excel file.

    Strategy:
      - Read the first `max_rows` rows with no header.
      - For each row, count:
          * number of non-null cells
          * number of cells containing alphabetic characters
      - Return the index of the first row that looks like a header
        (at least 3 non-null cells and at least 2 cells with letters).
    """
    tmp = pd.read_excel(file_path, header=None, nrows=max_rows)

    for i in range(min(max_rows, len(tmp))):
        row = tmp.iloc[i].astype(str)
        has_alpha = row.str.contains(r"[A-Za-z]", regex=True).sum()
        non_null = row.notna().sum()
        if non_null >= 3 and has_alpha >= 2:
            return i
    return 0


def parse_date(value) -> pd.Timestamp:
    """
    Parse different date formats into a pandas Timestamp.

    Supported:
      - NaN → NaT
      - integers/floats like 20240131 → parsed with format '%Y%m%d'
      - strings → parsed via pandas to_datetime with errors='coerce'
    """
    if pd.isna(value):
        return pd.NaT

    if isinstance(value, (int, float)):
        try:
            return pd.to_datetime(str(int(value)), format="%Y%m%d")
        except Exception:
            return pd.NaT

    return pd.to_datetime(value, errors="coerce")


def clean_zip(zip_series: pd.Series) -> pd.Series:
    """
    Clean ZIP codes:
      - cast to string
      - remove trailing '.0'
      - extract 5 digits
      - left pad with zeros to 5 characters
    """
    return (
        zip_series.astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .str.extract(r"(\d{5})", expand=False)
        .str.zfill(5)
    )


def clean_pledge_amount(amount_series: pd.Series) -> pd.Series:
    """
    Clean pledge amount column by removing non-numeric characters and
    converting to float.
    """
    cleaned = (
        amount_series.astype(str)
        .str.replace(r"[^0-9.\-]", "", regex=True)
    )
    return pd.to_numeric(cleaned, errors="coerce")


def build_zip_city_from_internal(df: pd.DataFrame) -> Dict[str, str]:
    """
    Build a ZIP → CITY mapping using existing LIHEAP data.

    The rules:
      - Only use rows where City is present and non-null.
      - Normalize City to uppercase and strip spaces.
      - For each ZIP, choose the most frequent city value.
        We sort by index to get deterministic results when there is a tie.
    """
    city_series = df["City"].astype(str)
    missing_city_mask = (
        df["City"].isna()
        | city_series.str.strip().eq("")
        | city_series.str.strip().str.upper().eq("NAN")
    )

    valid_city_mask = ~missing_city_mask

    zip_city_map = (
        df.loc[valid_city_mask, ["Zip_Code", "City"]]
        .assign(City=lambda d: d["City"].astype(str).str.strip().str.upper())
        .dropna()
        .groupby("Zip_Code")["City"]
        .agg(lambda s: s.value_counts().sort_index().idxmax())
        .to_dict()
    )

    print(f"[INFO] Internal ZIP→CITY mapping size: {len(zip_city_map)}")
    return zip_city_map


def load_geonames_zip_city(
    script_dir: Path,
    timeout: int = 10,
    use_http: bool = True,
) -> Dict[str, str]:
    """
    Load ZIP → CITY mapping from GeoNames.

    Priority:
      1) HTTP download from https://download.geonames.org/export/zip/US.zip
      2) Local file fallback: {script_dir}/data_geonames/US.txt

    Returns:
      dict[postal_code -> PLACE_NAME in uppercase]
    """
    geonames_zip_city: Dict[str, str] = {}
    geonames_file = script_dir / "data_geonames" / "US.txt"

    if use_http:
        print("[INFO] Attempting to load GeoNames data from HTTP...")
        try:
            url = "https://download.geonames.org/export/zip/US.zip"
            print(f"[INFO] Downloading from {url}...")

            with urllib.request.urlopen(url, timeout=timeout) as response:
                zip_data = response.read()

            with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
                with zf.open("US.txt") as txt_file:
                    df_geo = pd.read_csv(
                        txt_file,
                        sep="\t",
                        header=None,
                        names=[
                            "country",
                            "postal_code",
                            "place_name",
                            "admin_name1",
                            "admin_code1",
                            "admin_name2",
                            "admin_code2",
                            "admin_name3",
                            "admin_code3",
                            "latitude",
                            "longitude",
                            "accuracy",
                        ],
                        dtype={"postal_code": str},
                        usecols=["postal_code", "place_name"],
                    )
            geonames_zip_city = (
                df_geo.dropna(subset=["postal_code", "place_name"])
                .groupby("postal_code")["place_name"]
                .first()
                .str.upper()
                .to_dict()
            )

            print(f"[INFO] Loaded {len(geonames_zip_city)} ZIP codes from HTTP")
            return geonames_zip_city

        except Exception as e:
            print(f"[WARN] HTTP GeoNames load failed: {e}")

    print("[INFO] Falling back to local GeoNames file...")
    if geonames_file.exists():
        try:
            df_geo = pd.read_csv(
                geonames_file,
                sep="\t",
                header=None,
                names=[
                    "country",
                    "postal_code",
                    "place_name",
                    "admin_name1",
                    "admin_code1",
                    "admin_name2",
                    "admin_code2",
                    "admin_name3",
                    "admin_code3",
                    "latitude",
                    "longitude",
                    "accuracy",
                ],
                dtype={"postal_code": str},
                usecols=["postal_code", "place_name"],
            )
            geonames_zip_city = (
                df_geo.dropna(subset=["postal_code", "place_name"])
                .groupby("postal_code")["place_name"]
                .first()
                .str.upper()
                .to_dict()
            )
            print(f"[INFO] Loaded {len(geonames_zip_city)} ZIP codes from local file")
        except Exception as e2:
            print(f"[ERROR] Failed to load local GeoNames data: {e2}")
    else:
        print(f"[WARN] Local GeoNames file not found at {geonames_file}")

    return geonames_zip_city


# =========================
#  MAIN PIPELINE CLASS
# =========================

class LiheapPipeline:
    """
    Main pipeline for:
      1) Reading and normalizing all LIHEAP Excel files.
      2) Cleaning Zip, dates, and pledge amounts.
      3) Filling missing City values from:
           - internal LIHEAP data
           - GeoNames ZIP database
      4) Filtering by date range (optional).
      5) Saving:
           - combined raw-normalized file
           - final cleaned dataset.
    """

    def __init__(
        self,
        data_folder: Path,
        combined_output: Path,
        final_output: Path,
        column_mapping: Dict[str, str],
        required_strict: List[str],
        optional_cols: List[str],
        script_dir: Path,
        start_year_month: Optional[str] = "2023-01",
        end_year_month: Optional[str] = "2025-06",
    ):
        self.data_folder = data_folder
        self.combined_output = combined_output
        self.final_output = final_output
        self.column_mapping = column_mapping
        self.required_strict = required_strict
        self.optional_cols = optional_cols
        self.script_dir = script_dir
        self.start_year_month = start_year_month
        self.end_year_month = end_year_month

        self.skipped_files: List[Tuple[str, List[str]]] = []

    # ---------- STEP 1: LOAD & NORMALIZE EACH FILE ----------

    def _normalize_single_file(self, file: Path) -> Optional[pd.DataFrame]:
        """
        Normalize a single Excel file:
          - Detect header row.
          - Standardize column names.
          - Require essential columns.
          - Add missing optional columns as empty.
          - Keep only the core columns + SourceFile.
        """
        print(f"\n[INFO] Processing file: {file.name}")

        header_row = detect_header_row(file)
        print(f"[INFO]   Detected header row: {header_row}")

        df = pd.read_excel(file, header=header_row)

        # Normalize column names: strip spaces and collapse multiple spaces
        df.columns = (
            df.columns.astype(str)
            .str.strip()
            .str.replace(r"\s+", " ", regex=True)
        )

        # Apply column mapping
        df = df.rename(columns=lambda c: self.column_mapping.get(c, c))
        print(f"[INFO]   Columns after rename: {df.columns.tolist()}")

        # Check required columns
        missing_required = [c for c in self.required_strict if c not in df.columns]
        if missing_required:
            print(f"[WARN]   Skipping file (missing required columns): {missing_required}")
            self.skipped_files.append((file.name, missing_required))
            return None

        # Ensure optional columns exist
        for col in self.optional_cols:
            if col not in df.columns:
                df[col] = pd.NA

        # Keep core columns + SourceFile for traceability
        keep_cols = ["City", "Zip_Code", "Created_On", "Pledge_Amount"]
        df = df[keep_cols].copy()
        df["SourceFile"] = file.name

        return df

    def load_and_normalize_all_files(self) -> pd.DataFrame:
        """
        Load all Excel files under data_folder, normalize them,
        and concatenate into a single DataFrame.
        """
        excel_files = sorted(self.data_folder.rglob("*.xls*"), key=lambda x: x.name)

        print(f"[INFO] Number of Excel files found: {len(excel_files)}")
        print("[INFO] Sample files:", [f.name for f in excel_files[:5]])

        if not excel_files:
            raise FileNotFoundError(
                f"No Excel files (.xls or .xlsx) found under: {self.data_folder}"
            )

        normalized_dfs: List[pd.DataFrame] = []

        for file in excel_files:
            df_normalized = self._normalize_single_file(file)
            if df_normalized is not None:
                normalized_dfs.append(df_normalized)

        if not normalized_dfs:
            raise RuntimeError(
                "No valid files after normalization. "
                "Check column mappings and required columns."
            )

        df_all = pd.concat(normalized_dfs, ignore_index=True)
        print(f"\n[INFO] Combined df_all shape: {df_all.shape}")
        print(df_all.head())

        # Save combined normalized data (before further cleaning)
        self.combined_output.parent.mkdir(parents=True, exist_ok=True)
        df_all.to_excel(self.combined_output, index=False)
        print(f"[INFO] Combined normalized file saved to: {self.combined_output}")

        return df_all

    # ---------- STEP 2: CLEAN COLUMNS ----------

    def clean_core_columns(self, df_all: pd.DataFrame) -> pd.DataFrame:
        """
        Clean Zip_Code, Created_On, Pledge_Amount, and create YearMo.
        Also drop rows with missing or invalid Zip_Code.
        """
        # Identify missing Zip_Code rows (raw)
        zip_raw = df_all["Zip_Code"]
        missing_zip_mask = (
            zip_raw.isna()
            | zip_raw.astype(str).str.strip().eq("")
            | zip_raw.astype(str).str.strip().str.upper().eq("NAN")
        )
        print(f"[INFO] Rows dropped because of missing Zip_Code: {missing_zip_mask.sum()}")

        df_all = df_all[~missing_zip_mask].copy()

        # Clean ZIP
        df_all["Zip_Code"] = clean_zip(df_all["Zip_Code"])

        # Clean Created_On → datetime
        df_all["Created_On"] = df_all["Created_On"].apply(parse_date)

        # Create YearMo (YYYY-MM string)
        df_all["YearMo"] = df_all["Created_On"].dt.to_period("M").astype(str)

        # Clean Pledge_Amount
        df_all["Pledge_Amount"] = clean_pledge_amount(df_all["Pledge_Amount"])

        return df_all
    
    # ---------- STEP 2.1: DEDUPLICATE ----------
    def deduplicate(self, df_all: pd.DataFrame) -> pd.DataFrame:
        """ Deduplicate LIHEAP records for analytics.

    Business rule:
      - Records with the same Zip_Code, YearMo, and Pledge_Amount
        are considered duplicates.
      - Keep the first occurrence to avoid double counting.
        """
        dedupe_cols = ["Zip_Code", "YearMo", "Pledge_Amount"]

        before = len(df_all)

        df_all = (
        df_all
        .sort_values(dedupe_cols)  # deterministic
        .drop_duplicates(subset=dedupe_cols, keep="first")
        .copy()
        )

        after = len(df_all)
        print(f"[INFO] Deduplication removed {before - after} duplicate rows.")

        return df_all

    # ---------- STEP 3: FILL CITY (INTERNAL + GEONAMES) ----------

    def fill_missing_cities(self, df_all: pd.DataFrame) -> pd.DataFrame:
        """
        Fill missing City values in two stages:
          1) Using internal LIHEAP data (Zip → City mapping).
          2) Using GeoNames Zip database (HTTP or local file).
        """
        # First: internal LIHEAP mapping
        city_series = df_all["City"].astype(str)
        missing_city_mask = (
            df_all["City"].isna()
            | city_series.str.strip().eq("")
            | city_series.str.strip().str.upper().eq("NAN")
        )

        print(f"[INFO] Rows with missing City BEFORE fill: {missing_city_mask.sum()}")

        # Internal mapping
        zip_city_map_internal = build_zip_city_from_internal(df_all)

        # Fill from internal mapping
        df_all.loc[missing_city_mask, "City"] = (
            df_all.loc[missing_city_mask, "Zip_Code"].map(zip_city_map_internal)
        )

        # Second: GeoNames mapping for still-missing rows
        city_series2 = df_all["City"].astype(str)
        still_missing_mask = (
            df_all["City"].isna()
            | city_series2.str.strip().eq("")
            | city_series2.str.strip().str.upper().eq("NAN")
        )

        print(
            f"[INFO] Rows with missing City AFTER internal mapping: "
            f"{still_missing_mask.sum()}"
        )

        # Load GeoNames data
        geonames_zip_city = load_geonames_zip_city(self.script_dir)

        def lookup_city(zip_code: str) -> Optional[str]:
            if pd.isna(zip_code):
                return None
            zip_clean = str(zip_code).zfill(5)
            return geonames_zip_city.get(zip_clean)

        if geonames_zip_city:
            df_all.loc[still_missing_mask, "City"] = (
                df_all.loc[still_missing_mask, "Zip_Code"].apply(lookup_city)
            )
        else:
            print("[WARN] No GeoNames data available - skipping GeoNames city lookup")

        final_missing = df_all["City"].isna().sum()
        print(f"[INFO] Rows with missing City AFTER GeoNames lookup: {final_missing}")

        # Normalize City to uppercase and strip spaces
        df_all["City"] = df_all["City"].astype(str).str.strip().str.upper()

        return df_all

    # ---------- STEP 4: FILTER BY DATE RANGE ----------

    def filter_by_date_range(self, df_all: pd.DataFrame) -> pd.DataFrame:
        """
        Filter rows by YearMo if start_year_month / end_year_month are provided.

        If either boundary is None, that side is left open.
        Example:
          - start="2023-01", end="2025-06": restrict to that range
          - start=None, end=None: no filter at all
        """
        if self.start_year_month is None and self.end_year_month is None:
            print("[INFO] No YearMo filter applied (both start and end are None).")
            return df_all

        mask = pd.Series(True, index=df_all.index)

        if self.start_year_month is not None:
            mask &= df_all["YearMo"] >= self.start_year_month
        if self.end_year_month is not None:
            mask &= df_all["YearMo"] <= self.end_year_month

        filtered = df_all[mask].copy()
        print(
            f"[INFO] After filtering YearMo "
            f"{self.start_year_month or '...'} to "
            f"{self.end_year_month or '...'}: {filtered.shape[0]} rows"
        )
        return filtered

    # ---------- STEP 5: SAVE FINAL DATASET ----------

    def save_final_dataset(self, df_all: pd.DataFrame) -> None:
        """
        Build the final dataset and save it to Excel.

        Final columns:
          - City
          - Zip_Code (string, stored as text in Excel)
          - YearMo
          - Pledge_Amount
        """
        df_final = df_all[["City", "Zip_Code", "YearMo", "Pledge_Amount"]].copy()
        df_final["Zip_Code"] = df_final["Zip_Code"].astype(str)

        print(f"\n[INFO] Final df_final shape: {df_final.shape}")
        print(df_final.head())

        self.final_output.parent.mkdir(parents=True, exist_ok=True)

        # Save to Excel and force Zip_Code column as text
        with pd.ExcelWriter(self.final_output, engine="openpyxl") as writer:
            df_final.to_excel(writer, index=False, sheet_name="LIHEAP_Data")
            worksheet = writer.sheets["LIHEAP_Data"]
            for row in range(2, len(df_final) + 2):
                cell = worksheet.cell(row=row, column=2)  # Zip_Code column
                cell.number_format = "@"

        print(f"\n[INFO] Final cleaned file saved to: {self.final_output}")

        if self.skipped_files:
            print("\n[INFO] Skipped files (missing required columns):")
            for fname, cols in self.skipped_files:
                print(f"  {fname}: {cols}")

    # ---------- MASTER RUN METHOD ----------

    def run(self) -> None:
        """
        Run the full pipeline end-to-end.
        """
        df_all = self.load_and_normalize_all_files()
        df_all = self.clean_core_columns(df_all)
        df_all = self.deduplicate(df_all)
        df_all = self.fill_missing_cities(df_all)
        df_all = self.filter_by_date_range(df_all)
        self.save_final_dataset(df_all)


# =========================
#  SCRIPT ENTRY POINT
# =========================

def main():
    pipeline = LiheapPipeline(
        data_folder=DATA_FOLDER,
        combined_output=COMBINED_OUTPUT,
        final_output=FINAL_OUTPUT,
        column_mapping=COLUMN_MAPPING,
        required_strict=REQUIRED_STRICT,
        optional_cols=OPTIONAL_COLS,
        script_dir=SCRIPT_DIR,
        # >>> IMPORTANT:
        # Set these to None if you do NOT want to filter by YearMo
        start_year_month="2023-01",
        end_year_month="2025-06",
    )

    pipeline.run()


if __name__ == "__main__":
    main()