# LIHEAP Data Engineering Project (Python / Pandas)

## Project Overview
This project builds an end-to-end **Python-based data pipeline** to process and analyze  
**LIHEAP (Low Income Home Energy Assistance Program)** data and enrich it with  
**ACS demographic data** and **BLS unemployment statistics**.

The focus of this project is **data engineering using Python and Pandas**, including:
- data ingestion
- transformation
- aggregation
- enrichment
- validation

No SQL or BI tools are used in this project.

---

## Project Status
🚧 **Work in Progress**

The pipeline is under active development and continuously improved as new data
and validation requirements are added.

---

## Data Sources
- **LIHEAP**: Monthly assistance records
- **ACS (American Community Survey)**: Demographic and socioeconomic indicators
- **BLS LAUS**: County-level unemployment statistics

> Raw, clean, and intermediate datasets are intentionally excluded from version control.  
> All results are fully reproducible by running the Python pipeline.

---

## Project Structure
```text
liheap-data-engineering/
│
├── notebooks/
│   ├── pipeline/          # Ordered ETL scripts (01 → 06)
│   ├── tests/             # Data quality & reproducibility checks
│   └── utils/             # Helper utilities
│
├── outputs/               # Generated outputs (ignored by Git)
├── data/                  # Raw / clean / intermediate data (ignored)
├── .gitignore
└── README.md
