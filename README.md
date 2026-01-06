# LIHEAP Data Engineering Project

## Project Overview
This project builds an end-to-end data pipeline to process and analyze **LIHEAP (Low Income Home Energy Assistance Program)** data and enrich it with **ACS demographic data** and **BLS unemployment statistics**.

The goal is to transform raw, heterogeneous public datasets into **clean, analytics-ready datasets** that can be used for exploratory analysis, reporting, and further modeling.

This repository focuses on **data engineering principles**: reproducibility, data quality, and clear data flow — not BI dashboards.

---

## Project Status
🚧 **Work in Progress**

The pipeline is under active development.  
Core ETL steps are implemented and continuously refined.

---

## Data Sources
- **LIHEAP**: Monthly assistance records
- **ACS (American Community Survey)**: Demographic and socioeconomic indicators
- **BLS LAUS**: County-level unemployment statistics

> Raw and processed datasets are intentionally excluded from version control.  
> All outputs can be reproduced by running the pipeline scripts.

---

## Project Structure
```text
liheap-data-engineering/
│
├── notebooks/
│   ├── pipeline/          # Ordered ETL scripts (01 → 06)
│   ├── tests/             # Data quality & reproducibility checks
│   └── utils/             # Helper scripts
│
├── sql/                   # SQL models (Silver / Gold layers)
├── outputs/               # Generated outputs (ignored in Git)
├── data/                  # Raw / clean / intermediate data (ignored)
├── .gitignore
└── README.md
