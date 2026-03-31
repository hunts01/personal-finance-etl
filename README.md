# Personal Finance E-V

## Project overview
Personal finance data flow from extract to visualization. This pipeline ingests transaction CSVs exported from bank(s) and credit card provider(s), cleans and categorizes them using Python and dbt, stores the results in a local <> database, and serves a Power BI dashboard with key personal finance metrics.

### Key skills demonstrated:
	•	End-to-end ELT pipeline design
	•	Data modeling with dbt (staging → intermediate → mart layers)
	•	SQL transformations and dbt tests
	•	Local analytical storage with DuckDB
	•	Business intelligence with Power BI and DAX
	•	CI/CD with GitHub Actions

## Architecture
Bank CSV + Card CSV

       │
       ▼
  extract/load_csv.py      ← standardise columns, validate, load to DuckDB
  
       │
       ▼
  dbt: models/staging/     ← rename, cast, deduplicate raw sources
  
       │
       ▼
  dbt: models/intermediate/ ← union sources, classify categories
  
       │
       ▼
  dbt: models/marts/        ← monthly spend, income, savings rate
  
       │
       ▼
  Power BI dashboard        ← KPI cards, bar charts, line charts

## Repo structure
personal-finance-etl/

README.md
requirements.txt

setup.ps1

run_pipeline.ps1

.env.example

.gitignore

data/
  - sample_bank.csv          # fake data only
  - sample_card.csv

extract/
  - load_csv.py              # reads CSVs, standardizes schema, loads to DuckDB
  - validate.py              # row count checks, null checks before load

transform/                   # dbt project root
  - dbt_project.yml
  - profiles.yml
  - models/
      - staging/
          - tg_bank_txns.sql
          - stg_card_txns.sql
      - intermediate/
          - int_all_transactions.sql
          - int_categorized.sql
      - marts/
          - mart_monthly_spend.sql
          - mart_income.sql
          - mart_savings_rate.sql

notebooks/
  - 01_eda.ipynb             # exploratory analysis of raw transactions

powerbi/
  - finance_dashboard.pbix

.github/
  - workflows/
      - dbt_test.yml         # runs dbt test on every pull request

## Getting started
### Prerequisites
	•	Python 3.10+
	•	Node.js (optional, for dbt docs)
	•	Power BI Desktop (Windows)

### Installation
git clone https://github.com/your-username/personal-finance-etl.git
cd personal-finance-etl

python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate

pip install -r requirements.txt

### Configuration
Copy .env.example to .env and fill in your paths:
cp .env.example .env

### Running the pipeline
#### 1. Load raw CSVs into DuckDB
python extract/load_csv.py

#### 2. Run dbt transformations
cd transform
dbt run

#### 3. Run dbt tests
dbt test

#### 4. (Optional) Generate and serve dbt docs
dbt docs generate
dbt docs serve
Then open powerbi/finance_dashboard.pbix in Power BI Desktop and refresh the data source.

## dbt model layers
### Layer | Models | Purpose
Staging | stg_bank_txns, stg_card_txns | Rename columns, cast types, remove duplicates

Intermediate | int_all_transactions, int_categorized | Union sources, assign spend categories

Marts | mart_monthly_spend, mart_income, mart_savings_rate | Aggregated, Power BI-ready tables

## Spend categories
Transactions are classified by keyword matching on the merchant description:

### Category | Example merchants
Groceries | Walmart, Kroger, Whole Foods

Dining | McDonald's, Chipotle, local restaurants

Transport | Uber, Lyft, Shell, BP

Subscriptions | Netflix, Spotify, Adobe

Utilities | Electric, water, internet providers

Other | Anything unmatched

# Power BI dashboard

The .pbix file contains four report pages:

•	Overview — total income, total spend, and savings rate KPI cards for the selected month

•	Spending breakdown — bar chart of spend by category, with month slicer

•	Income vs expenses — dual-line chart showing monthly trends over time

•	Transactions — drill-through table with date, merchant, amount, and category

# Data privacy
The data/ folder contains only synthetic sample data generated for testing. Real transaction files are excluded via .gitignore. When running the pipeline locally, place your exported CSVs in a directory outside the repo, or in a data/real/ folder (already gitignored).

# CI/CD
A GitHub Actions workflow (.github/workflows/dbt_test.yml) runs dbt test automatically on every pull request using the sample data. This ensures all model assertions and schema tests pass before merging.

## Tech stack

### Tool | Role

Python + Pandas | Extract and load CSVs

DuckDB | Local analytical database

dbt | SQL transformations and testing

Power BI | Dashboard and visualisation

GitHub Actions | CI — automated dbt tests on PRs

## Potential extensions
	•	Add a Python scheduler (APScheduler or cron) to run the pipeline on a schedule
	•	Swap DuckDB for Snowflake to demonstrate cloud warehouse connectivity
	•	Add Great Expectations checkpoints for more thorough data quality validation
	•	Containerize the pipeline with Docker for reproducibility
	•	Publish dbt docs to GitHub Pages

## License
MIT
