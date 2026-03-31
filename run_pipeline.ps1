Write-Host "Running pipeline..."

.venv\Scripts\activate

python extract/validate.py
python extract/load_csv.py

Set-Location transform
dbt run --profiles-dir .
dbt test --profiles-dir .
Set-Location ..

Write-Host "Pipeline complete. Refresh Power BI to see updates."
