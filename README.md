# Personal Finance E-V

## Project overview
Personal finance data flow from extract to visualization. This pipeline ingests transaction CSVs exported from bank(s) and credit card provider(s), cleans and categorizes them using Python and dbt, stores the results in a local <> database, and serves a Power BI dashboard with key personal finance metrics.

### Key details:
	•	End-to-end ELT pipeline design
	•	Data modeling with dbt (staging → intermediate → mart layers)
	•	SQL transformations and dbt tests
	•	Local analytical storage with DuckDB
	•	Business intelligence with Power BI and DAX
	•	CI/CD with GitHub Actions
	•	! initial dev/testing in progress
	
## License
MIT
