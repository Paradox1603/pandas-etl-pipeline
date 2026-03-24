# Pandas ETL Pipeline

## Overview
* This project implements a **production-style ETL (Extract, Transform, Load) pipeline** using Python and Pandas to process transactional data.
* The pipeline reads JSON data, performs data cleaning and aggregation, and outputs filtered results based on business rules.


## Problem Statement
Given a dataset of customer transactions, identify **high-value customers** based on their total spending.


## Features
* ✅ JSON data ingestion
* ✅ Data cleaning and validation
* ✅ Pandas-based transformation
* ✅ SQL-like aggregation (GROUP BY + HAVING)
* ✅ Logging (console + file)
* ✅ CLI-based execution
* ✅ Class-based pipeline design


##  Architecture
Extract → Transform → Load
* **Extract** → Read JSON input
* **Transform** → Clean + Aggregate + Filter
* **Load** → Save processed data to CSV


## Transformation Logic
### SQL Equivalent

```sql
SELECT  customer_id, 
        name, 
        SUM(amount) AS total_spend
FROM    transactions
WHERE   name IS NOT NULL
GROUP BY customer_id, name
HAVING SUM(amount) > min_spend;
```


## Data Validation
The pipeline performs the following checks:
* Removes records with empty names
* Removes records with null values
* Logs dropped rows for traceability


## Project Structure

pandas-etl-pipeline/
│
├── main_script.py       # Entry point (CLI)
├── pipeline_Script.py  # ETL pipeline logic
├── logger.py           # Logging configuration
├── input.json          # Sample input data
├── output.csv          # Generated output
├── requirements.txt    # Dependencies
├── README.md           # Documentation


## How to Run

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Run the pipeline
```bash
python main_script.py --input input.json --output output.csv --min_spend 400
```


## Input Example

[
  {"customer_id": 1, "name": "Alice", "amount": 200},
  {"customer_id": 1, "name": "Alice", "amount": 300},
  {"customer_id": 2, "name": "Bob", "amount": 150}
]


## Output Example

customer_id,name,total_spend
1,Alice,500


## Logging
* Logs pipeline execution steps
* Tracks invalid/dropped records
* Outputs to:

  * Console
  * `pipeline.log` file


## Tech Stack

* Python
* Pandas
* Logging
* argparse (CLI)


## Key Learnings

* Translating SQL logic into Pandas
* Building modular ETL pipelines
* Handling data quality issues
* Implementing logging for observability


## Future Improvements

* Add config-driven pipeline (JSON/YAML)
* Implement unit tests (pytest)
* Add Airflow orchestration
* Handle large-scale data (chunking/streaming)


## Author
Arul Joe Kevin V
Aspiring Data Engineer
