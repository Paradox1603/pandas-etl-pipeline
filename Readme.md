# Pandas ETL Pipeline

## Overview
This project implements an ETL pipeline using Python and Pandas to process transaction data.

## Features
- JSON data ingestion
- Data cleaning and validation
- GroupBy + Aggregation (SQL-style)
- HAVING clause filtering
- Logging (console + file)
- CLI-based execution
- Class-based design

## Architecture
Extract → Transform → Load

## Transformation Logic
- Remove invalid records (empty name / null values)
- Aggregate total spend per customer
- Filter customers with spend > threshold

## How to Run
python main_script.py --input input.json --output output.csv --min_spend 400