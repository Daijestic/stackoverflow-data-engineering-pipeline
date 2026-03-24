# stackoverflow-data-engineering-pipeline

## Objective

Build an end-to-end data engineering pipeline using Stack Exchange API.

## Architecture

API -> Bronze -> Silver -> Gold -> PostgreSQL -> Airflow -> Power BI

## Tech Stack

Python, PySpark, PostgreSQL, Airflow, Power BI

## Scope v1

- Extract questions and answers from API
- Store raw JSON in Bronze
- Transform clean data into Silver parquet
- Build Gold analytics marts
