# LLM-Driven Schema Discovery, Data Ingestion, and CI/CD Automation Framework

## Overview
This project automates the discovery, versioning, and ingestion of tabular data models from the web based on a problem statement. It stores schemas in a versioned registry and generates AI-assisted pipelines to populate a target database. The system supports CI/CD, enabling automatic updates to ingestion pipelines when schema changes are detected.

## Key Capabilities

- Web scraping of tabular datasets and data models  
- Schema versioning and drift detection  
- AI-assisted generation of ingestion pipelines  
- Automatic CI/CD updates for database pipelines  

## Project Features

### Web Scraping of Data Models

- Scrapes tabular datasets from:
  - CSV files
  - HTML tables
  - JSON endpoints
- Automatically infers schema:
  - Column names
  - Data types
  - Nullability
- Supports both online sources and local files

### Schema Registry

- Stores versioned data model schemas in PostgreSQL  
- Tracks schema changes and maintains historical versions  
- Detects schema drift to trigger pipeline regeneration  

### AI (LLM) -Powered Pipeline Generation

- Generates SQL DDL for target database creation  
- Produces Python ingestion scripts using Pandas  
- Optional integration with OpenAI GPT models for enhanced pipeline logic  

### ETL Pipeline Execution

- Applies generated SQL to initialize target database  
- Executes ingestion scripts to populate tables  
- Uses Prefect flows to orchestrate ETL tasks  

### CI/CD Integration

- Monitors schema changes automatically  
- Regenerates ingestion pipelines on drift detection  
- Applies updates to target database  
- Supports automated commit or pull request creation for review  

## Tech Stack

| Layer               | Technology / Library                          |
|---------------------|-----------------------------------------------|
| Web Scraping        | BeautifulSoup, requests, duckduckgo_search    |
| Schema Inference    | pandas, deepdiff, jsonschema, lxml            |
| Database            | PostgreSQL, SQLAlchemy, psycopg2-binary       |
| ETL Orchestration   | Prefect, pendulum                             |
| CI/CD               | GitHub Actions / Jenkins                      |
| API Layer           | FastAPI, Starlette, Uvicorn                   |
| AI Integration      | huggingface-hub (Mistral-7B)                  |
| Utilities           | rich, tqdm, python-dotenv, PyYAML             |
| Versioning & Migrations | Alembic, semver, deepdiff                 |
| Monitoring & Alerts | Apprise, prometheus_client                    |