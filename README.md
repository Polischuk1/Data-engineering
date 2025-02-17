# Data Engineering Internship Repository

This repository contains my work from a Data Engineering internship, organized by different folders.

## Repository Structure

### Folders:
- **_python**: Contains Python scripts, such as `wiki_parser.py` for scraping Wikipedia data. Docker Compose is used for PostgreSQL and environment setup.
- **_dbt**: Includes dbt models for transforming healthcare data (appointments, procedures, etc.), with incremental updates and tests. Docker is used for environment management.
- **_sql**: Includes ERD "delivery" and SQL puzzle solutions.
- **_data_platforms**: Contains Jupyter notebook with snowpark and snowflake presentation.
- **personal_project**: Contains Weather Analysis Project 

### Docker Setup:
- `docker-compose.yml` is present in each project folder to manage local environments and services like PostgreSQL.

## Getting Started

### Prerequisites:
- Docker & Docker Compose
- Python 3.x
- dbt

### Cloning the Repository:
```bash
git clone https://github.com/maksymloktionov/olha-de-internship.git
cd demo
```
## Running projects
### Python
```
docker-compose up -d
python3 _python/wiki_scrapping.py
```
### dbt
```
docker-compose up -d
docker exec -it dbt-modeling
dbt run
```




