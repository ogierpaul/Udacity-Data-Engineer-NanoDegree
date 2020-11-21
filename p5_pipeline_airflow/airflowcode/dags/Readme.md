# Airflow DAGS repository
## pg_operator_dag.py
- For testing and learning purposes
- This DAGS run the data ingestion steps into a PostGreSQL Database
- Prerequisites
- Postgre connection registered as 'mypg'
- Staging files accessible by Postgres under /data/stagingarea
- SQL queries are defined in airflowcode/plugins/helpers
- see docker-compose.yml file