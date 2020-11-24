# Operators
## Purpose:
- Add custom operators to Airflow

## Content
- create_schema:
    - Init the database schema via `CREATE IF NOT EXISTS` the tables
    - the schema is defined in the file `create_tables.sql`
- pg_stage_data:
    - Load data from flat files into the staging tables
    - for data flow test purpose
- load_tables:
    - Upsert (Insert or Update) data from the staging tables into the start schema
    - Redshift does not natively support Upsert, thus a workaround is necessary
- data quality:
    - run data quality checks on the data
    - Checks that a table has a positive number of rows
    - And no duplicates on the primary key (Not natively enforced by Redshift)