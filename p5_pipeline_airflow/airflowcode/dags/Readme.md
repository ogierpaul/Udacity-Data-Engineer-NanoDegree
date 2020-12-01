# Airflow DAGS repository
## udacity_s3_redshift_dag.py
### Purpose:
- official submission for the project
- This DAGs schedule the Redshift Queries needed to ETL data from S3 to star schema

### Prerequisites
- Redshift connection needs to be registered in Airflow
- Raw files in S3 need to be accessible by Redshift (check ARN permissions)
- SQL queries are defined in the airflowcode/plugins/helpers
- see `docker-compose.yml` file to check how to mount the volume

