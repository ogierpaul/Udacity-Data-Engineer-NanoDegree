# Airflow DAGS repository
## udacity_s3_redshift_dag.py
### Purpose:
- official submission for the project
- This DAGs schedule the Redshift Queries needed to ETL data from S3 to star schema

### Prerequisites
- Redshift connection needs to be registered in Airflow as *aa_redshift*
- Raw files in S3 need to be accessible by Redshift (check ARN permissions)
- SQL queries are defined in the airflowcode/plugins/helpers
- see `docker-compose.yml` file to check how to mount the volume

### Notes
### Data quality checks
- It is better to separate the data quality check per table
- Airflow should be one task per step
- It is much easier to debug, without looking at the logs
- Oone problem in one table does not hamper downstream tasks from other tables.

### Sub Dags
- It is possible to use a SUBDAG for the operation LoadDimension (Upsert) > DataQualityChecks, And maybe for the Stage > LoadTable > DataQualityCheks > Truncate. I did not, I think that it has more potential for destabilization than for optimization
- In particular, it makes the complete flow less readable
- It also puts less flexibility if additional steps are needed later on
