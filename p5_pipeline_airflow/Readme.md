# Project 5 : Data Pipeline with Airflow
## Purpose
### End Result
- Demonstrate Airflow
- Create dags, queries, and ad-hoc operators with Airflow
- ETL data from S3 bucket to Redshit

### Reminder: Airflow is not ETL
- Airflow is at its core a scheduler
- It does not transport data itself, but can launch tasks that do that
- Here, Airflow will call on Redshift to query S3, and trigger other Redshift Queries

## Repository structure
- airflowcode:
    - store dags, operators, and helpers functions used in Airflow
    - is synced with the airflow running on docker (uses a bind mount, see docker-compose file)
- data:
    - sample data for testing purpose using Postgres
    - stagingarea: contains already concatenated tables of events and songs
    - not needed for Redshift as it will directly query S3
- Dockersetup
    - store Docker files to create images of Airflow and Postgres
    - and sample scripts how to launch airflow and pg as stand-alone for debugging
- docker-compose.yml, .env files:
    - environment-specific files, need to be configured before launching docker-compose

## Set-up of the project
### Test Environment: Use PG
#### Why PG
To test the environment without launching a Redshift Cluster, you can test Airflow with PG using:
- Using the sample data in `data/stagingarea/`
- Triggering the `pg_sample_dag.py` dag in `airflowcode/dags/`

#### how to launch PG + Airflow
- Modify the environment variables in the `.env` file
    - see `.sampleenv` for example
- Check the `docker-compose.yml` script
- run docker:
```shell script
docker-compose build
docker-compose up -d
```
- connect to `localhost:8080` for the Airflow UI
- Trigger the DAG **pg_sample_dag**

### AWS environment: Using AWS S3 and Redshift
#### Pre-requisite
- Update the `.env` file with your ARN role, as well as the Redshift Cluster connection details
- see section en configuring .env file with `.sampleenv`
- Make sure your redshift cluster has the right permissions
    - Can access S3
    - Authorize traffic TCP from your IP ( See Project 3)
  
#### Run airflow
- Using the same docker-compose as above
    - If needed, remove Postgre service as it is superfluous
- connect to `localhost:8080` for the Airflow UI
- Trigger the DAG **udacity_s3_redshift_dag**


## References
- [Connect airflow to AWS](https://airflow.apache.org/docs/1.10.3/howto/connection/aws.html)
- [Redshift: Copy data from S3](https://docs.aws.amazon.com/us_en/redshift/latest/dg/copy-parameters-data-source-s3.html)
- [Manage connections in Airflow](https://www.astronomer.io/guides/connections/)
- [Airflow: Generate a connection URI](https://airflow.apache.org/docs/stable/howto/connection/index.html#generating-a-connection-uri) for use as environment variable
- [Airflow: Input connection URI as environment variable](https://airflow.apache.org/docs/stable/howto/connection/index.html#storing-a-connection-in-environment-variables
) to be called this connection from the environment variables

