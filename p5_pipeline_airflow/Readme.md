# Project 5 : Data Pipeline with Airflow
## Purpose
### Context:
- A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.
- They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.
- The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of CSV logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

### End Result
- Demonstrate Airflow
- Create dags, queries, and ad-hoc operators with Airflow
- ETL data from S3 bucket to Redshit

### Submission comment
#### Instructions (short)
- git-clone and Install via docker (see below)
- Connection parameters are stored in the `.env` file --> adapt them to your configuration
- Connect to airflow on `localhost:8080` and run DAG `udacity_s3_redshift_dag`

#### Prerequisites
- Redshift has rights (ARN) to access S3
- Redshift cluster is running
- Redshift connection is saved in Airflow under *aa_redshift*

#### Order of Operations (Happy Flow)# Order of Operations (Happy Flow)
1. Create the Schema if not exits
2. Truncate staging tables and upload data from S3
3. Load fact and dimension tables with upsert
4. check tables are not empty and no duplicates on primary key
5. Truncate staging tables
6. End
![Happy_flow_p5](https://github.com/ogierpaul/Udacity-Data-Engineer-NanoDegree/blob/master/99-Appendix/Happy_flow_Redshift.png)



### Reminder: Airflow is not ETL
- Airflow is at its core a scheduler
- It does not transport data itself, but can launch tasks that do that
- Here, Airflow will call on Redshift to query S3, and trigger other Redshift Queries

## Structure
### Project Architecture
![Project_5_Architecture](https://github.com/ogierpaul/Udacity-Data-Engineer-NanoDegree/blob/master/99-Appendix/p5_Airflow.jpg)
### Repository
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
### How to Upsert
-[AWS example how to do upsert in redshift](https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-upsert.html)
![Upsert](https://github.com/ogierpaul/Udacity-Data-Engineer-NanoDegree/blob/master/99-Appendix/Upsert.jpg)

### Other references
- [Connect airflow to AWS](https://airflow.apache.org/docs/1.10.3/howto/connection/aws.html)
- [Redshift: Copy data from S3](https://docs.aws.amazon.com/us_en/redshift/latest/dg/copy-parameters-data-source-s3.html)
- [Manage connections in Airflow](https://www.astronomer.io/guides/connections/)
- [Airflow: Generate a connection URI](https://airflow.apache.org/docs/stable/howto/connection/index.html#generating-a-connection-uri) for use as environment variable
- [Airflow: Input connection URI as environment variable](https://airflow.apache.org/docs/stable/howto/connection/index.html#storing-a-connection-in-environment-variables
) to be called this connection from the environment variables

