# Operators
## Purpose
- Add custom operators to Airflow

## Content
### create_schema
- Init the database schema via `CREATE IF NOT EXISTS` the tables
- the schema is defined in the file `create_tables.sql`


### rs_stage_data
#### Steps
- truncate the Redshift staging table
- Copy JSON Data from S3 into the staging table
#### Arguments
- path: path to S3
- arn: ARN credentials to be assumed by Redshift
- table: table name

### rs_upsert_tables
- Redshift does not natively support Upsert, thus a workaround is necessary

#### Upsert data in Postgre
1. Create Temporary Staging Table
2. Delete rows from Target table that are present in Staging tables
3. Insert into Target Tables from Staging Table
4. Delete Staging Table

#### Data Quality Checks
1. Check the target table has a positive number of rows
2. Check the target table has no duplicate primary key   

#### Arguments:
- table: target table name
- staging_prefix: prefix to be added to the temporary staging table
- query: Query used to `SELECT` the data to be insert finally.
    - Make sure there are no duplicates on the primary key in that table
    - Do not conclude this query with semi colon `;` as it will be added later
- pkey: primary key used

