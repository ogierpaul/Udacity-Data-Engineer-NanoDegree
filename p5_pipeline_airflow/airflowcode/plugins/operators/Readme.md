# Operators
## Purpose
- Add custom operators to Airflow

## Content
### create_schema
- Init the database schema via `CREATE IF NOT EXISTS` the tables
- the schema is defined in the file `create_tables.sql`


### stage_redshift.py (StageToRedshiftOperator)
#### Steps
- truncate the Redshift staging table
- Copy JSON Data from S3 into the staging table
#### Arguments
- *path*: path to S3
- *arn*: ARN credentials to be assumed by Redshift
- *table*: table name
- *jsonformat* : Optional, Json mapping

### Load_table.py (LoadTableOperator)
- Redshift does not natively support Upsert, thus a workaround is necessary

#### Upsert data in Postgre
1. Create  an empty Temporary Staging Table and fill it with the values to be inserted in the Target Table
2. Delete rows from Target table that are present in Staging tables
3. Insert into Target Tables from Staging Table
4. Delete Staging Table

#### Arguments:
- *table*: target table name
- *staging_prefix*: prefix to be added to the temporary staging table
- *query*: Query used to `SELECT` the data to be insert finally.
    - Make sure there are no duplicates on the primary key in that table
    - Do not conclude this query with semi colon `;` as it will be added later
- *pkey*: primary key used

### data_quality.py  (DataQualityOperator)
#### Data quality checks implemented
1. Check the table has a positive number of rows
2. Check the table has no duplicates primary key   

#### Arguments:
- *table*: target table name
- *pkey*: primary key used



