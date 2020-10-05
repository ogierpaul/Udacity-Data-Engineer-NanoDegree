# Create Env Module
## Purpose
### Create a Redshift Cluster
Instructs AWS to:
1. create a IAM role for the DWH
2. create a Redshift Cluster with the associate DWH IAM Role
3. get the cluster properties
4. open the ports on EC2 for the DWH
5. test connection

### Create the tables in Redshift
1. Create a connection to the Redshift Cluster
2. Import the SQL statements from sql_queries_create
3. Drop the tables if they previously exists
4. Create the tables

### Parameters
