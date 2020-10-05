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

## Architecture Schema
### Tables created

### Rationale

## Parameters
* All those parameters can be found in the admin_config file

|Name|Description|Example|
|---|---|---|
|DWH_IAM_ROLE|Name of the db user IAM Role||
|DWH_CLUSTER_TYPE|Cluster Type|multi-node|
|DWH_NODE_TYPE|Node type|dc2.large|
|DWH_NUM_NODES|Number of nodes in cluster|2|
|DWH_DB|Database Name|mydatabase|
|DWH_CLUSTER_IDENTIFIER|Redshift Cluster identifier|mycluster|
|DWH_DB_USER|Redshift user name|myuser|
|DWH_DB_PASSWORD|Redshift user password|myuser|