# Config files
- contains two files:
    - admin_config to set-up the cluster and roles
    - dbuser_config to perform the ETL
- Separating admin rights from routine access enhances security:
    - One data engineer executant could continue develop on RedShift using dbuser rights
    - But he could not create new cluster, or launch EC2 clusters

## Admin config

````buildoutcfg
[AWS]
KEY=<Admin user AWS KEY>
SECRET=<SECRET KEY>

[REGION]
REGION=us-west-2

[DWH]
DWH_CLUSTER_TYPE=multi-node
DWH_NUM_NODES=2
DWH_NODE_TYPE=dc2.large
DWH_CLUSTER_IDENTIFIER=<Cluster identifier>

[DB]
DB_NAME=<Database name>
DB_USER=<Database user>
DB_PASSWORD=<Database password>
DB_PORT=<Port>

[IAM]
DWH_IAM_ROLE_NAME=<IAM role name>
````

## Db User config
- This parameters can be configured safely after the cluster has been created
- Get the ARN and Host parameter for step 1), create_cluster_main.py

````buildoutcfg
[AWS]
KEY=<db user key>
SECRET=<db user secret key>

[REGION]
REGION=us-west-2

[DB]
DB_NAME=
DB_HOST=
DB_USER=
DB_PASSWORD=
DB_PORT=

[IAM]
ARN=<ARN ROLE NAME>
````



    