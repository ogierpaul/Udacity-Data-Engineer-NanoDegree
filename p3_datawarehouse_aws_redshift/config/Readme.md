# Config files
- contains two files:
    - admin_config.cfg to set-up the cluster and roles
    - dbuser_config.cfg to perform the ETL
- Separates admin rights from routine access enhances security:
    - One data engineer executant can continue develop on RedShift using dbuser rights
    - But he can not create new cluster, or launch EC2 clusters

## Schema
![IAM Schema](https://github.com/ogierpaul/Udacity-Data-Engineer-NanoDegree/blob/master/99-Appendix/IAM_Architecture_Diagram.jpg)


## Admin config
### Content

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
IAM_ROLE_NAME=<IAM role name>
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
DB_NAME=<Database name>
DB_USER=<Database user>
DB_PASSWORD=<Database password>
DB_PORT=<Port>

[IAM]
IAM_ROLE_NAME=<IAM role name>
````

### Variables
#### AWS:
* [AWS User Credentials](https://docs.aws.amazon.com/general/latest/gr/aws-sec-cred-types.html#access-keys-and-secret-access-keys)
* Allow to access AWS programatically
* Key
* Secret Key

#### Region:
* Region should be in the same bucket as the DEND data sample bucket
* That is, US-West-2 (Oregon)

#### DB:
* All DB-specific variables, like for a standard JDBC connection

#### IAM:
* [IAM Role Name](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-iam-role.html)
* You should have two separate roles for the admin and the db user


    