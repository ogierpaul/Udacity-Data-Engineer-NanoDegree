# Routines
## Purpose
* This routines can be called on and automated
* They do:
    * Create the cluster
    * Create the tables
    * ETL the data from S3 to the tables

## Parameters
* They need as input the path to the config file for the admin and the database user
* Normally stored in the config file of the project

## Use
- Make sure the module p3src has been installed
- Make sure you have the pre-requisites imports
- Launch the routines via the command line

### Example for create_cluster_main:

```buildoutcfg
python create_cluster_main.py
```
