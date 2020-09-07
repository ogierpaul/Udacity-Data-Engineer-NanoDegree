# Config files
- contains two files:
    - admin_config to set-up the cluster and roles
    - dbuser_config to perform the ETL
- Separating admin rights from routine access enhances security:
    - One data engineer executant could continue develop on RedShift using dbuser rights
    - But he could not create new cluster, or launch EC2 clusters

    