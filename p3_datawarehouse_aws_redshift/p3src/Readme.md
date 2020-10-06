# Source Code Module for Project 3
## Purpose
* Create the Redshift cluster
* Create the Redshift tables
* ETL the data from S3 to Redshift

## Structure
* createenv: Module for setting-up the environment (Cluster, roles, security) and the tables
* etl: Module for loading the data from s3 to Redshift
* utils: Module with some convenient classes and funtions for manipulating AWS used in multiple places throughout the project

## Architecture Schema
### Users and Roles
![IAM Schema](https://github.com/ogierpaul/Udacity-Data-Engineer-NanoDegree/blob/master/p3_datawarehouse_aws_redshift/images/IAM_Architecture_Diagram.jpg)

### Tables created

|Name|PrimaryKey|DistStyle or DistKey|SortKey|
|---|---|---|---|
|staging_events|staging_event_id|||
|staging_songs|staging_song_id|||
|songplay|(start_time, user_id)|user_id|(user_id, start_time)|
|user|user_id|user_id|user_id|
|song|song_id|artist_id|(artist_id, year)|
|artists|artist_id|artist_id|artist_id|
|time|start_time|ALL|start_time

### Data Flow Target for ETL
![DataFlow](https://github.com/ogierpaul/Udacity-Data-Engineer-NanoDegree/blob/master/p3_datawarehouse_aws_redshift/images/DataTransformation.jpg)

## Installation
### Install package locally
* cd to where the setup.py file is located
* p3_datawarehouse_aws_redshift home

````shell script
cd <home>/Udacity-Data-Engineer-NanoDegree/p3_datawarehouse_aws_redshift
````

### Verify that you have the right env
* You can create a virtual env
* As laid out in requirements.txt

### Install the package locally
* with pip

```shell script
pip install --upgrade .
```
