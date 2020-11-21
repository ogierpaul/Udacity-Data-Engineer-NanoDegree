# P4_SparkDatalake
## Purpose
- ETL the data from one S3 bucket to another S3 bucket using Spark and parquet

## Introduction
- A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake.
- Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
- As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables.
- This will allow their analytics team to continue finding insights in what songs their users are listening to.
- You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Project Instructions
- In this project, you'll apply what you've learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3.
- To complete the project, you will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3.
- You'll deploy this Spark process on a cluster using AWS.

## Installation
- Adapt the instructions below to your environment

### Clone this project
- Clone this project using `git`
- cd to this directory

### Configure your AWS parameters
- Create a `config.cfg` file with your AWS credentials in the `aws` repository using the instructions in this reposority

### Install Docker
- Make sure you have docker installed: [Get docker](https://docs.docker.com/get-docker/)

### Build the docker image
- Launch the script to build the image called ogierpaul/p4spark
```shell script
bash a_build_docker_image.sh
```
Todo: you can as well directly pull the docker image from host using:
```shell script
docker pull ogierpaul/p4spark
```

### Run the docker image
```shell script
bash b_run_docker.sh
```
- connect to `localhost:8888` to see the jupyter notebook interface

### Run the etl script
- run the etl.py script located in `/home/jovyan/`
```shell script
bash c_run_etl.py
```
