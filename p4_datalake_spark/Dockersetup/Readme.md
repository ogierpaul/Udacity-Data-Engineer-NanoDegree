# Docker Set-up
## Purpose
- This directory will build the docker image for the project
- The docker image contains:
    - the spark environment with pyspark
    - the source code for the project

## Contains
### p4src
- `p4src` module
- python modules called in the etl function
- installed via the `setup.py`

### jupyter.dockerfile
- the instructions to build the docker image are contained in the `jupyter.dockerfile`
- It uses the base image `jupyter/pyspark-notebook`

### etl.py
- the script to run the ETL calling the sub-modules is located in `etl.py`
- it is copied on the docker image in `/home/jovyan`
