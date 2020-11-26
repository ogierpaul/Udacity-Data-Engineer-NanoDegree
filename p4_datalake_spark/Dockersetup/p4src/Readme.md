# P4src Code Structure
## Purpose
- Store the modules containing the methods used in the ETL process
- Is installed in the docker image via the dockerfile `jupyter.dockerfile`

## Structure
### sparkinit.py
 - initiate a spark session via the get_spark() method
 - uses a config file parsed by configparser with those keys:

````buildoutcfg
[AWS]
KEY=AWS_KEY_ID
SECRET=AWS_SECRET_KEY
````

- additional tip: make sure the package called in sparkinit (hadoop-aws) is in line with your hadoop version
- as of Nov. 20 , it was working with hadoop-aws 3.2.0.

```shell script
ls /usr/local/spark/jars/hadoop* # to figure out what version of hadoop
```

- See [the jupyter docker stacks recipes](https://github.com/jupyter/docker-stacks/blob/master/docs/using/recipes.md#using-pyspark-with-aws-s3)

### read.py
- read songs and logs data from a path
    - read_logs(logpath)
    - read_songs(songpath)
- example path: `s3a://udacity-dend/log_data/2018/11/2018-11-0*.json`

### transform.py
- extract the information from the raw data and convert it to a star schema
- Output tables:
    - songs
    - users
    - artists
    - time
    - songplay
 
    
