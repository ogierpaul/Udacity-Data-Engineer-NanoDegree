import os
import pyspark
# this script can be executed in the docker file
from pyspark.sql import SparkSession
import configparser
config = configparser.ConfigParser()
config.read('/home/jovyan/aws/config.cfg')
os.environ['AWS_KEY_ID'] = config.get("AWS", "KEY")
os.environ['AWS_SECRET'] = config.get("AWS", 'SECRET')

spark = SparkSession.builder.\
    master("local[*]").\
    config("spark.hadoop.fs.s3a.access.key", os.environ.get('AWS_KEY_ID')).\
    config("spark.hadoop.fs.s3a.secret.key", os.environ.get('AWS_SECRET')).\
    getOrCreate()
mycsv = "s3a://dendpaulogieruswest2/sampledata/titanic-data.csv"
df = spark.read.option("header", "true").csv(mycsv)
r = df.count()
spark.stop()

print('\n***\n***\n{} lines\n***\n***\n'.format(r))
