import os
import pyspark
# this script can be executed in the docker file
from pyspark.sql import SparkSession
import configparser

import dendutils.ec2.interact
import ec2.getorcreate

config = configparser.ConfigParser()
config.read('/home/jovyan/aws/config.cfg')
os.environ['AWS_KEY_ID'] = config.get("AWS", "KEY")
os.environ['AWS_SECRET'] = config.get("AWS", 'SECRET')

spark = ec2.getorcreate.getOrCreate()
mycsv = "s3a://dendpaulogieruswest2/sampledata/titanic-data.csv"
df = spark.read.option("header", "true").csv(mycsv)
r = df.count()
spark.stop()

print('\n***\n***\n{} lines\n***\n***\n'.format(r))
