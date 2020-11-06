import os
from pyspark.sql import SparkSession
import findspark


if __name__ == "__main__":
    os.environ['SPARK_HOME'] =  "/usr/local/Cellar/apache-spark/3.0.1/libexec"
    os.environ['JAVA_HOME'] = "/Library/Java/JavaVirtualMachines/adoptopenjdk-11.jdk/Contents/Home"
    os.environ['PYSPARK_PYTHON'] = "/usr/local/anaconda3/envs/dend/bin/python3"
    spark_home = os.environ.get('SPARK_HOME')
    print(spark_home)
    java_home = os.environ.get('JAVA_HOME')
    print(java_home)
    findspark.init()
    spark = SparkSession.builder.appName("Etl").getOrCreate()
    print('ss created')
    spark.stop()