import os
from pyspark import SparkContext
from pyspark.sql import SparkSession
import findspark




def calc_pi_sc(sc):
    """

    Args:
        sc (SparkContext):

    Returns:
        float
    """

    import random

    num_samples = 10 ** 6

    def inside(p):
        x = random.random()
        y = random.random()
        return x * x + y * y < 1

    count = sc.parallelize(range(0, num_samples)).filter(inside).count()
    pi = 4 * count / num_samples
    return pi

def calc_pi_ss(spark):
    """

    Args:
        spark (SparkSession):

    Returns:
        float
    """
    import random

    num_samples = 10 ** 6

    def inside(p):
        x = random.random()
        y = random.random()
        return x * x + y * y < 1
    # We access the .parallelize of spark via calling the sparkcontext of the spark session
    count = spark.sparkContext.parallelize(range(0, num_samples)).filter(inside).count()
    pi = 4 * count / num_samples
    return pi


if __name__ == '__main__':
    os.environ['SPARK_HOME'] =  "/usr/local/Cellar/apache-spark/3.0.1/libexec"
    os.environ['JAVA_HOME'] = "/Library/Java/JavaVirtualMachines/adoptopenjdk-11.jdk/Contents/Home"
    os.environ['PYSPARK_PYTHON'] = "/usr/local/anaconda3/envs/dend/bin/python3"
    findspark.init()
    sc = SparkContext.getOrCreate()
    print('spark context created')
    pi = calc_pi_sc(sc)
    print(pi)
    sc.stop()
    print('spark context stopped')
    spark = SparkSession.builder.appName("Etl").getOrCreate()
    print('Spark Session created')
    pi = calc_pi_ss(spark)
    print(pi)
    spark.stop()
    print('Spark Session stopped')

