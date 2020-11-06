import os
from pyspark import SparkContext
import findspark


def calc_pi(sc):
    import random

    num_samples = 10 ** 6

    def inside(p):
        x = random.random()
        y = random.random()
        return x * x + y * y < 1

    count = sc.parallelize(range(0, num_samples)).filter(inside).count()
    pi = 4 * count / num_samples
    return pi

if __name__ == '__main__':
    os.environ['SPARK_HOME'] =  "/usr/local/Cellar/apache-spark/3.0.1/libexec"
    os.environ['JAVA_HOME'] = "/Library/Java/JavaVirtualMachines/adoptopenjdk-11.jdk/Contents/Home"
    os.environ['PYSPARK_PYTHON'] = "/usr/local/anaconda3/envs/dend/bin/python3"
    spark_home = os.environ.get('SPARK_HOME')
    print(spark_home)
    java_home = os.environ.get('JAVA_HOME')
    print(java_home)
    findspark.init()
    sc = SparkContext.getOrCreate()
    pi = calc_pi(sc)
    print(pi)
    sc.stop()

