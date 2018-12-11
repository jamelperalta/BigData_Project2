from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Row
import csv

# Jamel Peralta Coss
# 802-13-5870
# JamelProject BD2
# Part 4: Keyword Trending vs Ocurrences on Third Date

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionInstance' not in globals()):
        globals()['sparkSessionInstance'] = SparkSession.builder.config(conf=sparkConf) \
                                            .enableHiveSupport().getOrCreate()
    return globals()['sparkSessionInstance']

def output():
    spark = getSparkSessionInstance(sc.getConf())
    df = spark.sql("use default")

    # Day 3 Query:
    df = spark.sql("select keyword, sum(total) as suma from ocurrences_table where timestamp between cast('2018-12-05 20:00:00' as timestamp)- INTERVAL 1 HOUR and cast('2018-12-05 20:00:00' as timestamp) group by keyword order by suma desc limit 10")
    df.show()

    # Write query into a csv in the hdfs
    df.repartition(1).write.csv("/data/jameloutput5day3.csv")

if __name__ == "__main__":
    sc = SparkContext(appName="Save CSV")
    output()
