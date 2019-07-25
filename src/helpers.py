from pyspark.sql import SparkSession
from pyspark.sql.types import *


def getOrCreateSparkSession(appName='ETL Spark', coresMax='12', executerCores='12',
                            executorMemory='40g', driverMemory='40g'):
    """ helper function for building spark session """

    return SparkSession.builder \
        .appName(appName) \
        .config('spark.executor.cores', executerCores) \
        .config('spark.cores.max', coresMax) \
        .config("spark.executor.memory", executorMemory) \
        .config("spark.driver.memory", driverMemory) \
        .getOrCreate()


def getLoanSchema():
    """ definition for loan Schema """
    return ArrayType(
        StructType([
            StructField('MSISDN', LongType(), True),
            StructField('Network', StringType(), True),
            StructField('Date', TimestampType(), True),
            StructField('Product', StringType(), True),
            StructField('Amount', DoubleType(), True)
        ]))


def importCsv(pathToCSVFile, schema, spark):
    """ helper function for importing CSV files """
    return spark.read.option("header", "true") \
        .option("inferSchema", "false") \
        .option("dateFormat", "dd-MMM-yyyy") \
        .option("schema", schema) \
        .csv(pathToCSVFile)
