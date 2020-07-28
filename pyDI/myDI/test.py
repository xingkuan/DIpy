import os
from os import environ
from pandas import DataFrame

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
 
 
def initialize_Spark():
    spark = SparkSession.builder \
        .master("spark://dbatool03:8081") \
        .appName("simple etl job") \
        .getOrCreate()
    return spark


def loadDFWithSchema(spark):

    schema = StructType([
        StructField("dateCrawled", TimestampType(), True),
        StructField("name", StringType(), True),
        StructField("seller", StringType(), True),
        StructField("offerType", StringType(), True),
        StructField("price", LongType(), True),
        StructField("abtest", StringType(), True),
        StructField("vehicleType", StringType(), True),
        StructField("yearOfRegistration", StringType(), True),
        StructField("gearbox", StringType(), True),
        StructField("powerPS", ShortType(), True),
        StructField("model", StringType(), True),
        StructField("kilometer", LongType(), True),
        StructField("monthOfRegistration", StringType(), True),
        StructField("fuelType", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("notRepairedDamage", StringType(), True),
        StructField("dateCreated", DateType(), True),
        StructField("nrOfPictures", ShortType(), True),
        StructField("postalCode", StringType(), True),
        StructField("lastSeen", TimestampType(), True)
    ])

    df = spark \
        .read \
        .format("csv") \
        .schema(schema)         \
        .option("header", "true") \
        .load(environ["HOME"] + "/Downloads/autos.csv")

    return df

def clean_drop_data(df):

    df_dropped = df.drop("dateCrawled","nrOfPictures","lastSeen")
    df_filtered = df_dropped.col("seller") != "gewerblich"
    df_dropped_seller = df_filtered.drop("seller")
    df_filtered2 = df_dropped_seller.col("offerType") != "Gesuch"
    df_final = df_filtered2.drop("offerType")

    return df_final



if __name__ == '__main__':
    #pass
    print("Helllo");
    
    spark = initialize_Spark()