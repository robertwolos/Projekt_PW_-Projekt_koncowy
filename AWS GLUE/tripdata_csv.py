from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, DateType
from pyspark.sql.window import Window
import pyspark.sql.functions as f
import boto3
from awsglue.utils import getResolvedOptions
import sys


class tech_func:
    def __init__(self):
        pass

    @staticmethod
    def digits(orginal_value):
        return f.regexp_replace(orginal_value, r"[^0-9]", "")

    @staticmethod
    def capitalize(string_col):
        return f.initcap(string_col)


def main_load_stg_datatrip():
    spark = SparkSession.builder \
        .appName('DataFrame') \
        .master('local[*]') \
        .getOrCreate()

    data_path = 's3://aws-glue-temporary-343103044627-eu-central-1.s3.eu-central-1.amazonaws.com/202203-divvy-tripdata.json'

    args = getResolvedOptions(sys.argv, ['ACCOUNT', 'WAREHOUSE', 'DB', 'SCHEMA', 'USERNAME', 'PASSWORD'])

    sf_params_dwh_store_stage = {
        "sfURL": args['ACCOUNT'],
        "sfUser": args['USERNAME'],
        "sfPassword": args['PASSWORD'],
        "sfDatabase": args['DB'],
        "sfSchema": args['SCHEMA'],
        "sfWarehouse": args['WAREHOUSE']
    }
   

    schema_datatrip = StructType([
        StructField("ride_id", StringType())
        , StructField("started_at", DateType())
        , StructField("ended_at", DateType())
        , StructField("start_station_name", StringType())
        , StructField("start_station_id", StringType())
        , StructField("end_station_name", StringType())
        , StructField("end_station_id", StringType())
        , StructField("start_lat", StringType())
        , StructField("start_lng", StringType())
        , StructField("end_lat", StringType())
        , StructField("end_lng", StringType())
        , StructField("member_casual", StringType())
        , StructField("First_Name", StringType())
        , StructField("Last_Name", StringType())
        Gender STRING)      
    ])
    df_datatrip = spark.read.option("inferSchema", "true") \
        .option("header", "true") \
        .option("mode", "DROPMALFORMED") \
        .json(data_path, schema_datatrip)

    df_datatrip = df_datatrip.select("First_Name", "Last_Name" \
                               , tech_func.digits(f.col("First_Name")).alias("First_Name") \
                               , tech_func.digits(f.col("Last_Name")).alias("Last_Name")) \
        .filter(f.col("ITEM_NBR") <> f.col("ended_at"))

    df_items.write.format("snowflake") \
        .options(**sf_params_dwh_store_stage) \
        .option("dbtable", "TRIPDATA") \
        .mode("append") \
        .save()

    glue_jobs = 'TRIPDATA_DATA_MART'
    glue = boto3.client('glue')
    response = glue.start_job_run(JobName=glue_jobs)


main_load_tmp_items()